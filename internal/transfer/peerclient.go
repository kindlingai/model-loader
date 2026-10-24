// Package transfer drives resumable, segment-verified downloads from
// whichever source (a LAN peer or a WAN source.Source) currently has a given
// byte range, falling back to the next candidate source when one doesn't
// have it yet rather than failing the whole file.
package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
)

// escapePath percent-escapes each slash-separated segment of a manifest
// path, so paths with spaces or other reserved characters round-trip.
func escapePath(p string) string {
	segments := strings.Split(p, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
}

// ErrRangeUnavailable means the source doesn't have the requested byte range
// ready yet (an HTTP 416, or the model/revision not found at all, a 404).
// Callers should try the next candidate source, not treat this as fatal.
var ErrRangeUnavailable = errors.New("transfer: range unavailable at source")

// PeerClient talks to another model-loader node's LAN-facing HTTP API.
type PeerClient struct {
	client *http.Client
}

// NewPeerClient builds a PeerClient. A nil client uses http.DefaultClient.
func NewPeerClient(client *http.Client) *PeerClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &PeerClient{client: client}
}

// Status fetches a peer's /status.
func (c *PeerClient) Status(ctx context.Context, addr string) (*server.StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer %s: status: %w", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s: status: %s", addr, resp.Status)
	}
	var s server.StatusResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSegmentsBody)).Decode(&s); err != nil {
		return nil, fmt.Errorf("peer %s: decode status: %w", addr, err)
	}
	return &s, nil
}

// Manifest fetches a peer's manifest for model/revision. It returns
// ErrRangeUnavailable if the peer doesn't have it (404).
func (c *PeerClient) Manifest(ctx context.Context, addr, model, revision string) (*store.Manifest, error) {
	url := fmt.Sprintf("http://%s/manifest/%s/%s", addr, model, revision)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer %s: manifest: %w", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrRangeUnavailable
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s: manifest: %s", addr, resp.Status)
	}
	var m store.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("peer %s: decode manifest: %w", addr, err)
	}
	return &m, nil
}

type noPullKey struct{}

// WithNoPull marks ctx so that blob requests made with it ask the node to
// serve only what it holds (see server.HeaderNoPull).
func WithNoPull(ctx context.Context) context.Context {
	return context.WithValue(ctx, noPullKey{}, true)
}

// maxSegmentsBody bounds a peer's /segments (and /status) answer. A revision with ten
// thousand files of a hundred segment runs each fits well inside it.
const maxSegmentsBody = 16 << 20

// ErrNoSegments means a peer predates GET /segments, so what it holds is
// unknown.
var ErrNoSegments = errors.New("peer does not report segments")

// Segments fetches which segments of model/revision a peer holds. It
// returns ErrNoSegments if the peer is too old to answer (404).
func (c *PeerClient) Segments(ctx context.Context, addr, model, revision string) (*server.SegmentsResponse, error) {
	u := fmt.Sprintf("http://%s/segments/%s/%s", addr, url.PathEscape(model), url.PathEscape(revision))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer %s: segments: %w", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNoSegments
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s: segments: %s", addr, resp.Status)
	}
	var s server.SegmentsResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSegmentsBody)).Decode(&s); err != nil {
		return nil, fmt.Errorf("peer %s: decode segments: %w", addr, err)
	}
	return &s, nil
}

// OpenBlob returns a reader for byte range [offset, offset+length) of path
// within model/revision on the peer at addr. length <= 0 means to EOF. It
// returns ErrRangeUnavailable if the peer reports 404 or 416 -- not yet
// having this file, or not yet having this range of it.
//
// checks, if any, go to the peer in the X-Range-XXH64 header. When the
// peer sends X-Segment-SHA256 (it does for a file with no segment hashes
// in the manifest) the reader returned is a Claimer with it and the
// peer's X-Segment-Trust.
func (c *PeerClient) OpenBlob(ctx context.Context, addr, model, revision, path string, offset, length int64, checks []store.RangeCheck) (io.ReadCloser, error) {
	url := fmt.Sprintf("http://%s/blob/%s/%s/%s", addr, model, revision, escapePath(path))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 || length > 0 {
		if length > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
	}
	if len(checks) > 0 {
		req.Header.Set(server.HeaderRangeXXH64, store.FormatChecks(checks))
	}
	if ctx.Value(noPullKey{}) != nil {
		req.Header.Set(server.HeaderNoPull, "1")
	}
	if p := server.Priority(ctx); p != 0 {
		req.Header.Set(server.HeaderPriority, strconv.Itoa(p))
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer %s: blob %s: %w", addr, path, err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		if sum := resp.Header.Get(server.HeaderSegmentSHA256); sum != "" {
			return claimed{resp.Body, sum, resp.Header.Get(server.HeaderSegmentTrust)}, nil
		}
		return resp.Body, nil
	case http.StatusNotFound, http.StatusRequestedRangeNotSatisfiable:
		resp.Body.Close()
		return nil, ErrRangeUnavailable
	case http.StatusUnprocessableEntity:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, checkFailed(fmt.Sprintf("peer %s: blob %s: %s", addr, path, strings.TrimSpace(string(msg))))
	default:
		// Say why: a root that fetched the range from its origin says
		// what went wrong there (a checksum mismatch, an origin error).
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		if m := strings.TrimSpace(string(msg)); m != "" {
			return nil, fmt.Errorf("peer %s: blob %s: %s: %s", addr, path, resp.Status, m)
		}
		return nil, fmt.Errorf("peer %s: blob %s: %s", addr, path, resp.Status)
	}
}

// checkFailed is a peer's answer that a consumer checksum did not match;
// its text is the peer's own, which names the check.
type checkFailed string

func (e checkFailed) Error() string { return string(e) }
func (e checkFailed) Unwrap() error { return store.ErrCheckFailed }
