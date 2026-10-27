package source

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// huggingfaceSource fetches a model from the Hugging Face Hub, using its
// plain HTTP API directly rather than depending on huggingface_hub: the tree
// API for the file list, and for content the CDN URL each file's resolve
// URL redirects to (which supports Range).
type huggingfaceSource struct {
	client *http.Client
	// token, if set, is sent as a bearer token so gated and private repos
	// can be fetched. It only ever goes to huggingface.co, never the CDN.
	token string

	mu sync.Mutex
	// files maps a file's resolve URL to where its content is served.
	// shortcut: entries are never pruned (about 1 KB per file ever fetched);
	// drop them after expiry if a daemon ever fetches enough files to care.
	files map[string]*hfFile
}

// hfFile is where one file's content is served: the Hub's redirect target,
// usually a signed CDN URL. mu is held while it is resolved, so all the
// segments of a file wait for one resolve.
type hfFile struct {
	mu  sync.Mutex
	url string
}

type hfTreeEntry struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Size int64  `json:"size"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs,omitempty"`
}

func (s *huggingfaceSource) request(ctx context.Context, method, u string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	if s.token != "" && req.URL.Host == "huggingface.co" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	return req, nil
}

// getJSON decodes the JSON answer to a GET of u into v.
func (s *huggingfaceSource) getJSON(ctx context.Context, u string, v any) error {
	req, err := s.request(ctx, http.MethodGet, u)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("huggingface source: GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("huggingface source: GET %s: %s", u, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("huggingface source: decode %s: %w", u, err)
	}
	return nil
}

// Resolve resolves m's revision (a branch, tag or commit) to its commit
// first, and lists the files of that commit, so a branch that moves
// meanwhile cannot mix two commits' files.
func (s *huggingfaceSource) Resolve(ctx context.Context, m catalog.Model) (*Resolved, error) {
	var info struct {
		SHA string `json:"sha"`
	}
	if err := s.getJSON(ctx, fmt.Sprintf("https://huggingface.co/api/models/%s/revision/%s", m.Source.Repo, url.PathEscape(m.Revision)), &info); err != nil {
		return nil, err
	}
	if !isCommit(info.SHA) {
		return nil, fmt.Errorf("huggingface source: %s@%s: no commit id in the revision's info (got %q)", m.Source.Repo, m.Revision, info.SHA)
	}

	var entries []hfTreeEntry
	if err := s.getJSON(ctx, fmt.Sprintf("https://huggingface.co/api/models/%s/tree/%s?recursive=true", m.Source.Repo, info.SHA), &entries); err != nil {
		return nil, err
	}

	resolved := &Resolved{Commit: info.SHA}
	for _, e := range entries {
		if e.Type != "file" {
			continue
		}
		f := File{Path: e.Path, Size: e.Size}
		if e.LFS != nil {
			// LFS pointer files carry the real content's size and
			// sha256 separately from the pointer blob itself. For a
			// gated repo the caller has no access to, the Hub masks the
			// sha256 with asterisks; downloads would then fail anyway.
			if !isSHA256(e.LFS.OID) {
				return nil, fmt.Errorf("huggingface source: %s@%s: no sha256 for %s (gated repo: request access on huggingface.co and set HF_TOKEN to a token that has it)", m.Source.Repo, m.Revision, e.Path)
			}
			f.Size = e.LFS.Size
			f.SHA256 = e.LFS.OID
		}
		resolved.Files = append(resolved.Files, f)
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("huggingface source: %s@%s has no files (check the revision exists and the repo is public or HF_TOKEN can read it)", m.Source.Repo, m.Revision)
	}
	return resolved, nil
}

// contentURL returns where the file at resolve URL hub is served, asking
// the Hub only when that isn't known yet or is stale, a URL the caller
// found expired. The Hub rate-limits resolve URLs far below the request
// rate of a small-segment fetch, so each file is resolved once and its
// segments go to the CDN (#43).
func (s *huggingfaceSource) contentURL(ctx context.Context, hub, stale string) (string, error) {
	s.mu.Lock()
	if s.files == nil {
		s.files = map[string]*hfFile{}
	}
	f := s.files[hub]
	if f == nil {
		f = &hfFile{}
		s.files[hub] = f
	}
	s.mu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.url != "" && f.url != stale {
		return f.url, nil
	}
	req, err := s.request(ctx, http.MethodHead, hub)
	if err != nil {
		return "", err
	}
	noRedirect := *s.client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := noRedirect.Do(req)
	if err != nil {
		return "", fmt.Errorf("huggingface source: HEAD %s: %w", hub, err)
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		f.url = hub // the Hub serves the file itself
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		loc, err := resp.Location()
		if err != nil {
			return "", fmt.Errorf("huggingface source: HEAD %s: %s: %w", hub, resp.Status, err)
		}
		f.url = loc.String()
	default:
		return "", fmt.Errorf("huggingface source: HEAD %s: %s", hub, resp.Status)
	}
	return f.url, nil
}

// Open reads a range of the file at path. Errors name its resolve URL,
// not the signed CDN URL, which would let whoever reads the log fetch it.
func (s *huggingfaceSource) Open(ctx context.Context, m catalog.Model, path string, offset, length int64) (io.ReadCloser, error) {
	hub := joinURLPath(fmt.Sprintf("https://huggingface.co/%s/resolve/%s", m.Source.Repo, m.Revision), path)
	ranged := offset > 0 || length > 0
	get := func(u string) (*http.Response, error) {
		req, err := s.request(ctx, http.MethodGet, u)
		if err != nil {
			return nil, err
		}
		if ranged {
			req.Header.Set("Range", rangeHeader(offset, length))
		}
		resp, err := s.client.Do(req)
		if ue, ok := err.(*url.Error); ok {
			err = ue.Err // it names the signed URL
		}
		return resp, err
	}
	u, err := s.contentURL(ctx, hub, "")
	if err != nil {
		return nil, err
	}
	resp, err := get(u)
	if err == nil && resp.StatusCode == http.StatusForbidden {
		// The signed CDN URL has expired: resolve the file again.
		resp.Body.Close()
		if u, err = s.contentURL(ctx, hub, u); err != nil {
			return nil, err
		}
		resp, err = get(u)
	}
	if err != nil {
		return nil, fmt.Errorf("huggingface source: GET %s: %w", hub, err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		if ranged && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("huggingface source: GET %s: %w", hub, ErrRangeIgnored)
		}
		return resp.Body, nil
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("huggingface source: GET %s: %s", hub, resp.Status)
	}
}

func isSHA256(s string) bool { return isHex(s, 64) }

// isCommit reports whether s is a git commit id, as the Hub gives them.
func isCommit(s string) bool { return isHex(s, 40) }

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
