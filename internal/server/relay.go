package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/kindlingai/model-loader/internal/store"
)

// HeaderRelayed, on a request to another node, says a relay sent it on
// behalf of the node that asked the relay. A node answers it only from
// what it holds itself: it relays nothing on, so relays never chain or
// loop, and its /segments has no relayed part.
const HeaderRelayed = "X-Relayed"

// RelayedHolder is what one node a relay relays for holds of a revision,
// as that node answered its /segments.
type RelayedHolder struct {
	NodeID string                  `json:"node_id,omitempty"`
	Files  map[string]FileSegments `json:"files"`
}

// ErrNotRelayed means no node this one relays for holds the range.
var ErrNotRelayed = errors.New("no node relayed for holds the range")

// Relayer serves, on a node in relay mode, ranges the node does not hold
// from nodes it relays for, without storing them (see docs/design.md,
// "Relay").
type Relayer interface {
	// RelayedSegments returns what the nodes this one relays for hold of
	// model@revision, one entry for each that answered.
	RelayedSegments(ctx context.Context, model, revision string) []RelayedHolder
	// Relay asks the nodes this one relays for that hold some of path,
	// in turn, for the range r asks for, holding one of this node's
	// transfer slots. It returns the first answer of 200 or 206 and a
	// function to call once its body is copied, which gives the slot
	// back. ErrNotRelayed means none had the range.
	Relay(ctx context.Context, model, revision, path string, r *http.Request) (*http.Response, func(), error)
}

// SetRelayer makes s serve, through rl, a blob range it does not hold, and
// list in /segments what rl relays.
func (s *Server) SetRelayer(rl Relayer) { s.relay = rl }

// relayable reports whether s may relay r: it has a Relayer and r is not
// itself from a relay.
func (s *Server) relayable(r *http.Request) bool {
	return s.relay != nil && r.Header.Get(HeaderRelayed) == ""
}

// relayHeaders are the response headers a relay passes on from the node
// that holds the range. The sha256 and trust go with the bytes, so the
// asker checks them as it would a direct copy: the relay adds no trust.
var relayHeaders = []string{"Content-Length", "Content-Range", "Accept-Ranges", "Content-Type", HeaderSegmentSHA256, HeaderSegmentTrust}

// relayBufs holds the buffers a relay copies through: what a relay holds
// of one transfer at once.
var relayBufs = sync.Pool{New: func() any { b := make([]byte, RelayBuffer); return &b }}

// RelayBuffer is how many bytes of one relayed transfer a relay holds at
// once.
const RelayBuffer = 256 << 10

// relayBlob serves r through s.relay. It reports false, having written
// nothing, when no node relayed for has the range.
func (s *Server) relayBlob(w http.ResponseWriter, r *http.Request, model, revision, path string) bool {
	resp, done, err := s.relay.Relay(r.Context(), model, revision, path, r)
	if errors.Is(err, ErrNotRelayed) {
		return false
	}
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, store.ErrCheckFailed) {
			// A consumer checksum the holder's bytes do not match: the
			// checksum is wrong, not this relay (see handleBlob).
			status = http.StatusUnprocessableEntity
		}
		http.Error(w, "relay: "+err.Error(), status)
		return true
	}
	defer done()
	defer resp.Body.Close()
	for _, h := range relayHeaders {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return true
	}
	buf := relayBufs.Get().(*[]byte)
	defer relayBufs.Put(buf)
	// writerOnly hides the ResponseWriter's ReadFrom, so the copy goes
	// through buf and no more than it is held.
	_, _ = io.CopyBuffer(writerOnly{w}, readerOnly{resp.Body}, *buf)
	return true
}

// writerOnly and readerOnly hide ReadFrom and WriteTo, so io.CopyBuffer
// copies through its buffer.
type writerOnly struct{ io.Writer }
type readerOnly struct{ io.Reader }
