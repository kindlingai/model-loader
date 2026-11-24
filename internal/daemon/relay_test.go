package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// linkedPeers is a Discoverer whose peers say how they are reached.
type linkedPeers []discovery.Peer

func (l linkedPeers) Peers(context.Context) ([]discovery.Peer, error) {
	return append([]discovery.Peer(nil), l...), nil
}

// holderNode serves st as node id and counts the blob requests it gets
// directly and through a relay (server.HeaderRelayed).
type holderNode struct {
	ts              *httptest.Server
	direct, relayed atomic.Int32
}

func newHolderNode(t *testing.T, st *store.Store, id string) *holderNode {
	t.Helper()
	h := &holderNode{}
	srv := server.New(st, fakeStatus{server.StatusResponse{NodeID: id, Roles: []string{"peer"}}})
	h.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			if r.Header.Get(server.HeaderRelayed) != "" {
				h.relayed.Add(1)
			} else {
				h.direct.Add(1)
			}
		}
		srv.ServeHTTP(w, r)
	}))
	t.Cleanup(h.ts.Close)
	return h
}

func (h *holderNode) addr() string { return h.ts.Listener.Addr().String() }

// relayNode runs a daemon with an empty store that relays for addrs, and
// serves its HTTP API through wrap, if set.
func relayNode(t *testing.T, id string, addrs []string, wrap func(http.Handler) http.Handler) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: id, Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{}, Selection: &catalog.Selection{},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
		RelayFor: addrs,
	})
	srv := server.New(st, d)
	srv.SetRelayer(d)
	var h http.Handler = srv
	if wrap != nil {
		h = wrap(srv)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts, st
}

func relayAsker(t *testing.T, peers linkedPeers) (*Daemon, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	return New(Config{
		NodeID: "spark-1", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: allSelection(),
		Sources: source.New(nil, ""), Discoverer: peers, MaxConcurrentTransfers: 2,
	}), st
}

func checkFile(t *testing.T, st *store.Store, want []byte) {
	t.Helper()
	got, err := os.ReadFile(st.FilePath("glm", "r1", "w.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("file content mismatch")
	}
}

// The layout of kindlingai/model-loader#48: S holds the model and is
// reached only over the slow LAN; R relays for S. A serving box copies
// every segment through R, not from S directly, and R stores none of it.
// That holds for a manifest with segment hashes and one without, whose
// segments the asker checks against the sha256 S claims through R.
func TestRelayServesHolderSegments(t *testing.T) {
	for _, hashed := range []bool{true, false} {
		t.Run(map[bool]string{true: "hashed", false: "claimed"}[hashed], func(t *testing.T) {
			content := pattern(60, 'r') // six segments of 10
			sStore, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			m := seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
			if !hashed {
				m = stripSegmentHashes(t, sStore, m)
			}
			s := newHolderNode(t, sStore, "nas")
			r, rStore := relayNode(t, "relay", []string{s.addr()}, nil)

			d, st := relayAsker(t, linkedPeers{
				{Addr: s.addr(), Link: discovery.LinkOther},
				{Addr: r.Listener.Addr().String(), Link: discovery.LinkPreferred},
			})
			if err := d.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if ms := d.Status().Models; len(ms) != 1 || ms[0].State != server.StateReady {
				t.Fatalf("status = %+v, want ready", ms)
			}
			checkFile(t, st, content)
			if n := s.direct.Load(); n != 0 {
				t.Fatalf("S served %d blob requests directly, want 0", n)
			}
			if n := s.relayed.Load(); n == 0 {
				t.Fatal("S served nothing through the relay")
			}
			if _, err := rStore.LoadManifest("glm", "r1"); err == nil {
				t.Fatal("relay saved a manifest; it should hold nothing")
			}
		})
	}
}

// stripSegmentHashes rewrites m in st with no segment hashes, as a root
// that publishes for pull-through does, keeping the segments held with
// their sha256 recorded, as origin.
func stripSegmentHashes(t *testing.T, st *store.Store, m *store.Manifest) *store.Manifest {
	t.Helper()
	lazy := &store.Manifest{Model: m.Model, Revision: m.Revision}
	sums := map[string][]string{}
	for _, fm := range m.Files {
		sums[fm.Path] = fm.SegmentSHA256
		fm.SegmentSHA256 = nil
		lazy.Files = append(lazy.Files, fm)
	}
	if err := st.SaveManifest(lazy); err != nil {
		t.Fatal(err)
	}
	ls := store.NewRevisionState(lazy)
	for _, fm := range lazy.Files {
		for i, sum := range sums[fm.Path] {
			ls.MarkSegment(fm, i, sum, store.TrustOrigin)
		}
	}
	if err := st.SaveState(ls); err != nil {
		t.Fatal(err)
	}
	return lazy
}

// A holder this node reaches at a preferred address goes before a relay:
// the box copies from it directly.
func TestRelayRanksAfterPreferredHolder(t *testing.T) {
	content := pattern(40, 'p')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	s := newHolderNode(t, sStore, "nas")
	r, _ := relayNode(t, "relay", []string{s.addr()}, nil)
	d, st := relayAsker(t, linkedPeers{
		{Addr: s.addr(), Link: discovery.LinkPreferred},
		{Addr: r.Listener.Addr().String(), Link: discovery.LinkPreferred},
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	checkFile(t, st, content)
	if n := s.relayed.Load(); n != 0 {
		t.Fatalf("S served %d blob requests through the relay, want 0", n)
	}
}

// A holder named in -peers, so of unknown link, that the relay relays for
// ranks after the relay too: the relay's operator said it reaches that
// node on a better path.
func TestRelayRanksBeforeRelayedHolderOfUnknownLink(t *testing.T) {
	content := pattern(40, 'u')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	s := newHolderNode(t, sStore, "nas")
	r, _ := relayNode(t, "relay", []string{s.addr()}, nil)
	d, st := relayAsker(t, linkedPeers{{Addr: s.addr()}, {Addr: r.Listener.Addr().String()}})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	checkFile(t, st, content)
	if n := s.direct.Load(); n != 0 {
		t.Fatalf("S served %d blob requests directly, want 0", n)
	}
}

// A relay that fails mid-copy drops out for the rest of the file: the
// rest comes from S directly.
func TestRelayFailsOver(t *testing.T) {
	content := pattern(60, 'f')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	s := newHolderNode(t, sStore, "nas")
	var relayBlobs atomic.Int32
	r, _ := relayNode(t, "relay", []string{s.addr()}, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if strings.HasPrefix(req.URL.Path, "/blob/") {
				relayBlobs.Add(1)
				w.Header().Set("Content-Length", "10")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("abc"))
				panic(http.ErrAbortHandler)
			}
			h.ServeHTTP(w, req)
		})
	})
	d, st := relayAsker(t, linkedPeers{
		{Addr: s.addr(), Link: discovery.LinkOther},
		{Addr: r.Listener.Addr().String(), Link: discovery.LinkPreferred},
	})
	d.cfg.MaxConcurrentTransfers = 1
	d = New(d.cfg)
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	checkFile(t, st, content)
	// FetchFile runs two transfers per slot, so two may be in flight to
	// the relay before it is marked dead; none start after.
	if n := relayBlobs.Load(); n < 1 || n > 2 {
		t.Fatalf("relay got %d blob requests, want 1 or 2 (dropped after it failed)", n)
	}
	if n := s.direct.Load(); n != 6 {
		t.Fatalf("S served %d blob requests directly, want 6", n)
	}
}

// A relayed transfer holds one of the relay's -max-concurrent-transfers
// slots: with one, the holder never sees two relayed requests at once.
func TestRelayHoldsTransferSlot(t *testing.T) {
	content := pattern(40, 's')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	sSrv := server.New(sStore, fakeStatus{server.StatusResponse{NodeID: "nas", Roles: []string{"peer"}}})
	var inflight, peak atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			now := inflight.Add(1)
			defer inflight.Add(-1)
			for p := peak.Load(); now > p && !peak.CompareAndSwap(p, now); p = peak.Load() {
			}
			time.Sleep(30 * time.Millisecond)
		}
		sSrv.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: "relay", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{}, Selection: &catalog.Selection{},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
		RelayFor: []string{s.Listener.Addr().String()}, MaxConcurrentTransfers: 1,
	})
	srv := server.New(st, d)
	srv.SetRelayer(d)
	r := httptest.NewServer(srv)
	t.Cleanup(r.Close)

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, r.URL+"/blob/glm/r1/w.bin", nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", i*10, i*10+9))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, content[i*10:i*10+10]) {
				t.Errorf("range %d: %s %q", i, resp.Status, got)
			}
		}()
	}
	wg.Wait()
	if p := peak.Load(); p != 1 {
		t.Fatalf("%d relayed transfers ran at once, want 1 (-max-concurrent-transfers)", p)
	}
}

// A consumer checksum the holder's bytes fail is a 422 through the relay
// too, so the asker keeps the relay for its other ranges.
func TestRelayPassesCheckFailure(t *testing.T) {
	content := pattern(20, 'k')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(server.HeaderRangeXXH64) != "" {
			http.Error(w, "range offset 0 length 10: check failed", http.StatusUnprocessableEntity)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(s.Close)
	r, _ := relayNode(t, "relay", []string{s.Listener.Addr().String()}, nil)
	req, _ := http.NewRequest(http.MethodGet, r.URL+"/blob/glm/r1/w.bin", nil)
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set(server.HeaderRangeXXH64, "0+10=0123456789abcdef")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("relay = %s, want 422", resp.Status)
	}
}

// Two relays that relay for each other do not loop: a request from a
// relay is answered from what the node holds, so a range neither holds is
// a 416, and neither lists the other's relayed segments.
func TestRelayDoesNotChain(t *testing.T) {
	content := pattern(20, 'c')
	sStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, sStore, "glm", 10, map[string][]byte{"w.bin": content}, "w.bin")
	s := newHolderNode(t, sStore, "nas")

	// R2 relays for S; R1 relays for R2 only.
	r2, _ := relayNode(t, "r2", []string{s.addr()}, nil)
	r1, _ := relayNode(t, "r1", []string{r2.Listener.Addr().String()}, nil)

	resp, err := http.Get(r1.URL + "/blob/glm/r1/w.bin")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("R1 blob = %s, want 404: R2 must not relay a relayed request on", resp.Status)
	}
	if n := s.relayed.Load() + s.direct.Load(); n != 0 {
		t.Fatalf("S served %d blob requests, want 0", n)
	}
	// R2 itself relays.
	resp, err = http.Get(r2.URL + "/blob/glm/r1/w.bin")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("R2 blob = %s, want 200", resp.Status)
	}
}
