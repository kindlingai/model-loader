package daemon

import (
	"bytes"
	"context"
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

// holdOnly drops every segment of name@r1's file at path from st's state
// except those in held, as if st had fetched only those.
func holdOnly(t *testing.T, st *store.Store, m *store.Manifest, path string, held ...int) {
	t.Helper()
	rs, err := st.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	fm, _ := m.Find(path)
	fs := rs.FileState(fm)
	fs.Complete = false
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = false
	}
	for _, i := range held {
		fs.SegmentsDone[i] = true
	}
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
}

// fleetNode serves st and records which segments of path it served, how
// many blob requests it got, and the most blob requests in flight at once
// across every node sharing inflight.
type fleetNode struct {
	ts       *httptest.Server
	blobs    atomic.Int32
	mu       sync.Mutex
	served   []string
	inflight *atomic.Int32
	peak     *atomic.Int32
}

func newFleetNode(t *testing.T, st *store.Store, id string, inflight, peak *atomic.Int32) *fleetNode {
	t.Helper()
	n := &fleetNode{inflight: inflight, peak: peak}
	roles := []string{"peer"}
	if id == "nas" {
		roles = []string{"root"}
	}
	srv := server.New(st, fakeStatus{server.StatusResponse{NodeID: id, Roles: roles}})
	n.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			n.blobs.Add(1)
			now := n.inflight.Add(1)
			defer n.inflight.Add(-1)
			for {
				p := n.peak.Load()
				if now <= p || n.peak.CompareAndSwap(p, now) {
					break
				}
			}
			// Hold each transfer briefly, so parallel ones overlap.
			time.Sleep(20 * time.Millisecond)
			n.mu.Lock()
			n.served = append(n.served, r.Header.Get("Range"))
			n.mu.Unlock()
		}
		srv.ServeHTTP(w, r)
	}))
	t.Cleanup(n.ts.Close)
	return n
}

func (n *fleetNode) addr() string { return n.ts.Listener.Addr().String() }

// The segments of one file are spread across three peers. A fourth node
// builds the whole file from them, fetching different segments from
// different peers at once, with no traffic to the root that also holds it.
func TestDaemonBuildsFileFromSegmentsSpreadAcrossPeers(t *testing.T) {
	content := pattern(60, 'x') // six segments of 10
	files := map[string][]byte{"w.bin": content}
	var inflight, peak atomic.Int32

	rootStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := seedFiles(t, rootStore, "glm", 10, files, "w.bin")
	root := newFleetNode(t, rootStore, "nas", &inflight, &peak)

	var peers []*fleetNode
	for k, held := range [][]int{{0, 3}, {1, 4}, {2, 5}} {
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		seedFiles(t, st, "glm", 10, files, "w.bin")
		holdOnly(t, st, manifest, "w.bin", held...)
		peers = append(peers, newFleetNode(t, st, "spark-"+string(rune('1'+k)), &inflight, &peak))
	}

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID: "spark-4", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: allSelection(),
		Sources: source.New(nil, ""),
		// The root is listed first: holders must still win.
		Discoverer:             discovery.NewStatic([]string{root.addr(), peers[0].addr(), peers[1].addr(), peers[2].addr()}),
		MaxConcurrentTransfers: 3,
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ms := d.Status().Models; len(ms) != 1 || ms[0].State != server.StateReady {
		t.Fatalf("status = %+v, want ready", ms)
	}
	got, err := os.ReadFile(st.FilePath("glm", "r1", "w.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("file content mismatch")
	}
	if n := root.blobs.Load(); n != 0 {
		t.Fatalf("root served %d blob requests, want 0", n)
	}
	for k, p := range peers {
		if n := p.blobs.Load(); n != 2 {
			t.Fatalf("peer %d served %d blob requests (%v), want 2 (only its own segments)", k, n, p.served)
		}
	}
	if p := peak.Load(); p < 2 {
		t.Fatalf("at most %d segment transfers ran at once, want several in parallel", p)
	}
	if p := peak.Load(); p > 3 {
		t.Fatalf("%d segment transfers ran at once, above the limit of 3", p)
	}
}

// A peer that held a segment goes away mid-copy: that segment fails over to
// the next source for it, and the rest of the file still comes from peers.
func TestDaemonSegmentFailsOverWhenHolderGoesAway(t *testing.T) {
	content := pattern(40, 'f')
	files := map[string][]byte{"w.bin": content}
	var inflight, peak atomic.Int32

	aStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := seedFiles(t, aStore, "glm", 10, files, "w.bin")
	a := newFleetNode(t, aStore, "a", &inflight, &peak) // holds everything

	bStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, bStore, "glm", 10, files, "w.bin")
	holdOnly(t, bStore, manifest, "w.bin", 0, 1, 2, 3)
	bSrv := server.New(bStore, fakeStatus{server.StatusResponse{NodeID: "b"}})
	var bBlobs atomic.Int32
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") && bBlobs.Add(1) == 1 {
			// Gone mid-copy: headers, then a cut connection.
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abc"))
			panic(http.ErrAbortHandler)
		}
		bSrv.ServeHTTP(w, r)
	}))
	t.Cleanup(b.Close)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID: "c", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: allSelection(),
		Sources:                source.New(nil, ""),
		Discoverer:             discovery.NewStatic([]string{b.Listener.Addr().String(), a.addr()}),
		MaxConcurrentTransfers: 1,
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, err := os.ReadFile(st.FilePath("glm", "r1", "w.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("file content mismatch")
	}
}

// A plain peer too old for /segments, whose /status reports the model
// ready, still comes before a root that holds it.
func TestDaemonPrefersOlderReadyPeerOverRoot(t *testing.T) {
	content := pattern(30, 'o')
	files := map[string][]byte{"w.bin": content}
	var inflight, peak atomic.Int32
	rootStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, rootStore, "glm", 10, files, "w.bin")
	root := newFleetNode(t, rootStore, "nas", &inflight, &peak)

	oldStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, oldStore, "glm", 10, files, "w.bin")
	oldSrv := server.New(oldStore, fakeStatus{server.StatusResponse{NodeID: "old", Roles: []string{"peer"},
		Models: []server.ModelStatus{{Model: "glm", Revision: "r1", State: server.StateReady}}}})
	var oldBlobs atomic.Int32
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/segments/") {
			http.NotFound(w, r) // too old to have it
			return
		}
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			oldBlobs.Add(1)
		}
		oldSrv.ServeHTTP(w, r)
	}))
	t.Cleanup(old.Close)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID: "new", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: allSelection(),
		Sources:    source.New(nil, ""),
		Discoverer: discovery.NewStatic([]string{root.addr(), old.Listener.Addr().String()}),
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if n := root.blobs.Load(); n != 0 {
		t.Fatalf("root served %d blobs, want 0", n)
	}
	if n := oldBlobs.Load(); n != 3 {
		t.Fatalf("older peer served %d blobs, want 3", n)
	}
}
