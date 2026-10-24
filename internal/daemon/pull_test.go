package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	"github.com/kindlingai/model-loader/internal/xxh64"
)

// fakeOrigin is a plain HTTP model source (manifest.json plus files, with
// Range support) that counts what it serves.
type fakeOrigin struct {
	ts        *httptest.Server
	files     map[string][]byte
	bytes     atomic.Int64 // file bytes served
	ranges    atomic.Int32 // file requests
	manifests atomic.Int32 // manifest.json requests
	conns     atomic.Int32 // connections opened
	delay     time.Duration
}

func newFakeOrigin(t *testing.T, files map[string][]byte) *fakeOrigin {
	t.Helper()
	o := &fakeOrigin{files: files}
	o.ts = httptest.NewUnstartedServer(http.HandlerFunc(o.serve))
	o.ts.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			o.conns.Add(1)
		}
	}
	o.ts.Start()
	t.Cleanup(o.ts.Close)
	return o
}

func (o *fakeOrigin) serve(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "manifest.json" {
		o.manifests.Add(1)
		type file struct {
			Path   string `json:"path"`
			Size   int64  `json:"size"`
			SHA256 string `json:"sha256"`
		}
		var hm struct {
			Files []file `json:"files"`
		}
		for _, name := range sortedKeys(o.files) {
			sum := sha256.Sum256(o.files[name])
			hm.Files = append(hm.Files, file{name, int64(len(o.files[name])), hex.EncodeToString(sum[:])})
		}
		_ = json.NewEncoder(w).Encode(hm)
		return
	}
	content, ok := o.files[p]
	if !ok {
		http.NotFound(w, r)
		return
	}
	o.ranges.Add(1)
	if o.delay > 0 {
		time.Sleep(o.delay)
	}
	start, end := int64(0), int64(len(content))-1
	if h := r.Header.Get("Range"); h != "" {
		a, b, _ := strings.Cut(strings.TrimPrefix(h, "bytes="), "-")
		start, _ = strconv.ParseInt(a, 10, 64)
		if b != "" {
			end, _ = strconv.ParseInt(b, 10, 64)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	}
	n, _ := w.Write(content[start : end+1])
	o.bytes.Add(int64(n))
}

func sortedKeys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// pullFleet is a root with no copy of anything, serving pull-through from
// origin, and a way to make peers that know only the root.
type pullFleet struct {
	t      *testing.T
	origin *fakeOrigin
	model  catalog.Model
	root   *Daemon
	rootSt *store.Store
	rootTS *httptest.Server
}

func newPullFleet(t *testing.T, files map[string][]byte, rootSel *catalog.Selection) *pullFleet {
	t.Helper()
	f := &pullFleet{t: t, origin: newFakeOrigin(t, files)}
	f.model = catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: f.origin.ts.URL}}
	var err error
	if f.rootSt, err = store.Open(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if rootSel == nil {
		rootSel = &catalog.Selection{}
	}
	f.root = New(Config{
		NodeID: "nas", Roles: []string{"root"}, Store: f.rootSt,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: rootSel,
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
		SegmentSize: 10,
	})
	srv := server.New(f.rootSt, f.root)
	srv.SetPuller(f.root)
	f.rootTS = httptest.NewServer(srv)
	t.Cleanup(f.rootTS.Close)
	return f
}

func (f *pullFleet) peer(sel *catalog.Selection) (*Daemon, *store.Store) {
	f.t.Helper()
	st, err := store.Open(f.t.TempDir())
	if err != nil {
		f.t.Fatal(err)
	}
	return New(Config{
		NodeID: "spark", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: sel,
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{f.rootTS.Listener.Addr().String()}),
	}), st
}

func rangeSel(path string, rs ...catalog.ByteRange) *catalog.Selection {
	return &catalog.Selection{Partial: []catalog.Pieces{{Model: "glm", Ranges: []catalog.FileRanges{{Path: path, Ranges: rs}}}}}
}

func checkHeld(t *testing.T, st *store.Store, path string, content []byte, want []bool, trust string) {
	t.Helper()
	mf, err := st.LoadManifest("glm", "r1")
	if err != nil {
		t.Fatal(err)
	}
	fm, _ := mf.Find(path)
	rs, err := st.LoadState(mf)
	if err != nil {
		t.Fatal(err)
	}
	fs := rs.FileState(fm)
	got, err := os.ReadFile(st.FilePath("glm", "r1", path))
	if err != nil {
		t.Fatal(err)
	}
	for i, w := range want {
		if fs.SegmentsDone[i] != w {
			t.Fatalf("%s segment %d held = %v, want %v (%v)", path, i, fs.SegmentsDone[i], w, fs.SegmentsDone)
		}
		if !w {
			continue
		}
		if fs.TrustOf(i) != trust {
			t.Fatalf("%s segment %d trust = %q, want %q", path, i, fs.TrustOf(i), trust)
		}
		s, e := fm.SegmentBounds(i)
		if !bytes.Equal(got[s:e], content[s:e]) {
			t.Fatalf("%s segment %d content mismatch", path, i)
		}
	}
}

// A peer asks for a range of a file no LAN node holds. The root fetches
// only the segments that range touches from the origin, and both hold them,
// verified by the root's origin fetch.
func TestPullThroughFetchesOnlyTheRange(t *testing.T) {
	content := pattern(100, 'p') // ten segments of 10
	f := newPullFleet(t, map[string][]byte{"w.bin": content, "other.bin": pattern(30, 'o')}, nil)
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 25, Length: 10}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.origin.bytes.Load(); got != 20 {
		t.Fatalf("origin served %d bytes, want 20 (segments 2 and 3 only)", got)
	}
	if got := f.origin.manifests.Load(); got != 1 {
		t.Fatalf("manifest.json fetched %d times, want 1", got)
	}
	want := []bool{false, false, true, true, false, false, false, false, false, false}
	checkHeld(t, st, "w.bin", content, want, store.TrustOrigin)
	checkHeld(t, f.rootSt, "w.bin", content, want, store.TrustOrigin)
	if ms := peer.Status().Models; len(ms) != 1 || ms[0].State != server.StatePartial {
		t.Fatalf("peer status = %+v, want partial", ms)
	}

	// Again, from a fresh peer: the root holds it now, so no WAN traffic.
	peer2, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 25, Length: 10}))
	if err := peer2.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.origin.bytes.Load(); got != 20 {
		t.Fatalf("origin served %d bytes after a second peer, want still 20", got)
	}
}

// Two peers asking for the same range at the same time cause one fetch from
// the origin.
func TestPullThroughDedupesConcurrentRequests(t *testing.T) {
	content := pattern(100, 'd')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	f.origin.delay = 100 * time.Millisecond
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for k := range errs {
		peer, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 40, Length: 5}))
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[k] = peer.Reconcile(context.Background())
		}()
	}
	wg.Wait()
	for k, err := range errs {
		if err != nil {
			t.Fatalf("peer %d: %v", k, err)
		}
	}
	if got := f.origin.ranges.Load(); got != 1 {
		t.Fatalf("origin got %d range requests, want 1", got)
	}
	if got := f.origin.manifests.Load(); got != 1 {
		t.Fatalf("manifest.json fetched %d times, want 1", got)
	}
}

// A consumer's checksums: right ones mark the segments requester-verified
// on the root and the peer; a wrong one keeps the range off both, after the
// root tried the origin once more.
func TestPullThroughRequesterChecksums(t *testing.T) {
	content := pattern(100, 'c')
	good := xxh64.Format(xxh64.Sum64(content[12:18]))
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 12, Length: 6, XXH64: good}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := []bool{false, true, false, false, false, false, false, false, false, false}
	checkHeld(t, st, "w.bin", content, want, store.TrustRequester)
	checkHeld(t, f.rootSt, "w.bin", content, want, store.TrustRequester)

	before := f.origin.ranges.Load()
	bad, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 52, Length: 6, XXH64: "0123456789abcdef"}))
	err := bad.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "xxh64") {
		t.Fatalf("Reconcile = %v, want a checksum error", err)
	}
	if got := f.origin.ranges.Load() - before; got != 2 {
		t.Fatalf("root fetched the bad range %d times, want 2 (once more after the mismatch)", got)
	}
	rootMF, _ := f.rootSt.LoadManifest("glm", "r1")
	rs, _ := f.rootSt.LoadState(rootMF)
	if fs := rs.FileState(rootMF.Files[0]); fs.SegmentsDone[5] {
		t.Fatal("root holds a segment that failed the requester's checksum")
	}
}

// A root that holds the whole model (a NAS with the release) serves any
// range with no WAN traffic.
func TestPullThroughMasterCopyNoWAN(t *testing.T) {
	content := pattern(100, 'm')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, &catalog.Selection{Wanted: []string{"glm"}})
	if err := f.root.Reconcile(context.Background()); err != nil {
		t.Fatalf("root Reconcile: %v", err)
	}
	served := f.origin.bytes.Load()
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 70, Length: 20}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.origin.bytes.Load(); got != served {
		t.Fatalf("origin served %d more bytes for a range the root holds", got-served)
	}
	// The root downloaded it whole, in default-size segments: one here.
	checkHeld(t, st, "w.bin", content, []bool{true}, store.TrustManifest)
}

// Pulling many segments reuses kept-alive connections: no more connections
// to the origin than pull workers.
func TestPullThroughReusesConnections(t *testing.T) {
	content := pattern(400, 'k') // forty segments
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	peer, st := f.peer(&catalog.Selection{Partial: []catalog.Pieces{{Model: "glm"}}})
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.origin.ranges.Load(); got != 40 {
		t.Fatalf("origin got %d range requests, want 40", got)
	}
	if got := f.origin.conns.Load(); got > defaultPullWorkers+1 {
		t.Fatalf("origin saw %d connections for 40 ranges, want at most %d", got, defaultPullWorkers+1)
	}
	got, _ := os.ReadFile(st.FilePath("glm", "r1", "w.bin"))
	if !bytes.Equal(got, content) {
		t.Fatal("content mismatch")
	}
	rootMF, _ := f.rootSt.LoadManifest("glm", "r1")
	rs, _ := f.rootSt.LoadState(rootMF)
	if !rs.FileState(rootMF.Files[0]).Complete {
		t.Fatal("root did not mark the file complete once it held every segment")
	}
}

// A root wanting part of a model publishes a pull-through manifest and
// fetches only its own part from the origin.
func TestRootPartialSelectionPullsOnlyItsPart(t *testing.T) {
	content := pattern(100, 'r')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, rangeSel("w.bin", catalog.ByteRange{Offset: 0, Length: 15}))
	if err := f.root.Reconcile(context.Background()); err != nil {
		t.Fatalf("root Reconcile: %v", err)
	}
	if got := f.origin.bytes.Load(); got != 20 {
		t.Fatalf("origin served %d bytes, want 20", got)
	}
	if ms := f.root.Status().Models; len(ms) != 1 || ms[0].State != server.StatePartial {
		t.Fatalf("root status = %+v, want partial", ms)
	}
}
