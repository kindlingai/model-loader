package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	digest    func(name string, sum string) string // rewrites a file's sha256 in manifest.json
	onRange   func(path, rangeHeader string)       // called for each file request
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
			SHA256 string `json:"sha256,omitempty"`
		}
		var hm struct {
			Files []file `json:"files"`
		}
		for _, name := range sortedKeys(o.files) {
			sum := sha256.Sum256(o.files[name])
			d := hex.EncodeToString(sum[:])
			if o.digest != nil {
				d = o.digest(name, d)
			}
			hm.Files = append(hm.Files, file{name, int64(len(o.files[name])), d})
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
	if o.onRange != nil {
		o.onRange(p, r.Header.Get("Range"))
	}
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

func newPullFleet(t *testing.T, files map[string][]byte, rootSel *catalog.Selection, opts ...func(*fakeOrigin)) *pullFleet {
	t.Helper()
	f := &pullFleet{t: t, origin: newFakeOrigin(t, files)}
	for _, o := range opts {
		o(f.origin)
	}
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
		NodeID: "spark", Roles: []string{"peer"}, Store: st, SegmentSize: 10,
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
	// The root checked it whole; the peer holds the root's word for it.
	checkHeld(t, st, "w.bin", content, []bool{7: true, 8: true, 9: false}, store.TrustOrigin)
}

// A root that wants a model whole publishes its manifest before it has
// downloaded it, and fetches several files at once: while one file is
// held up at the origin it gets the next, and a peer gets that one from it.
func TestRootWholeServesWhileDownloading(t *testing.T) {
	a, b := pattern(20, 'a'), pattern(20, 'b')
	held := make(chan struct{})
	f := newPullFleet(t, map[string][]byte{"a.bin": a, "b.bin": b}, &catalog.Selection{Wanted: []string{"glm"}}, func(o *fakeOrigin) {
		o.onRange = func(path, _ string) {
			if path == "a.bin" {
				<-held
			}
		}
	})
	release := sync.OnceFunc(func() { close(held) })
	var rootErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		rootErr = f.root.Reconcile(context.Background())
	}()
	t.Cleanup(func() { release(); <-done }) // before the origin closes

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if mf, err := f.rootSt.LoadManifest("glm", "r1"); err == nil {
			rs, _ := f.root.revState(mf)
			if rs.FileState(mf.Files[1]).Complete {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("root did not get b.bin while a.bin was held up")
		}
	}
	peer, st := f.peer(&catalog.Selection{Partial: []catalog.Pieces{{Model: "glm", Include: []string{"b.bin"}}}})
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("peer Reconcile: %v", err)
	}
	checkHeld(t, st, "b.bin", b, []bool{true, true}, store.TrustManifest) // checked whole
	select {
	case <-done:
		t.Fatalf("root finished (%v) with a.bin held up", rootErr)
	default:
	}

	release()
	if <-done; rootErr != nil {
		t.Fatalf("root Reconcile: %v", rootErr)
	}
	if ms := f.root.Status().Models; len(ms) != 1 || ms[0].State != server.StateReady {
		t.Fatalf("root status = %+v, want ready", ms)
	}
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

func blobGet(t *testing.T, addr, path string, start, end int64, checks string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/blob/glm/r1/"+path, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	if checks != "" {
		req.Header.Set(server.HeaderRangeXXH64, checks)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// A file whose whole-file sha256 fails after every segment came from the
// origin is not served on the next request: its segments are dropped and
// fetched again.
func TestPullThroughWholeFileFailureNotServed(t *testing.T) {
	content := pattern(20, 'w')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil, func(o *fakeOrigin) {
		o.digest = func(string, string) string { return strings.Repeat("0", 64) }
	})
	addr := f.rootTS.Listener.Addr().String()
	f.root.Manifest(context.Background(), "glm", "r1")
	if code := blobGet(t, addr, "w.bin", 0, 20, ""); code != http.StatusBadGateway {
		t.Fatalf("first GET = %d, want 502", code)
	}
	before := f.origin.ranges.Load()
	if code := blobGet(t, addr, "w.bin", 0, 20, ""); code != http.StatusBadGateway {
		t.Fatalf("second GET = %d, want 502: bytes that failed the whole-file check were served", code)
	}
	if f.origin.ranges.Load() == before {
		t.Fatal("no new origin fetch after the whole-file failure")
	}
}

// A checksum given after its range was fetched without one is checked
// against the held bytes; a wrong one is not hidden by the held segments.
func TestNewChecksumForHeldRange(t *testing.T) {
	content := pattern(100, 'n')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 12, Length: 6}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := peer.cfg
	cfg.Selection = rangeSel("w.bin", catalog.ByteRange{Offset: 12, Length: 6, XXH64: "0123456789abcdef"})
	again := New(cfg)
	if err := again.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile = nil with a wrong checksum for a held range")
	}
	if ms := again.Status().Models; len(ms) == 1 && ms[0].State == server.StatePartial {
		t.Fatalf("status = %+v: reports the wanted part held", ms)
	}
	_ = st
}

// A checksum that spans two segments reaches the root, which fetches both
// and checks it before it serves either.
func TestSpanningChecksumReachesRoot(t *testing.T) {
	content := pattern(100, 's')
	good := xxh64.Format(xxh64.Sum64(content[25:35]))
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 25, Length: 10, XXH64: good}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	want := []bool{false, false, true, true, false, false, false, false, false, false}
	checkHeld(t, f.rootSt, "w.bin", content, want, store.TrustRequester)
	checkHeld(t, st, "w.bin", content, want, store.TrustRequester)

	before := f.origin.ranges.Load()
	bad, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 55, Length: 10, XXH64: "0123456789abcdef"}))
	if err := bad.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile = nil with a wrong spanning checksum")
	}
	rootMF, _ := f.rootSt.LoadManifest("glm", "r1")
	rs, _ := f.rootSt.LoadState(rootMF)
	if fs := rs.FileState(rootMF.Files[0]); fs.SegmentsDone[5] || fs.SegmentsDone[6] {
		t.Fatalf("root holds segments a spanning checksum failed: %v", fs.SegmentsDone)
	}
	if got := f.origin.ranges.Load() - before; got < 4 {
		t.Fatalf("root fetched %d ranges for the bad check, want both segments twice", got)
	}
}

// A peer whose manifest came from a pull-through root (no segment hashes)
// can copy from a NAS whose manifest has them.
func TestLazyManifestPeerUsesHashedNAS(t *testing.T) {
	content := pattern(30, 'h')
	files := map[string][]byte{"w.bin": content}
	f := newPullFleet(t, files, nil)
	lazyMF, err := f.root.Manifest(context.Background(), "glm", "r1")
	if err != nil {
		t.Fatal(err)
	}
	nasStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, nasStore, "glm", 10, files, "w.bin")
	nas := httptest.NewServer(server.New(nasStore, fakeStatus{server.StatusResponse{NodeID: "nas2", Roles: []string{"peer"}}}))
	t.Cleanup(nas.Close)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(lazyMF); err != nil {
		t.Fatal(err)
	}
	peer := New(Config{
		NodeID: "spark", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: allSelection(),
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{nas.Listener.Addr().String()}),
	})
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ms := peer.Status().Models; len(ms) != 1 || ms[0].State != server.StateReady {
		t.Fatalf("status = %+v, want ready", ms)
	}
	if f.origin.bytes.Load() != 0 {
		t.Fatal("origin was used")
	}
}

// An empty file the origin gives no digest for completes.
func TestPullThroughEmptyFileNoDigest(t *testing.T) {
	f := newPullFleet(t, map[string][]byte{"empty.txt": {}, "w.bin": pattern(10, 'e')}, nil, func(o *fakeOrigin) {
		o.digest = func(string, string) string { return "" }
	})
	peer, st := f.peer(&catalog.Selection{Partial: []catalog.Pieces{{Model: "glm"}}})
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	mf, _ := st.LoadManifest("glm", "r1")
	rs, _ := st.LoadState(mf)
	for _, fm := range mf.Files {
		if !rs.FileState(fm).Complete {
			t.Fatalf("%s not complete", fm.Path)
		}
	}
}

// A root filling a pull takes a segment from a LAN node that holds it
// before the origin.
func TestPullThroughPrefersLAN(t *testing.T) {
	content := pattern(100, 'l')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	first, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 40, Length: 5}))
	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := f.origin.bytes.Load()

	// A second root, knowing only the first, for a peer of its own.
	st2, _ := store.Open(t.TempDir())
	root2 := New(Config{
		NodeID: "nas2", Roles: []string{"root"}, Store: st2,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: &catalog.Selection{},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{f.rootTS.Listener.Addr().String()}),
		SegmentSize: 10,
	})
	srv2 := server.New(st2, root2)
	srv2.SetPuller(root2)
	ts2 := httptest.NewServer(srv2)
	t.Cleanup(ts2.Close)
	if _, err := root2.Manifest(context.Background(), "glm", "r1"); err != nil {
		t.Fatal(err)
	}
	if code := blobGet(t, ts2.Listener.Addr().String(), "w.bin", 40, 50, ""); code != http.StatusPartialContent {
		t.Fatalf("GET from the second root = %d", code)
	}
	if got := f.origin.bytes.Load(); got != served {
		t.Fatalf("origin served %d more bytes for a segment a LAN root holds", got-served)
	}
}

// Once a pull-through file is whole and matches the origin's digest, its
// bytes are the origin's: a wrong consumer checksum does not drop them or
// cause more origin traffic. A peer that copies all of it checks the whole
// file and trusts it the same. One that copies part of it has only the
// root's word for that, so it checks a consumer checksum itself (#35).
func TestCompletedLazyFileTrustsOrigin(t *testing.T) {
	content := pattern(40, 't')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	whole, wst := f.peer(&catalog.Selection{Partial: []catalog.Pieces{{Model: "glm"}}})
	if err := whole.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	all := []bool{true, true, true, true}
	checkHeld(t, wst, "w.bin", content, all, store.TrustManifest)
	served := f.origin.bytes.Load()
	bad, bst := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 5, Length: 3, XXH64: "0123456789abcdef"}))
	if err := bad.Reconcile(context.Background()); !errors.Is(err, store.ErrCheckFailed) {
		t.Fatalf("Reconcile = %v, want a failed consumer checksum", err)
	}
	checkHeld(t, bst, "w.bin", content, []bool{false}, "")
	checkHeld(t, f.rootSt, "w.bin", content, all, store.TrustManifest)
	if f.origin.bytes.Load() != served {
		t.Fatal("origin refetched bytes that matched its own digest")
	}
}

// A root whose LAN source sends a bad copy (consistent with its own
// recorded sha256) gets the segment from the origin when a consumer
// checksum shows it is wrong.
func TestPullThroughBadLANCopyRetriesOrigin(t *testing.T) {
	content := pattern(100, 'b')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	first, _ := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 40, Length: 5}))
	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Corrupt the first root's copy of segment 4 and its recorded hash.
	bad := bytes.Repeat([]byte("Z"), 10)
	fp := f.rootSt.FilePath("glm", "r1", "w.bin")
	fh, _ := os.OpenFile(fp, os.O_WRONLY, 0)
	fh.WriteAt(bad, 40)
	fh.Close()
	mf, _ := f.rootSt.LoadManifest("glm", "r1")
	rs, _ := f.root.revState(mf)
	sum := sha256.Sum256(bad)
	rs.MarkSegment(mf.Files[0], 4, hex.EncodeToString(sum[:]), store.TrustOrigin)
	f.rootSt.SaveState(rs)

	st2, _ := store.Open(t.TempDir())
	root2 := New(Config{
		NodeID: "nas2", Roles: []string{"root"}, Store: st2,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: &catalog.Selection{},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{f.rootTS.Listener.Addr().String()}),
		SegmentSize: 10,
	})
	srv2 := server.New(st2, root2)
	srv2.SetPuller(root2)
	ts2 := httptest.NewServer(srv2)
	t.Cleanup(ts2.Close)
	if _, err := root2.Manifest(context.Background(), "glm", "r1"); err != nil {
		t.Fatal(err)
	}
	served := f.origin.bytes.Load()
	check := store.FormatChecks([]store.RangeCheck{{Offset: 40, Length: 10, XXH64: xxh64.Sum64(content[40:50])}})
	if code := blobGet(t, ts2.Listener.Addr().String(), "w.bin", 40, 50, check); code != http.StatusPartialContent {
		t.Fatalf("GET = %d, want 206 after an origin retry", code)
	}
	if f.origin.bytes.Load() == served {
		t.Fatal("no origin fetch after the LAN copy failed the checksum")
	}
	checkHeld(t, st2, "w.bin", content, []bool{false, false, false, false, true, false, false, false, false, false}, store.TrustRequester)
}

// A new checksum for a held range whose file was cut short drops what it
// covers, and the range is fetched again.
func TestNewChecksumForTruncatedFile(t *testing.T) {
	content := pattern(100, 'k')
	f := newPullFleet(t, map[string][]byte{"w.bin": content}, nil)
	peer, st := f.peer(rangeSel("w.bin", catalog.ByteRange{Offset: 12, Length: 6}))
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	os.Truncate(st.FilePath("glm", "r1", "w.bin"), 0)
	cfg := peer.cfg
	cfg.Selection = rangeSel("w.bin", catalog.ByteRange{Offset: 12, Length: 6, XXH64: xxh64.Format(xxh64.Sum64(content[12:18]))})
	again := New(cfg)
	if err := again.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _ := os.ReadFile(st.FilePath("glm", "r1", "w.bin"))
	if len(got) < 20 || !bytes.Equal(got[10:20], content[10:20]) {
		t.Fatal("the range was not fetched again")
	}
}

// A root builds a manifest it does not hold to the end even when the peer
// that asked gives up, and saves it, so the next asking finds it rather
// than starting the build over.
func TestLazyManifestOutlivesTheAsker(t *testing.T) {
	f := newPullFleet(t, map[string][]byte{"w.bin": pattern(20, 'w')}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.root.Manifest(ctx, "glm", "r1")
	if _, err := f.rootSt.LoadManifest("glm", "r1"); err != nil {
		t.Fatalf("no manifest saved after the asker went away: %v", err)
	}
}
