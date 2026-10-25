package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// Small segments (kindlingai/model-loader#30): a catalog rule gives a
// file segments near the size of the ranges a consumer asks for, so a
// node holds a range at close to its own size, not whole 64 MiB segments.

// countingServer serves h and counts blob requests.
func countingServer(t *testing.T, h http.Handler) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var blobs atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			blobs.Add(1)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts, &blobs
}

// withChunk sets store.ChunkSize for one test.
func withChunk(t *testing.T, n int64) {
	t.Helper()
	old := store.ChunkSize
	store.ChunkSize = n
	t.Cleanup(func() { store.ChunkSize = old })
}

// interleaved is a synthetic width file laid out as gr8's exlr8 release
// is: expert halves of every rank in turn, so every 64 MiB of it holds
// halves of every rank. It returns the file and rank 0's halves.
func interleaved(halves, halfSize, ranks int) ([]byte, []catalog.ByteRange) {
	content := pattern(halves*halfSize, 'h')
	var mine []catalog.ByteRange
	for h := range halves {
		if h%ranks == 0 {
			mine = append(mine, catalog.ByteRange{Offset: int64(h * halfSize), Length: int64(halfSize)})
		}
	}
	return content, mine
}

// The issue's acceptance test: a request for one rank's halves of an
// interleaved width file holds at most about 1.5x the bytes of its ranges
// on disk, on the peer and on the root that pulled them through, and the
// state file stays small.
func TestSmallSegmentsHoldNearRangeSize(t *testing.T) {
	withChunk(t, 1<<20)
	const (
		halves   = 256
		halfSize = 200_000 // not a multiple of the segment size, as in the release
		ranks    = 4
		segSize  = 64 << 10
	)
	content, mine := interleaved(halves, halfSize, ranks)
	var want int64
	for _, r := range mine {
		want += r.Length
	}

	f := newPullFleet(t, map[string][]byte{"experts/layer-003.k4.safetensors": content, "config.json": pattern(100, 'c')}, nil)
	f.root.cfg.SegmentSize = 0 // 64 MiB, but for the rule
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "experts/*.k?.safetensors", Size: segSize}}
	f.root.cfg.Catalog = &catalog.Catalog{Models: []catalog.Model{f.model}}
	peer, st := f.peer(&catalog.Selection{})
	peer.cfg.Catalog = f.root.cfg.Catalog
	reqs, err := request.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer.cfg.Requests = reqs
	if _, err := peer.PutRequest(request.Request{Name: "gr8-rank0", Pieces: catalog.Pieces{Model: "glm",
		Include: []string{"config.json"},
		Ranges:  []catalog.FileRanges{{Path: "experts/layer-003.k4.safetensors", Ranges: mine}}}}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rs, _ := peer.RequestStatus("gr8-rank0"); rs.State != server.StateReady {
		t.Fatalf("request: %s (%s), want ready", rs.State, rs.Error)
	}

	mf, err := st.LoadManifest("glm", "r1")
	if err != nil {
		t.Fatal(err)
	}
	fm, _ := mf.Find("experts/layer-003.k4.safetensors")
	if fm.SegmentSize != segSize || fm.Hashed() {
		t.Fatalf("manifest entry %+v, want %d byte segments with no segment hashes", fm, segSize)
	}
	for name, s := range map[string]*store.Store{"peer": st, "root": f.rootSt} {
		held := s.FileAllocated("glm", "r1", fm.Path)
		t.Logf("%s: %d bytes on disk for %d bytes of ranges (%.2fx); the file is %d bytes", name, held, want, float64(held)/float64(want), len(content))
		if ratio := float64(held) / float64(want); ratio > 1.5 {
			t.Errorf("%s holds %d bytes on disk for %d bytes of ranges: %.2fx, want at most 1.5x", name, held, want, ratio)
		}
		// Every byte of the ranges is the origin's, at its offset.
		got, err := os.ReadFile(s.FilePath("glm", "r1", fm.Path))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range mine {
			if !bytes.Equal(got[r.Offset:r.Offset+r.Length], content[r.Offset:r.Offset+r.Length]) {
				t.Fatalf("%s: range at %d differs from the origin's", name, r.Offset)
			}
		}
	}
	if got := f.origin.bytes.Load(); float64(got) > 1.5*float64(want)+100 {
		t.Errorf("origin served %d bytes for %d bytes of ranges", got, want)
	}
	// Runs of segments go as one range each: far fewer requests than
	// segments.
	segs := 0
	rs, _ := st.LoadState(mf)
	for _, d := range rs.FileState(fm).SegmentsDone {
		if d {
			segs++
		}
	}
	if n := int(f.origin.ranges.Load()); n >= segs {
		t.Errorf("origin got %d range requests for %d segments, want fewer (runs)", n, segs)
	}

	// The store contract: held, as index ranges, and no segments_done
	// for a file of small segments.
	var raw struct {
		Files []map[string]json.RawMessage `json:"files"`
	}
	data, err := os.ReadFile(filepath.Join(st.Dir(), "state", "glm", "r1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, f := range raw.Files {
		var path string
		json.Unmarshal(f["path"], &path)
		_, hasHeld := f["held"]
		_, hasDone := f["segments_done"]
		if !hasHeld || hasDone != (path == "config.json") { // config.json: 100 bytes, one 64 MiB segment
			t.Errorf("state of %s has held %v, segments_done %v; want held always, segments_done only for 64 MiB segments", path, hasHeld, hasDone)
		}
	}
	cmf, files := consumerRead(t, st.Dir(), "glm", "r1")
	for _, r := range mine {
		if !consumerVerified(cmf, files, fm.Path, r.Offset, r.Length) {
			t.Fatalf("a consumer reading the store finds [%d, +%d) not verified", r.Offset, r.Length)
		}
	}
	if consumerVerified(cmf, files, fm.Path, int64(halfSize), int64(halfSize)) {
		t.Fatal("a consumer reading the store finds rank 1's half verified")
	}
}

// A second peer copies the ranges from the first one, which holds them
// in small segments with no hashes in the manifest: each segment is
// checked against the sha256 the first peer vouches for, many segments a
// request, and nothing more comes from the root or the origin.
func TestSmallSegmentsPeerToPeer(t *testing.T) {
	withChunk(t, 1<<20)
	const segSize = 16 << 10
	content, mine := interleaved(64, 50_000, 4)
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, nil)
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k?.safetensors", Size: segSize}}
	cat := &catalog.Catalog{Models: []catalog.Model{f.model}}
	f.root.cfg.Catalog = cat
	sel := &catalog.Selection{Partial: []catalog.Pieces{{Model: "glm", Ranges: []catalog.FileRanges{{Path: "w.k4.safetensors", Ranges: mine}}}}}
	first, firstSt := f.peer(sel)
	first.cfg.Catalog = cat
	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatalf("first peer: %v", err)
	}
	fromOrigin := f.origin.bytes.Load()

	srv := server.New(firstSt, first)
	ts, blobs := countingServer(t, srv)
	st2, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	second := New(Config{
		NodeID: "spark-2", Roles: []string{"peer"}, Store: st2, Catalog: cat, Selection: sel,
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}),
	})
	if err := second.Reconcile(context.Background()); err != nil {
		t.Fatalf("second peer: %v", err)
	}
	if got := f.origin.bytes.Load(); got != fromOrigin {
		t.Fatalf("origin served %d more bytes for the second peer, want none", got-fromOrigin)
	}
	mf, _ := st2.LoadManifest("glm", "r1")
	fm, _ := mf.Find("w.k4.safetensors")
	rs, _ := st2.LoadState(mf)
	fs := rs.FileState(fm)
	held := 0
	for i, d := range fs.SegmentsDone {
		if d {
			held++
			if fs.TrustOf(i) != store.TrustOrigin {
				t.Fatalf("segment %d trust %q, want origin (kept from the first peer)", i, fs.TrustOf(i))
			}
		}
	}
	if n := int(blobs.Load()); n >= held || held == 0 {
		t.Fatalf("%d blob requests for %d segments, want fewer (runs)", n, held)
	}
	got, _ := os.ReadFile(st2.FilePath("glm", "r1", fm.Path))
	for _, r := range mine {
		if !bytes.Equal(got[r.Offset:r.Offset+r.Length], content[r.Offset:r.Offset+r.Length]) {
			t.Fatalf("range at %d differs from the origin's", r.Offset)
		}
	}
	// A third node asks the second, which can vouch only through its side
	// file: every segment it holds has a sha256 there.
	sums := st2.SegmentSums("glm", "r1", fm, 0, fm.NumSegments())
	for i, d := range fs.SegmentsDone {
		if d && sums[i] == "" {
			t.Fatalf("held segment %d has no sha256 in the side file", i)
		}
	}
}

// A peer with a bad copy of one small segment is caught by the sha256
// its source vouches for, not trusted.
func TestSmallSegmentsBadClaimRejected(t *testing.T) {
	withChunk(t, 1<<20)
	const segSize = 4 << 10
	content := pattern(64<<10, 'b')
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, nil)
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k4.safetensors", Size: segSize}}
	cat := &catalog.Catalog{Models: []catalog.Model{f.model}}
	f.root.cfg.Catalog = cat
	sel := rangeSel("w.k4.safetensors", catalog.ByteRange{Offset: 0, Length: 16 << 10})
	first, firstSt := f.peer(sel)
	first.cfg.Catalog = cat
	if err := first.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Damage segment 2 on disk; the first peer still says it holds it.
	path := firstSt.FilePath("glm", "r1", "w.k4.safetensors")
	fh, _ := os.OpenFile(path, os.O_RDWR, 0)
	fh.WriteAt([]byte("XXXX"), 2*segSize+5)
	fh.Close()

	srv := server.New(firstSt, first)
	ts, _ := countingServer(t, srv)
	st2, _ := store.Open(t.TempDir())
	second := New(Config{
		NodeID: "spark-2", Roles: []string{"peer"}, Store: st2, Catalog: cat, Selection: sel,
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String(), f.rootTS.Listener.Addr().String()}),
	})
	if err := second.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _ := os.ReadFile(st2.FilePath("glm", "r1", "w.k4.safetensors"))
	if !bytes.Equal(got[:16<<10], content[:16<<10]) {
		t.Fatal("the second peer holds the bad copy")
	}
}

// Eviction removes a chunk of small segments at once, and the state on
// disk stays well under the size of one bool a segment.
func TestSmallSegmentsEvictByChunk(t *testing.T) {
	withChunk(t, 64<<10)
	const segSize = 4 << 10
	content := pattern(1<<20, 'e') // 256 segments, 16 chunks
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, nil)
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k4.safetensors", Size: segSize}}
	cat := &catalog.Catalog{Models: []catalog.Model{f.model}}
	f.root.cfg.Catalog = cat
	f.root.cfg.DiskBudget = 1 << 40
	sel := &catalog.Selection{Wanted: []string{"glm"}}
	f.root.cfg.Selection = &catalog.Selection{}
	peer, st := f.peer(&catalog.Selection{Partial: []catalog.Pieces{{Model: "glm", Ranges: []catalog.FileRanges{{Path: "w.k4.safetensors", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1 << 20}}}}}}})
	peer.cfg.Catalog = cat
	if err := peer.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = sel
	// The root holds all of it; it can fetch it again, so it may evict.
	mf, _ := f.rootSt.LoadManifest("glm", "r1")
	fm, _ := mf.Find("w.k4.safetensors")
	rs, _ := f.root.revState(mf)
	vs := heldChunks(mf, rs, fm)
	if len(vs) != 16 || len(vs[0].segs) != 16 || vs[0].bytes != 64<<10 {
		t.Fatalf("victims: %d of %d segments, %d bytes; want 16 chunks of 16 segments, 64 KiB", len(vs), len(vs[0].segs), vs[0].bytes)
	}
	before := f.rootSt.FileAllocated("glm", "r1", fm.Path)
	if ok, err := f.root.evict(vs[3]); !ok || err != nil {
		t.Fatalf("evict: %v, %v", ok, err)
	}
	after := f.rootSt.FileAllocated("glm", "r1", fm.Path)
	if before-after < 64<<10 {
		t.Fatalf("evicting a 64 KiB chunk freed %d bytes", before-after)
	}
	saved, _ := f.rootSt.LoadState(mf)
	sfs := saved.FileState(fm)
	for i, d := range sfs.SegmentsDone {
		if d == (i >= 48 && i < 64) {
			t.Fatalf("segment %d held = %v after evicting chunk 3", i, d)
		}
	}
	if !f.root.inUse(victim{mf: mf, fm: fm, chunk: 5}) {
		done := f.root.BeginUse("glm", "r1", fm, 5*64<<10+100, 5*64<<10+200)
		if !f.root.inUse(victim{mf: mf, fm: fm, chunk: 5}) {
			t.Fatal("a chunk being served is not in use")
		}
		done()
	}
	_ = st
}

// State for the release's scale stays small: tens of MB at most for 821
// GB of width files in 64 KiB segments, and it loads quickly.
func TestSmallSegmentsStateScale(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("large, and timed")
	}
	withChunk(t, store.DefaultSegmentSize)
	const segSize = 64 << 10
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// 821 GB as 230 width files of about 3.57 GB.
	m := &store.Manifest{Model: "glm", Revision: "r1"}
	const files, size = 230, int64(3_570_000_000)
	for i := range files {
		m.Files = append(m.Files, store.FileManifest{Path: fmt.Sprintf("experts/layer-%03d.k%d.safetensors", i/3, 2+i%3), Size: size, SegmentSize: segSize})
	}
	rs := store.NewRevisionState(m)
	// One rank's halves at TP=4: every fourth run of three segments held,
	// some origin and some requester trust; a fully held file too.
	for fi, fm := range m.Files {
		fs := rs.FileState(fm)
		for i := range fs.SegmentsDone {
			fs.SegmentsDone[i] = fi == 0 || (i/3)%4 == 0
		}
		rs.SetFileState(fs)
		for i := 0; i < fm.NumSegments(); i += 12 {
			rs.SetTrust(fm, i, store.TrustOrigin)
			if i%120 == 0 {
				rs.SetTrust(fm, i, store.TrustRequester)
			}
		}
	}
	start := time.Now()
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	saveTook := time.Since(start)
	info, err := os.Stat(filepath.Join(st.Dir(), "state", "glm", "r1.json"))
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	got, err := st.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	loadTook := time.Since(start)
	t.Logf("state: %d segments, %.1f MB on disk, saved in %v, loaded in %v",
		files*m.Files[0].NumSegments(), float64(info.Size())/1e6, saveTook, loadTook)
	if info.Size() > 50<<20 {
		t.Errorf("state file is %d bytes, want tens of MB at most", info.Size())
	}
	if loadTook > 10*time.Second {
		t.Errorf("state took %v to load", loadTook)
	}
	fs := got.FileState(m.Files[1])
	if !fs.SegmentsDone[0] || fs.SegmentsDone[3] || !fs.SegmentsDone[12] || fs.TrustOf(12) != store.TrustOrigin || fs.TrustOf(120) != store.TrustRequester || fs.TrustOf(1) != store.TrustManifest {
		t.Fatal("state did not round-trip")
	}
}
