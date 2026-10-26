package daemon

// Scale tests from the review of #30: the release's shape in memory (230
// width files of ~3.57 GB in 64 KiB segments, ~12.5M segments) with one
// rank's halves held, timing daemon operations that run often.

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/want"
)

const (
	rbFiles    = 230
	rbFileSize = int64(3_570_000_000)
	rbSegSize  = int64(64 << 10)
)

// rbManifest is the release: 230 width files of 64 KiB segments, no
// segment hashes (a root's pull-through manifest).
func rbManifest() *store.Manifest {
	m := &store.Manifest{Model: "glm", Revision: "r1"}
	for i := range rbFiles {
		m.Files = append(m.Files, store.FileManifest{
			Path: fmt.Sprintf("experts/layer-%03d.k%d.safetensors", i/3, 2+i%3),
			Size: rbFileSize, SegmentSize: rbSegSize,
			SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		})
	}
	return m
}

// rbState holds one rank's halves at TP=4 (every fourth run of three
// segments) of every file, with file 0 held whole, as the PR's own scale
// test does.
func rbState(m *store.Manifest) *store.RevisionState {
	rs := store.NewRevisionState(m)
	for fi := range rs.Files {
		done := rs.Files[fi].SegmentsDone
		for i := range done {
			done[i] = fi == 0 || (i/3)%4 == 0
		}
		rs.Files[fi].Complete = fi == 0
		rs.Files[fi].Used = make([]int64, m.Files[fi].NumChunks())
	}
	return rs
}

func rbDaemon(t *testing.T, st *store.Store, m *store.Manifest, rs *store.RevisionState) *Daemon {
	t.Helper()
	cat := &catalog.Catalog{Models: []catalog.Model{{Name: "glm", Revision: "r1",
		Source:       catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"},
		SegmentSizes: []catalog.SegmentSizeRule{{Path: "experts/*", Size: rbSegSize}}}}}
	reqs, err := request.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: "nas", Roles: []string{"root"}, Store: st, Catalog: cat,
		Selection: &catalog.Selection{}, Requests: reqs, Sources: source.New(nil, ""),
		Discoverer: discovery.NewStatic(nil), DiskBudget: 1 << 50,
	})
	d.stateMu.Lock()
	d.states[want.Key(m.Model, m.Revision)] = rs
	d.stateMu.Unlock()
	return d
}

func rbHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// useMuProbe measures how long d.use (what every blob request's BeginUse
// takes) is blocked while fn runs.
func useMuProbe(d *Daemon, fn func()) (fnTook, maxBlocked time.Duration) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			t0 := time.Now()
			d.use([]string{"probe"})()
			if dt := time.Since(t0); dt > maxBlocked {
				maxBlocked = dt
			}
			time.Sleep(time.Millisecond)
		}
	}()
	t0 := time.Now()
	fn()
	fnTook = time.Since(t0)
	close(stop)
	wg.Wait()
	return fnTook, maxBlocked
}

func TestScaleReservations(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("large, and timed")
	}
	withChunk(t, store.DefaultSegmentSize)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := rbManifest()
	rs := store.NewRevisionState(m) // nothing held: a node about to fetch a whole rank's worth
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	d := rbDaemon(t, st, m, rs)
	// A request for one rank's halves of every width file: every fourth
	// run of three segments.
	before := rbHeap()
	t0 := time.Now()
	var needs []spaceNeed
	total := 0
	for _, fm := range m.Files {
		segs := make([]bool, fm.NumSegments())
		for i := range segs {
			segs[i] = (i/3)%4 == 0
			if segs[i] {
				total++
			}
		}
		needs = append(needs, segmentNeeds(m, rs, fm, segs, 4096)...)
	}
	built := time.Since(t0)
	heapMB := int64(rbHeap()-before) / 1e6
	d.diskMu.Lock()
	for _, n := range needs {
		d.reserved[n.key] = &reservation{refs: 1, remaining: n.remaining, chunk: n.chunk}
	}
	t0 = time.Now()
	const k = 5
	for range k {
		d.fitsLocked()
	}
	fits := time.Since(t0) / k
	d.diskMu.Unlock()
	t.Logf("%d reserved segments in %d chunkNeeds: built in %v, %d MB of heap; fitsLocked takes %v (once per batch of victims)",
		total, len(needs), built, heapMB, fits)
	if fits > 20*time.Millisecond {
		t.Errorf("fitsLocked takes %v with one rank's reservations held: too slow", fits)
	}
	if heapMB > 64 {
		t.Errorf("reservations take %d MB of heap for %d segments", heapMB, total)
	}
}

// Evicting every chunk of a file saves the state once, and blob requests
// (d.use) are not held up while it does.
func TestScaleEvictBatch(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("large, and timed")
	}
	withChunk(t, store.DefaultSegmentSize)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := rbManifest()
	rs := rbState(m)
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	d := rbDaemon(t, st, m, rs)
	fm := m.Files[0]
	f, err := st.EnsureFile(m.Model, m.Revision, fm)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	vs := heldChunks(m, rs, fm)
	took, blocked := useMuProbe(d, func() {
		if n, errs := d.evictBatchN(vs); n != len(vs) || len(errs) > 0 {
			t.Fatalf("evicted %d of %d: %v", n, len(vs), errs)
		}
	})
	t.Logf("evicting %d chunks in one batch took %v; d.use blocked up to %v", len(vs), took, blocked)
	if took > 2*time.Second || blocked > 50*time.Millisecond {
		t.Errorf("evicting %d chunks took %v, d.use blocked %v", len(vs), took, blocked)
	}
}

func TestScaleRequestStatus(t *testing.T) {
	if testing.Short() || raceOn {
		t.Skip("large, and timed")
	}
	withChunk(t, store.DefaultSegmentSize)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := rbManifest()
	rs := rbState(m)
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	d := rbDaemon(t, st, m, rs)
	// One rank's halves of every width file, as gr8 asks: a few hundred
	// ranges per file.
	var fr []catalog.FileRanges
	for _, fm := range m.Files {
		var ranges []catalog.ByteRange
		for off := int64(0); off+3*rbSegSize <= fm.Size && len(ranges) < 200; off += 12 * rbSegSize {
			ranges = append(ranges, catalog.ByteRange{Offset: off, Length: 3 * rbSegSize})
		}
		fr = append(fr, catalog.FileRanges{Path: fm.Path, Ranges: ranges})
	}
	r := request.Request{Name: "rank0", Pieces: catalog.Pieces{Model: "glm", Revision: "r1", Ranges: fr}}
	if err := request.Resolve(d.cfg.Catalog, &r); err != nil {
		t.Fatal(err)
	}
	if _, err := d.cfg.Requests.Put(r); err != nil {
		t.Fatal(err)
	}
	d.requestStatus(r) // warm
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	const k = 5
	t0 := time.Now()
	for range k {
		st := d.requestStatus(r)
		if st.State != server.StateReady {
			t.Fatalf("state %s (%s), want ready", st.State, st.Error)
		}
	}
	per := time.Since(t0) / k
	runtime.ReadMemStats(&ms1)
	allocMB := float64(ms1.TotalAlloc-ms0.TotalAlloc) / k / 1e6
	t.Logf("requestStatus: %v and %.0f MB allocated per call; 8 ranks waiting poll it 32x/s: %v of CPU and %.1f GB/s of garbage",
		per, allocMB, per*32, allocMB*32/1e3)
	if per > 20*time.Millisecond || allocMB > 20 {
		t.Errorf("requestStatus costs %v / %.0f MB per call at release scale", per, allocMB)
	}
}
