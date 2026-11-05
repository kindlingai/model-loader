package transfer

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/store"
)

// hookSync makes syncFile call fn instead of syncing, for the test.
func hookSync(t *testing.T, fn func(f *os.File) error) {
	t.Helper()
	old := syncFile
	syncFile = fn
	t.Cleanup(func() { syncFile = old })
}

// A transfer gives its slot back before it syncs what it wrote, so a
// slow store sync does not hold the source idle: the next segment is
// fetched while the first one syncs (#49).
func TestFetchFileSyncsOutsideTheSlot(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("y"), 20)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	var syncing atomic.Int32
	hookSync(t, func(f *os.File) error {
		if syncing.Add(1) == 1 {
			<-release // the first sync takes until the second segment is fetched
		}
		return f.Sync()
	})
	var opened atomic.Int32
	src := fetcherFunc(func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
		if opened.Add(1) == 2 {
			free()
		}
		return io.NopCloser(bytes.NewReader(content[offset : offset+length])), nil
	})
	done := make(chan error, 1)
	go func() {
		done <- FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{src}, Options{Limit: NewLimiter(1), Flush: NewLimiter(1)})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		free()
		<-done
		t.Fatal("the second segment was not fetched while the first one synced: the slot is held across the sync")
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}

// A store slower than the sources holds them back: no more transfers
// sync at once than Flush has slots.
func TestFetchFileFlushBound(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("z"), 80)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	var mu sync.Mutex
	in, most := 0, 0
	hookSync(t, func(f *os.File) error {
		mu.Lock()
		in++
		most = max(most, in)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		in--
		mu.Unlock()
		return f.Sync()
	})
	if err := FetchFile(context.Background(), st, "m", "r1", fm, store.NewRevisionState(m), []Fetcher{&countingFetcher{content: content}}, Options{Limit: NewLimiter(4), Flush: NewLimiter(1)}); err != nil {
		t.Fatal(err)
	}
	if most != 1 {
		t.Fatalf("%d syncs at once, want at most 1", most)
	}
}

// Stats adds up each transfer's bytes and stages, and Take starts it over.
func TestFetchFileStats(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("q"), 30)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	var stats Stats
	if err := FetchFile(context.Background(), st, "m", "r1", fm, store.NewRevisionState(m), []Fetcher{&countingFetcher{content: content}}, Options{Limit: NewLimiter(2), Stats: &stats}); err != nil {
		t.Fatal(err)
	}
	s := stats.Take()
	if s.Transfers != 3 || s.Bytes != 30 {
		t.Fatalf("stats %+v, want 3 transfers of 30 bytes", s)
	}
	if s.Sync <= 0 {
		t.Fatalf("stats %+v: no sync time", s)
	}
	if s := stats.Take(); s.Transfers != 0 || s.Bytes != 0 {
		t.Fatalf("after Take: %+v, want zero", s)
	}
}

// A run's segments are hashed one by one as they stream in, however the
// writes fall across their boundaries.
func TestSegmentHasher(t *testing.T) {
	content := []byte("0123456789abcdefghijKLMNO")
	fm := buildManifestFile("w.bin", content, 10)
	for _, step := range []int{1, 3, 7, 10, 25} {
		h := newSegmentHasher(fm, 1, 2, &Stats{})
		for p := content[10:]; len(p) > 0; {
			k := min(step, len(p))
			h.Write(p[:k])
			p = p[k:]
		}
		sums := h.done()
		if len(sums) != 2 {
			t.Fatalf("step %d: %d sums, want 2", step, len(sums))
		}
		for j, sum := range sums {
			if got := hex.EncodeToString(sum[:]); got != fm.SegmentSHA256[1+j] {
				t.Fatalf("step %d: segment %d sha256 %s, want %s", step, 1+j, got, fm.SegmentSHA256[1+j])
			}
		}
	}
	h := newSegmentHasher(fm, 0, 2, &Stats{})
	h.Write(content[:15])
	if h.done() != nil {
		t.Fatal("done() of a run cut short is not nil")
	}
}

// throughput fetches a file of n segments of size bytes through slots
// pull slots from a source that reads at about rate bytes a second per
// transfer, onto a store whose sync takes syncTook, and returns the
// bytes a second it got.
func throughput(t *testing.T, n int, size int64, slots int, syncTook time.Duration) float64 {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("t"), n*int(size))
	fm := buildManifestFile("w.bin", content, size)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	hookSync(t, func(f *os.File) error { time.Sleep(syncTook); return nil })
	src := fetcherFunc(func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
		time.Sleep(syncTook) // the transfer itself takes as long as a sync
		return io.NopCloser(bytes.NewReader(content[offset : offset+length])), nil
	})
	start := time.Now()
	if err := FetchFile(context.Background(), st, "m", "r1", fm, store.NewRevisionState(m), []Fetcher{src}, Options{Limit: NewLimiter(slots), Flush: NewLimiter(slots)}); err != nil {
		t.Fatal(err)
	}
	return float64(len(content)) / time.Since(start).Seconds()
}

// With a sync as slow as the transfer, holding a slot across it halves
// what the slots carry; outside it, they carry nearly what they would
// with a store that takes no time (#49).
func TestFetchFileSlowSyncThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	const took = 40 * time.Millisecond
	got := throughput(t, 16, 10, 2, took)
	ideal := float64(2*10) / took.Seconds() // two slots, a segment per took each
	t.Logf("%.0f B/s, ideal %.0f B/s", got, ideal)
	if got < 0.75*ideal {
		t.Fatalf("%.0f B/s with a sync as slow as the transfer, want at least 75%% of %.0f B/s", got, ideal)
	}
}

// Timeout counts from when a transfer has its slot, not while it waits
// for one: a root's long queue of pulls must not eat their time.
func TestFetchFileTimeoutAfterSlot(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("w"), 10)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	slots := NewLimiter(1)
	slots.Acquire(context.Background(), 0) // busy for longer than Timeout
	time.AfterFunc(150*time.Millisecond, slots.Release)
	src := fetcherFunc(func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		return io.NopCloser(bytes.NewReader(content[offset : offset+length])), nil
	})
	rs := store.NewRevisionState(m)
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{src}, Options{Limit: slots, Timeout: 100 * time.Millisecond}); err != nil {
		t.Fatalf("FetchFile: %v (the timeout ran while waiting for the slot)", err)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}
