package daemon

// Regression (review of #30): pull-through dedup is keyed per chunk but a leader fetches
// only its own caller's segments, so concurrent pulls of *different*
// segments of one chunk serialize behind each other (and the third and
// later callers bypass dedup altogether).

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// Regression (review of #30): pullChunk's once() key is the chunk, and the leader's fetch
// covers only the segments its own caller asked for (missing() closes over
// the caller's segs). In the interleaved release every 64 MiB chunk holds
// halves of every rank, so N ranks asking a root for their halves of one
// chunk at once take ~N x the origin latency: caller 2 waits for caller
// 1's fetch of other bytes, then fetches; caller 3 waits for both, then
// (attempt 2) fetches without dedup. PullWorkers=4 buys nothing for one
// chunk. On main the key was per segment, so disjoint segments ran in
// parallel.
func TestReviewPullChunkSerializesDisjointCallers(t *testing.T) {
	withChunk(t, 1<<20)
	const segSize = 4 << 10
	const delay = 150 * time.Millisecond
	content := pattern(1<<20, 'p') // one chunk of 256 segments
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, nil)
	f.origin.delay = delay
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k4.safetensors", Size: segSize}}
	f.root.cfg.Catalog = &catalog.Catalog{Models: []catalog.Model{f.model}}
	ctx := context.Background()
	if _, err := f.root.Manifest(ctx, "glm", "r1"); err != nil {
		t.Fatal(err)
	}
	before := f.origin.ranges.Load()
	// Four callers, four disjoint ranges of the same chunk, at once, as
	// four ranks ask for their halves.
	const callers = 4
	var wg sync.WaitGroup
	errs := make([]error, callers)
	start := time.Now()
	for k := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(k) * 64 * segSize
			errs[k] = f.root.Pull(ctx, "glm", "r1", "w.k4.safetensors", off, off+3*segSize, nil)
		}()
	}
	wg.Wait()
	took := time.Since(start)
	for k, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", k, err)
		}
	}
	fetches := f.origin.ranges.Load() - before
	t.Logf("%d concurrent pulls of disjoint ranges in one chunk took %v with a %v origin (ideal ~%v with PullWorkers=%d); %d origin requests",
		callers, took, delay, delay, f.root.cfg.PullWorkers, fetches)
	if took > 2*delay {
		t.Errorf("pulls of disjoint segments of one chunk serialized (%v for %d callers, %v each): dedup per chunk makes callers wait on a leader that fetches other bytes. Key the once() by the segment run actually fetched (or have the leader fetch the union of what every waiter wants), and let disjoint runs of a chunk proceed in parallel", took, callers, delay)
	}
}
