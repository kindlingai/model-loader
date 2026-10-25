package daemon

// Regression (review of #30): pullChunk's attempt-2 fallback bypasses dedup, so callers that
// lost two leader elections for a chunk all fetch the same segments from
// the origin at once.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// Regression (review of #30): pullChunk dedups per chunk, but a leader fetches only its own
// caller's missing segments. A caller that waited on two leaders that
// fetched other segments (attempt == 2) calls pullFetch directly, with no
// once() around it, so every such caller wanting the same segment fetches
// it from the origin itself: duplicate WAN requests, and concurrent
// writers of the same file region (identical bytes, so no corruption, but
// the "one fetch per segment however many ask" guarantee in the design
// doc no longer holds).
//
// Deterministic shape: one leader A for segment 0; five callers for
// segment 1 and five for segment 2 all waiting on A. One of the ten wins
// attempt 1; the five of the other group all lose twice and each fetch
// their segment themselves: 5 origin requests for one segment.
func TestReviewPullChunkDuplicatesOriginFetches(t *testing.T) {
	withChunk(t, 1<<20)
	const segSize = 4 << 10
	const delay = 120 * time.Millisecond
	content := pattern(1<<20, 'd') // one chunk of 256 segments
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, nil)
	f.origin.delay = delay
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k4.safetensors", Size: segSize}}
	f.root.cfg.Catalog = &catalog.Catalog{Models: []catalog.Model{f.model}}
	f.root.cfg.PullWorkers = 16
	ctx := context.Background()
	if _, err := f.root.Manifest(ctx, "glm", "r1"); err != nil {
		t.Fatal(err)
	}
	before := f.origin.ranges.Load()
	var wg sync.WaitGroup
	pull := func(seg int64) {
		defer wg.Done()
		if err := f.root.Pull(ctx, "glm", "r1", "w.k4.safetensors", seg*segSize, (seg+1)*segSize, nil); err != nil {
			t.Error(err)
		}
	}
	wg.Add(1)
	go pull(0)
	time.Sleep(delay / 3) // A is the leader for the chunk, fetching segment 0.
	for range 5 {
		wg.Add(2)
		go pull(1)
		go pull(2)
	}
	wg.Wait()
	fetches := f.origin.ranges.Load() - before
	t.Logf("%d origin requests for 3 distinct segments wanted by 11 callers", fetches)
	if fetches > 3 {
		t.Errorf("%d origin requests for 3 distinct segments: callers past attempt 2 fetch without dedup. Dedup per segment run (key on the run), or have the leader fetch the union of what every waiter of the chunk needs (chunkNeed-style), so no caller falls through", fetches)
	}
}
