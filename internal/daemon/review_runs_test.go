package daemon

import (
	"context"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// Regression (review of #30): sources.order rotated the holders of segment i by i
// (appendRotated), so with two or more nodes holding the same segments
// consecutive segments get different orders and transfer.groupRuns (which
// requires slices.Equal(order(i), order(i+1))) makes a run of one segment.
// The run optimisation the PR relies on (one request per up to 256
// consecutive small segments) only works with exactly one holder. In a
// fleet where several sparks and the root hold a rank's halves, every
// 64 KiB segment is its own HTTP request.
func TestReviewRunsBreakWithTwoHolders(t *testing.T) {
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

	// Two LAN nodes hold exactly the same segments (here: the same store
	// behind two addresses).
	srv := server.New(firstSt, first)
	tsA, blobsA := countingServer(t, srv)
	tsB, blobsB := countingServer(t, srv)
	st2, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	second := New(Config{
		NodeID: "spark-2", Roles: []string{"peer"}, Store: st2, Catalog: cat, Selection: sel,
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{tsA.Listener.Addr().String(), tsB.Listener.Addr().String()}),
	})
	if err := second.Reconcile(context.Background()); err != nil {
		t.Fatalf("second peer: %v", err)
	}
	mf, _ := st2.LoadManifest("glm", "r1")
	fm, _ := mf.Find("w.k4.safetensors")
	rs, _ := st2.LoadState(mf)
	fs := rs.FileState(fm)
	held, runs := 0, len(store.Runs(fs.SegmentsDone))
	for _, d := range fs.SegmentsDone {
		if d {
			held++
		}
	}
	blobs := int(blobsA.Load() + blobsB.Load())
	t.Logf("%d held segments in %d runs; %d blob requests from two holders (TestSmallSegmentsPeerToPeer, one holder: fewer than held)", held, runs, blobs)
	if blobs >= held {
		t.Errorf("%d blob requests for %d segments: one request per segment; runs break because order(i) rotates the holders by i. Rotate per run (or per chunk), not per segment, or compare order sets rather than sequences in groupRuns", blobs, held)
	}
}
