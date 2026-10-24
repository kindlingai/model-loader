package daemon

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// diskFleet is a node under test with a disk budget, a peer, and a NAS.
// The node holds all of model "old" (a1.bin, a2.bin: 100 bytes each, 10
// byte segments); the peer holds a1.bin of it; the NAS holds model "new"
// (b.bin, 100 bytes) and none of "old".
type diskFleet struct {
	t       *testing.T
	st      *store.Store
	peer    *fleetNode
	peerSt  *store.Store
	nas     *fleetNode
	catalog *catalog.Catalog
}

func newDiskFleet(t *testing.T, peerID string) *diskFleet {
	t.Helper()
	f := &diskFleet{t: t}
	var inflight, peak atomic.Int32
	open := func() *store.Store {
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	old := map[string][]byte{"a1.bin": pattern(100, 'a'), "a2.bin": pattern(100, 'A')}
	f.st = open()
	seedFiles(t, f.st, "old", 10, old, "a1.bin", "a2.bin")
	f.peerSt = open()
	m := seedFiles(t, f.peerSt, "old", 10, old, "a1.bin", "a2.bin")
	rs, _ := f.peerSt.LoadState(m)
	fm, _ := m.Find("a2.bin")
	for i := range fm.NumSegments() {
		rs.EvictSegment(fm, i)
	}
	if err := f.peerSt.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	f.peer = newFleetNode(t, f.peerSt, peerID, &inflight, &peak)
	nasSt := open()
	seedFiles(t, nasSt, "new", 10, map[string][]byte{"b.bin": pattern(100, 'b')}, "b.bin")
	f.nas = newFleetNode(t, nasSt, "nas", &inflight, &peak)
	src := catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}
	f.catalog = &catalog.Catalog{Models: []catalog.Model{
		{Name: "old", Revision: "r1", Source: src}, {Name: "new", Revision: "r1", Source: src},
	}}
	return f
}

func (f *diskFleet) node(budget int64, sel *catalog.Selection) *Daemon {
	f.t.Helper()
	reqs, err := request.Open(f.t.TempDir())
	if err != nil {
		f.t.Fatal(err)
	}
	return New(Config{
		NodeID: "spark-m", Roles: []string{"peer"}, Store: f.st,
		Catalog: f.catalog, Selection: sel, Requests: reqs,
		Sources: source.New(nil, ""), DiskBudget: budget,
		Discoverer: discovery.NewStatic([]string{f.peer.addr(), f.nas.addr()}),
	})
}

// held returns which segments of path in model this node holds, as a
// string of 1s and 0s.
func (f *diskFleet) held(model, path string) string {
	f.t.Helper()
	mf, err := f.st.LoadManifest(model, "r1")
	if err != nil {
		f.t.Fatal(err)
	}
	rs, err := f.st.LoadState(mf)
	if err != nil {
		f.t.Fatal(err)
	}
	fm, _ := mf.Find(path)
	var b strings.Builder
	for _, d := range rs.FileState(fm).SegmentsDone {
		if d {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}

const all10, none10 = "1111111111", "0000000000"

// The done-when test of #15: a node with a small budget fetches a second
// model, evicts the unpinned parts of the first that another node keeps,
// and keeps every segment that has only one copy in the fleet.
func TestBudgetEvictsOnlyWhatTheFleetKeeps(t *testing.T) {
	f := newDiskFleet(t, "spark-a") // the peer's ID is lower: it keeps a1.bin.
	d := f.node(200, &catalog.Selection{Wanted: []string{"new"}})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.held("new", "b.bin"); got != all10 {
		t.Fatalf("new model: held %s, want all", got)
	}
	if got := f.held("old", "a1.bin"); got != none10 {
		t.Fatalf("a1.bin (the peer has a copy): held %s, want evicted", got)
	}
	if got := f.held("old", "a2.bin"); got != all10 {
		t.Fatalf("a2.bin (the only copy): held %s, want kept", got)
	}
	if _, err := os.Stat(f.st.FilePath("old", "r1", "a1.bin")); !os.IsNotExist(err) {
		t.Fatalf("a1.bin with nothing held is still on disk: %v", err)
	}
	d.diskPass(context.Background())
	if ds := d.Status().Disk; ds == nil || ds.BudgetBytes != 200 || ds.UsedBytes != 200 || ds.EvictableBytes != 0 {
		t.Fatalf("status disk = %+v, want budget 200, used 200, evictable 0", ds)
	}
}

// A peer with the lowest ID among the holders keeps a segment no root
// holds, so the fetch that needs its space fails rather than drop it.
func TestBudgetLowestIDKeeps(t *testing.T) {
	f := newDiskFleet(t, "spark-z") // the peer's ID is higher: this node keeps a1.bin.
	d := f.node(200, &catalog.Selection{Wanted: []string{"new"}})
	err := d.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "disk budget") {
		t.Fatalf("Reconcile = %v, want a disk budget error", err)
	}
	if got := f.held("old", "a1.bin") + f.held("old", "a2.bin"); got != all10+all10 {
		t.Fatalf("old model: held %s, want all of it kept", got)
	}
	ms := d.Status().Models
	if len(ms) != 1 || ms[0].State != server.StateError || !strings.Contains(ms[0].Error, "disk budget") {
		t.Fatalf("status = %+v, want an error naming the budget", ms)
	}
}

// What a root holds may be evicted whatever the peers' IDs.
func TestBudgetRootCopyCounts(t *testing.T) {
	f := newDiskFleet(t, "spark-z")
	nasSt, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, nasSt, "old", 10, map[string][]byte{"a1.bin": pattern(100, 'a'), "a2.bin": pattern(100, 'A')}, "a1.bin", "a2.bin")
	seedFiles(t, nasSt, "new", 10, map[string][]byte{"b.bin": pattern(100, 'b')}, "b.bin")
	var inflight, peak atomic.Int32
	f.nas = newFleetNode(t, nasSt, "nas", &inflight, &peak)
	d := f.node(100, &catalog.Selection{Wanted: []string{"new"}})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.held("old", "a1.bin") + f.held("old", "a2.bin"); got != none10+none10 {
		t.Fatalf("old model: held %s, want all evicted (the NAS has it)", got)
	}
}

// Pieces a request names are pinned: eviction leaves them.
func TestBudgetKeepsPinned(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	d := f.node(230, &catalog.Selection{Wanted: []string{"new"}})
	if _, err := d.PutRequest(request.Request{Name: "keep", Pieces: catalog.Pieces{Model: "old",
		Ranges: []catalog.FileRanges{{Path: "a1.bin", Ranges: []catalog.ByteRange{{Offset: 0, Length: 30}}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.held("old", "a1.bin"); got != "1110000000" {
		t.Fatalf("a1.bin: held %s, want the pinned 0-2 kept and the rest evicted", got)
	}
	if st, _ := d.RequestStatus("keep"); st.State != server.StateReady {
		t.Fatalf("pinned request: %s, want ready", st.State)
	}
}

// Eviction removes only as much as the fetch needs, least recently used
// first, and never a segment being served.
func TestBudgetOldestFirstNotServing(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	d := f.node(250, &catalog.Selection{Wanted: []string{"new"}})
	mf, _ := f.st.LoadManifest("old", "r1")
	rs, _ := d.revState(mf)
	fm, _ := mf.Find("a1.bin")
	fs := rs.FileState(fm)
	fs.Used = make([]int64, 10)
	for i := range fs.Used { // segment 9 oldest, 0 newest
		fs.Used[i] = int64(1000 - i)
	}
	rs.SetFileState(fs)
	done := d.BeginUse("old", "r1", fm, 90, 100) // segment 9 is being served
	defer done()
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.held("old", "a1.bin"); got != "1111000001" {
		t.Fatalf("a1.bin: held %s, want 8, 7, 6, 5 and 4 evicted (oldest, not the one served)", got)
	}
}

// Revisions the catalog no longer names are removed.
func TestBudgetRemovesOldRevisions(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	seedFiles(t, f.st, "gone", 10, map[string][]byte{"g.bin": pattern(20, 'g')}, "g.bin")
	d := f.node(1000, &catalog.Selection{})
	d.diskPass(context.Background())
	if _, err := os.Stat(f.st.ModelDir("gone", "r1")); !os.IsNotExist(err) {
		t.Fatalf("old revision still on disk: %v", err)
	}
	if f.st.HasManifest("gone", "r1") {
		t.Fatal("old revision's manifest still there")
	}
	if ds := d.Status().Disk; ds == nil || ds.UsedBytes != 200 || ds.EvictableBytes != 100 {
		t.Fatalf("status disk = %+v, want used 200 (old model), evictable 100 (a1.bin)", ds)
	}

	// Without a budget nothing is removed.
	seedFiles(t, f.st, "gone", 10, map[string][]byte{"g.bin": pattern(20, 'g')}, "g.bin")
	f.node(0, &catalog.Selection{}).diskPass(context.Background())
	if !f.st.HasManifest("gone", "r1") {
		t.Fatal("removed with no budget")
	}
}
