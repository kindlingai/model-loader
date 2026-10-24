package daemon

import (
	"context"
	"net/http/httptest"
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

// seg is the segment size of the models here: whole filesystem blocks,
// as the store's use is counted in allocated blocks.
const seg = 16 << 10

// diskFleet is a node under test with a disk budget, a peer, and a NAS.
// The node holds all of model "old" (a1.bin, a2.bin: 10 segments each); the peer holds a1.bin of it; the NAS holds model "new"
// (b.bin, 10 segments) and none of "old".
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
	old := map[string][]byte{"a1.bin": pattern(10*seg, 'a'), "a2.bin": pattern(10*seg, 'A')}
	f.st = open()
	seedFiles(t, f.st, "old", seg, old, "a1.bin", "a2.bin")
	f.peerSt = open()
	m := seedFiles(t, f.peerSt, "old", seg, old, "a1.bin", "a2.bin")
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
	seedFiles(t, nasSt, "new", seg, map[string][]byte{"b.bin": pattern(10*seg, 'b')}, "b.bin")
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
	d := f.node(20*seg, &catalog.Selection{Wanted: []string{"new"}})
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
	if ds := d.Status().Disk; ds == nil || ds.BudgetBytes != 20*seg || ds.UsedBytes != 20*seg || ds.EvictableBytes != 0 {
		t.Fatalf("status disk = %+v, want budget 200, used 200, evictable 0", ds)
	}
}

// A peer with the lowest ID among the holders keeps a segment no root
// holds, so the fetch that needs its space fails rather than drop it.
func TestBudgetLowestIDKeeps(t *testing.T) {
	f := newDiskFleet(t, "spark-z") // the peer's ID is higher: this node keeps a1.bin.
	d := f.node(20*seg, &catalog.Selection{Wanted: []string{"new"}})
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
	seedFiles(t, nasSt, "old", seg, map[string][]byte{"a1.bin": pattern(10*seg, 'a'), "a2.bin": pattern(10*seg, 'A')}, "a1.bin", "a2.bin")
	seedFiles(t, nasSt, "new", seg, map[string][]byte{"b.bin": pattern(10*seg, 'b')}, "b.bin")
	var inflight, peak atomic.Int32
	f.nas = newFleetNode(t, nasSt, "nas", &inflight, &peak)
	d := f.node(10*seg, &catalog.Selection{Wanted: []string{"new"}})
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
	d := f.node(23*seg, &catalog.Selection{Wanted: []string{"new"}})
	if _, err := d.PutRequest(request.Request{Name: "keep", Pieces: catalog.Pieces{Model: "old",
		Ranges: []catalog.FileRanges{{Path: "a1.bin", Ranges: []catalog.ByteRange{{Offset: 0, Length: 3 * seg}}}}}}); err != nil {
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
	d := f.node(25*seg, &catalog.Selection{Wanted: []string{"new"}})
	mf, _ := f.st.LoadManifest("old", "r1")
	rs, _ := d.revState(mf)
	fm, _ := mf.Find("a1.bin")
	fs := rs.FileState(fm)
	fs.Used = make([]int64, 10)
	for i := range fs.Used { // segment 9 oldest, 0 newest
		fs.Used[i] = int64(1000 - i)
	}
	rs.SetFileState(fs)
	done := d.BeginUse("old", "r1", fm, 9*seg, 10*seg) // segment 9 is being served
	defer done()
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := f.held("old", "a1.bin"); got != "1111000001" {
		t.Fatalf("a1.bin: held %s, want 8, 7, 6, 5 and 4 evicted (oldest, not the one served)", got)
	}
}

// statusSays is a peer's status that also says whether its catalog
// names a revision.
type statusSays struct {
	fakeStatus
	cataloged bool
}

func (s statusSays) Cataloged(model, revision string) bool { return s.cataloged }

// peerSays serves st as node id, its catalog naming every revision or
// none.
func peerSays(t *testing.T, st *store.Store, id string, cataloged bool) string {
	t.Helper()
	roles := []string{"peer"}
	if id == "nas" {
		roles = []string{"root"}
	}
	ts := httptest.NewServer(server.New(st, statusSays{fakeStatus{server.StatusResponse{NodeID: id, Roles: roles}}, cataloged}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

// A revision the catalog no longer names is removed once nothing names
// it: not this node's catalog, requests or selection, and no peer's
// catalog.
func TestBudgetRemovesOldRevisions(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	seedFiles(t, f.st, "gone", seg, map[string][]byte{"g.bin": pattern(2*seg, 'g')}, "g.bin")
	gone := func() bool {
		_, err := os.Stat(f.st.ModelDir("gone", "r1"))
		return os.IsNotExist(err) && !f.st.HasManifest("gone", "r1")
	}
	retired := []string{peerSays(t, f.peerSt, "spark-a", false), peerSays(t, f.peerSt, "nas", false)}
	node := func(budget int64, sel *catalog.Selection, peers []string) *Daemon {
		d := f.node(budget, sel)
		d.cfg.Discoverer = discovery.NewStatic(peers)
		return d
	}

	// Without a budget nothing is removed.
	node(0, &catalog.Selection{}, retired).diskPass(context.Background())
	if gone() {
		t.Fatal("removed with no budget")
	}
	// A peer whose catalog still names it may count on this copy.
	node(100*seg, &catalog.Selection{}, []string{peerSays(t, f.peerSt, "spark-a", true), retired[1]}).diskPass(context.Background())
	if gone() {
		t.Fatal("removed while a peer's catalog names it")
	}
	// So may a peer that does not say, or does not answer.
	node(100*seg, &catalog.Selection{}, []string{f.peer.addr(), retired[1]}).diskPass(context.Background())
	node(100*seg, &catalog.Selection{}, []string{"127.0.0.1:1", retired[1]}).diskPass(context.Background())
	if gone() {
		t.Fatal("removed while a peer did not say")
	}
	// A saved request, or the selection, names it.
	d := node(100*seg, &catalog.Selection{}, retired)
	if _, err := d.cfg.Requests.Put(request.Request{Name: "old", Pieces: catalog.Pieces{Model: "gone", Revision: "r1"}}); err != nil {
		t.Fatal(err)
	}
	d.diskPass(context.Background())
	node(100*seg, &catalog.Selection{Wanted: []string{"gone"}}, retired).diskPass(context.Background())
	if gone() {
		t.Fatal("removed while a request or the selection names it")
	}

	d = node(100*seg, &catalog.Selection{}, retired)
	d.diskPass(context.Background())
	if !gone() {
		t.Fatal("old revision still there")
	}
	if ds := d.Status().Disk; ds == nil || ds.UsedBytes != 20*seg || ds.EvictableBytes != 10*seg {
		t.Fatalf("status disk = %+v, want used %d (old model), evictable %d (a1.bin)", ds, 20*seg, 10*seg)
	}
}

// gatedPeers is a Discoverer that, once armed, holds the first Peers call
// until released.
type gatedPeers struct {
	discovery.Discoverer
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *gatedPeers) Peers(ctx context.Context) ([]discovery.Peer, error) {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
	return g.Discoverer.Peers(ctx)
}

// A request acknowledged while eviction waits on the fleet pins what it
// names before eviction acts.
func TestBudgetPinDuringEviction(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	d := f.node(20*seg, &catalog.Selection{})
	g := &gatedPeers{Discoverer: d.cfg.Discoverer, entered: make(chan struct{}), release: make(chan struct{})}
	d.cfg.Discoverer = g
	g.armed.Store(true)
	errc := make(chan error, 1)
	go func() {
		_, err := d.makeRoom(context.Background(), []spaceNeed{{key: "x", remaining: func() int64 { return 10 * seg }}})
		errc <- err
	}()
	<-g.entered
	if _, err := d.PutRequest(request.Request{Name: "pin", Pieces: catalog.Pieces{Model: "old", Include: []string{"a1.bin"}}}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	if err := <-errc; err == nil {
		t.Fatal("makeRoom fit by evicting what a request pins")
	}
	if got := f.held("old", "a1.bin"); got != all10 {
		t.Fatalf("a1.bin, pinned during eviction: held %s, want all", got)
	}
}

// A file being written is not evicted, nor removed under the writer; a
// segment being served is not counted as evictable.
func TestBudgetLeavesFilesInUse(t *testing.T) {
	f := newDiskFleet(t, "spark-a")
	d := f.node(20*seg, &catalog.Selection{})
	mf, _ := f.st.LoadManifest("old", "r1")
	fm, _ := mf.Find("a1.bin")
	done := d.writing("old", "r1", fm)
	if _, err := d.makeRoom(context.Background(), []spaceNeed{{key: "x", remaining: func() int64 { return 10 * seg }}}); err == nil {
		t.Fatal("makeRoom fit by evicting a file being written")
	}
	if got := f.held("old", "a1.bin"); got != all10 {
		t.Fatalf("a1.bin, being written: held %s, want all", got)
	}
	done()
	defer d.BeginUse("old", "r1", fm, 0, 3*seg)()
	d.diskPass(context.Background())
	if ds := d.Status().Disk; ds == nil || ds.EvictableBytes != 7*seg {
		t.Fatalf("status disk = %+v, want %d evictable (3 of a1.bin's segments served)", ds, 7*seg)
	}
}

// Bytes a failed transfer left, under segments not held, are freed before
// anything held is evicted, and count against the budget until they are.
func TestBudgetReclaimsFailedWrites(t *testing.T) {
	f := newDiskFleet(t, "spark-z") // nothing of "old" may be evicted.
	mf, _ := f.st.LoadManifest("old", "r1")
	rs, _ := f.st.LoadState(mf)
	fm, _ := mf.Find("a1.bin")
	for i := 5; i < 10; i++ { // written, then dropped as a transfer failed
		rs.DropSegment(fm, i)
	}
	if err := f.st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	d := f.node(25*seg, &catalog.Selection{})
	if _, err := d.makeRoom(context.Background(), []spaceNeed{{key: "x", remaining: func() int64 { return 10 * seg }}}); err != nil {
		t.Fatalf("makeRoom: %v", err)
	}
	if n, _ := f.st.Allocated(); n != 15*seg {
		t.Fatalf("store allocates %d bytes, want %d: the dropped segments freed", n, 15*seg)
	}
	if got := f.held("old", "a1.bin"); got != "1111100000" {
		t.Fatalf("a1.bin: held %s", got)
	}
}

// Fetches of the same segment share one reservation.
func TestBudgetSharedReservation(t *testing.T) {
	f := newDiskFleet(t, "spark-z")
	d := f.node(30*seg, &catalog.Selection{})
	need := spaceNeed{key: "same", remaining: func() int64 { return 10 * seg }}
	r1, err := d.makeRoom(context.Background(), []spaceNeed{need})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := d.makeRoom(context.Background(), []spaceNeed{need})
	if err != nil {
		t.Fatalf("second fetch of the same segment: %v", err)
	}
	if _, err := d.makeRoom(context.Background(), []spaceNeed{{key: "other", remaining: need.remaining}}); err == nil {
		t.Fatal("a fetch of another segment fit over the budget")
	}
	r1()
	r2()
	if len(d.reserved) != 0 {
		t.Fatalf("reservations left after release: %v", d.reserved)
	}
}

// A root makes room before it downloads a revision from its origin.
func TestBudgetBeforeWAN(t *testing.T) {
	origin := newFakeOrigin(t, map[string][]byte{"w.bin": pattern(20*seg, 'w')})
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: origin.ts.URL}}
	d := New(Config{
		NodeID: "nas", Roles: []string{"root"}, Store: st, DiskBudget: 10 * seg,
		Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: &catalog.Selection{Wanted: []string{"glm"}},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
	})
	_ = d.Reconcile(context.Background()) // a manifest error goes to the status
	if ms := d.Status().Models; len(ms) != 1 || !strings.Contains(ms[0].Error, "disk budget") {
		t.Fatalf("status = %+v, want a disk budget error", ms)
	}
	if n := origin.bytes.Load(); n != 0 {
		t.Fatalf("origin served %d bytes over the budget", n)
	}
}
