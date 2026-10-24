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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
	"github.com/kindlingai/model-loader/internal/xxh64"
)

type requestFleet struct {
	t       *testing.T
	root    *fleetNode
	model   catalog.Model
	content map[string][]byte
}

func newRequestFleet(t *testing.T) *requestFleet {
	t.Helper()
	files := map[string][]byte{"w.bin": pattern(100, 'w'), "config.json": pattern(15, 'c')}
	rootStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, rootStore, "glm", 10, files, "config.json", "w.bin")
	var inflight, peak atomic.Int32
	return &requestFleet{
		t: t, root: newFleetNode(t, rootStore, "nas", &inflight, &peak), content: files,
		model: catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}},
	}
}

// node returns a peer daemon over storeDir, with its requests in
// storeDir/requests, as a restart would see them.
func (f *requestFleet) node(storeDir string) *Daemon {
	f.t.Helper()
	st, err := store.Open(storeDir)
	if err != nil {
		f.t.Fatal(err)
	}
	reqs, err := request.Open(storeDir + "/requests")
	if err != nil {
		f.t.Fatal(err)
	}
	return New(Config{
		NodeID: "spark", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{f.model}}, Selection: &catalog.Selection{},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{f.root.addr()}),
		Requests: reqs,
	})
}

func rankRequest(name string, offset, length int64) request.Request {
	return request.Request{Name: name, Pieces: catalog.Pieces{
		Model:   "glm",
		Include: []string{"config.json"},
		Ranges:  []catalog.FileRanges{{Path: "w.bin", Ranges: []catalog.ByteRange{{Offset: offset, Length: length}}}},
	}}
}

func (f *requestFleet) reconcile(d *Daemon) int32 {
	f.t.Helper()
	before := f.root.blobs.Load()
	if err := d.Reconcile(context.Background()); err != nil {
		f.t.Fatalf("Reconcile: %v", err)
	}
	return f.root.blobs.Load() - before
}

func (f *requestFleet) ready(d *Daemon, name string) server.RequestStatus {
	f.t.Helper()
	st, ok := d.RequestStatus(name)
	if !ok {
		f.t.Fatalf("no request %s", name)
	}
	if st.State != server.StateReady {
		f.t.Fatalf("request %s: state %s (%s), want ready", name, st.State, st.Error)
	}
	return st
}

// Replacing a request with an overlapping one fetches only the difference:
// the pieces both name are kept, not fetched again. This is a live change
// of a rank's pieces, as in a TP=4 to TP=3 change.
func TestReplaceRequestFetchesOnlyDifference(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())

	// Rank 0 at TP=4: w.bin bytes 0-29 (segments 0-2), and config.json.
	if _, err := d.PutRequest(rankRequest("gr8-rank0", 0, 30)); err != nil {
		t.Fatal(err)
	}
	if n := f.reconcile(d); n != 5 { // 3 of w.bin, 2 of config.json
		t.Fatalf("first request fetched %d segments, want 5", n)
	}
	f.ready(d, "gr8-rank0")

	// Rank 0 at TP=3: bytes 10-59 (segments 1-5). Only 3, 4 and 5 are new.
	if _, err := d.PutRequest(rankRequest("gr8-rank0", 10, 50)); err != nil {
		t.Fatal(err)
	}
	if st, _ := d.RequestStatus("gr8-rank0"); st.State != server.StateDownloading {
		t.Fatalf("replaced request: state %s, want downloading", st.State)
	}
	if n := f.reconcile(d); n != 3 {
		t.Fatalf("replaced request fetched %d segments, want only the 3 new ones", n)
	}
	st := f.ready(d, "gr8-rank0")
	if st.BytesTotal != 50+15 || st.BytesDone != st.BytesTotal {
		t.Fatalf("bytes %d/%d, want 65/65", st.BytesDone, st.BytesTotal)
	}
	if n := f.reconcile(d); n != 0 {
		t.Fatalf("a held request fetched %d segments again", n)
	}
}

// Requests survive a restart: a new daemon over the same store has them,
// ready, and fetches nothing.
func TestRequestsSurviveRestart(t *testing.T) {
	f := newRequestFleet(t)
	dir := t.TempDir()
	d := f.node(dir)
	if _, err := d.PutRequest(rankRequest("gr8-rank1", 40, 20)); err != nil {
		t.Fatal(err)
	}
	f.reconcile(d)

	again := f.node(dir)
	if n := f.reconcile(again); n != 0 {
		t.Fatalf("after a restart, fetched %d segments", n)
	}
	f.ready(again, "gr8-rank1")
	if list := again.ListRequests(); len(list) != 1 || list[0].Name != "gr8-rank1" {
		t.Fatalf("ListRequests = %+v", list)
	}
	if found, err := again.DeleteRequest("gr8-rank1"); !found || err != nil {
		t.Fatalf("DeleteRequest = %v, %v", found, err)
	}
	if list := f.node(dir).ListRequests(); len(list) != 0 {
		t.Fatalf("released request still there after a restart: %+v", list)
	}
}

// The selection file and requests both apply on one node.
func TestRequestsWithSelection(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	d.cfg.Selection = &catalog.Selection{Partial: []catalog.Pieces{{Model: "glm", Include: []string{"config.json"}}}}
	if _, err := d.PutRequest(request.Request{Name: "r", Pieces: catalog.Pieces{Model: "glm",
		Ranges: []catalog.FileRanges{{Path: "w.bin", Ranges: []catalog.ByteRange{{Offset: 90, Length: 10}}}}}}); err != nil {
		t.Fatal(err)
	}
	if n := f.reconcile(d); n != 3 { // config.json's 2, and w.bin's last
		t.Fatalf("fetched %d segments, want 3", n)
	}
	f.ready(d, "r")
	if ms := d.Status().Models; len(ms) != 1 || ms[0].State != server.StatePartial {
		t.Fatalf("status = %+v, want the selection and request both held", ms)
	}
}

// WaitRequest returns once the request is ready, and a request whose range
// is past the end of its file is invalid and does not stop the rest of the
// model.
func TestWaitRequestAndInvalid(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	if _, err := d.PutRequest(rankRequest("good", 0, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.PutRequest(rankRequest("bad", 95, 10)); err != nil {
		t.Fatal(err)
	}
	done := make(chan server.RequestStatus, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		st, _ := d.WaitRequest(ctx, "good")
		done <- st
	}()
	f.reconcile(d)
	if st := <-done; st.State != server.StateReady {
		t.Fatalf("WaitRequest: state %s, want ready", st.State)
	}
	if st, _ := d.RequestStatus("bad"); st.State != server.StateInvalid || st.Error == "" {
		t.Fatalf("bad request: %+v, want invalid with a reason", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if st, _ := d.WaitRequest(ctx, "bad"); st.State != server.StateInvalid {
		t.Fatalf("WaitRequest(bad) = %s, want invalid at once", st.State)
	}
}

// Higher-priority requests' models are reconciled first.
func TestSortByPriority(t *testing.T) {
	ws := wantsFor("a", "b", "c")
	sortByPriority(ws, map[string]int{"c@r1": 5, "b@r1": 1})
	if got := ws[0].Model.Name + ws[1].Model.Name + ws[2].Model.Name; got != "cba" {
		t.Fatalf("order = %s, want cba", got)
	}
}

func wantsFor(names ...string) []want.Want {
	var out []want.Want
	for _, n := range names {
		out = append(out, want.Want{Model: catalog.Model{Name: n, Revision: "r1"}})
	}
	return out
}

// The local API end to end: put, read, wait, release.
func TestLocalAPI(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	ts := httptest.NewServer(server.NewLocal(d))
	t.Cleanup(ts.Close)
	do := func(method, path, body string) (int, server.RequestStatus) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var st server.RequestStatus
		json.NewDecoder(resp.Body).Decode(&st)
		return resp.StatusCode, st
	}
	body := `{"model":"glm","include":["config.json"],"ranges":[{"path":"w.bin","ranges":[{"offset":0,"length":20,"xxh64":"` +
		xxh64.Format(xxh64.Sum64(f.content["w.bin"][:20])) + `"}]}],"priority":3}`
	if code, st := do("PUT", "/requests/rank0", body); code != 200 || st.Revision != "r1" || st.Priority != 3 {
		t.Fatalf("PUT = %d %+v", code, st)
	}
	if code, _ := do("PUT", "/requests/rank0", `{"model":"nope"}`); code != 400 {
		t.Fatalf("PUT unknown model = %d, want 400", code)
	}
	if code, _ := do("PUT", "/requests/rank0", `{"model":"glm","bogus":1}`); code != 400 {
		t.Fatalf("PUT unknown field = %d, want 400", code)
	}
	if code, st := do("GET", "/requests/rank0?wait=50ms", ""); code != 504 || st.State == server.StateReady {
		t.Fatalf("wait before fetching = %d %s, want 504", code, st.State)
	}
	go d.Reconcile(context.Background())
	code, st := do("GET", "/requests/rank0?wait=10s", "")
	if code != 200 || st.State != server.StateReady || st.Dir == "" {
		t.Fatalf("wait = %d %+v, want 200 ready", code, st)
	}
	if got, _ := os.ReadFile(filepath.Join(st.Dir, "w.bin")); !bytes.Equal(got[:20], f.content["w.bin"][:20]) {
		t.Fatal("piece not at its offset in dir")
	}
	if code, _ := do("DELETE", "/requests/rank0", ""); code != 204 {
		t.Fatalf("DELETE = %d", code)
	}
	if code, _ := do("GET", "/requests/rank0", ""); code != 404 {
		t.Fatalf("GET released = %d, want 404", code)
	}
}

// Trailing content after the JSON object, or a body over the limit, is
// refused.
func TestLocalAPIBody(t *testing.T) {
	f := newRequestFleet(t)
	ts := httptest.NewServer(server.NewLocal(f.node(t.TempDir())))
	t.Cleanup(ts.Close)
	for name, body := range map[string]string{
		"trailing": `{"model":"glm"} not json`,
		"two":      `{"model":"glm"}{"model":"glm"}`,
		"huge":     `{"model":"glm"}` + strings.Repeat(" ", 17<<20),
	} {
		req, _ := http.NewRequest("PUT", ts.URL+"/requests/r", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("%s: PUT = %d, want 400", name, resp.StatusCode)
		}
	}
}

// A request whose checksum is wrong does not stop another request for a
// different file of the same model.
func TestBadChecksumDoesNotBlockOtherFiles(t *testing.T) {
	// Through a pull-through root, so the manifest has no segment hashes
	// and the checksum applies. a.bin, the bad one, comes first.
	content := pattern(40, 'q')
	pf := newPullFleet(t, map[string][]byte{"a.bin": content, "z.bin": pattern(10, 'z')}, nil)
	peer, _ := pf.peer(&catalog.Selection{})
	reg, err := request.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer.cfg.Requests = reg
	if _, err := peer.PutRequest(request.Request{Name: "bad", Pieces: catalog.Pieces{Model: "glm",
		Ranges: []catalog.FileRanges{{Path: "a.bin", Ranges: []catalog.ByteRange{{Offset: 0, Length: 10, XXH64: "0123456789abcdef"}}}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.PutRequest(request.Request{Name: "good", Pieces: catalog.Pieces{Model: "glm", Include: []string{"z.bin"}}}); err != nil {
		t.Fatal(err)
	}
	if err := peer.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile = nil with a bad checksum")
	}
	if st, _ := peer.RequestStatus("good"); st.State != server.StateReady {
		t.Fatalf("good request: %s %d/%d, want ready despite the bad one", st.State, st.BytesDone, st.BytesTotal)
	}
}

// A request's checksums still apply when the selection wants the whole
// model.
func TestRequestChecksWithWholeSelection(t *testing.T) {
	content := pattern(40, 'h')
	pf := newPullFleet(t, map[string][]byte{"a.bin": content}, nil, func(o *fakeOrigin) {
		o.digest = func(string, string) string { return "" } // no whole-file digest
	})
	peer, _ := pf.peer(allSelection())
	reg, err := request.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer.cfg.Requests = reg
	good := xxh64.Format(xxh64.Sum64(content[10:20]))
	if _, err := peer.PutRequest(request.Request{Name: "r", Pieces: catalog.Pieces{Model: "glm",
		Ranges: []catalog.FileRanges{{Path: "a.bin", Ranges: []catalog.ByteRange{{Offset: 10, Length: 10, XXH64: good}}}}}}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := peer.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	if st, _ := peer.RequestStatus("r"); st.State != server.StateReady {
		t.Fatalf("request: %s, want ready (its checksum was checked)", st.State)
	}
}

// A wait sees a request become ready even when no status changes (another
// model's transfer can hold the status), and a timed-out wait reports the
// status as it is then.
func TestWaitSeesStateWithoutStatusChange(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	if _, err := d.PutRequest(rankRequest("r", 0, 10)); err != nil {
		t.Fatal(err)
	}
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.PutRequest(rankRequest("r", 50, 10)); err != nil { // not held yet
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		d.DeleteRequest("r")
	}()
	if _, ok := d.WaitRequest(ctx, "r"); ok {
		t.Fatal("WaitRequest still reports a request released while it waited")
	}
}

// A request made while a pass is running for its model is reconciled when
// that pass ends, without waiting for the next tick; passes do not wake
// each other without end.
func TestKickDuringPass(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var passes atomic.Int32
	d.onPass = func() { passes.Add(1) }
	go d.Run(ctx, time.Hour, nil)
	if _, err := d.PutRequest(rankRequest("r", 0, 10)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if st, _ := d.WaitRequest(wctx, "r"); st.State != server.StateReady {
		t.Fatalf("first request: %s", st.State)
	}
	if _, err := d.PutRequest(rankRequest("r", 50, 30)); err != nil {
		t.Fatal(err)
	}
	if st, _ := d.WaitRequest(wctx, "r"); st.State != server.StateReady {
		t.Fatalf("replaced request: %s, want ready before the hourly tick", st.State)
	}
	n := passes.Load()
	time.Sleep(300 * time.Millisecond)
	if extra := passes.Load() - n; extra > 1 {
		t.Fatalf("%d more passes with nothing changed: passes wake each other", extra)
	}
}

// A bad checksum on one range of a file does not stop another request's
// range of the same file: the root's 422 is about the range, not the root,
// so the peer still fetches other segments from it.
func TestBadChecksumSameFileDoesNotBlock(t *testing.T) {
	content := pattern(40, 's')
	pf := newPullFleet(t, map[string][]byte{"a.bin": content}, nil)
	peer, _ := pf.peer(&catalog.Selection{})
	peer.cfg.MaxConcurrentTransfers = 1
	peer.limit = transfer.NewLimiter(1)
	reg, err := request.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer.cfg.Requests = reg
	put := func(name string, off int64, sum string) {
		t.Helper()
		if _, err := peer.PutRequest(request.Request{Name: name, Pieces: catalog.Pieces{Model: "glm",
			Ranges: []catalog.FileRanges{{Path: "a.bin", Ranges: []catalog.ByteRange{{Offset: off, Length: 10, XXH64: sum}}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	put("bad", 0, "0123456789abcdef")
	put("good", 20, xxh64.Format(xxh64.Sum64(content[20:30])))
	if err := peer.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile = nil with a bad checksum")
	}
	st, _ := peer.RequestStatus("good")
	if st.State != server.StateReady {
		t.Fatalf("good request: %s %d/%d (%s), want ready", st.State, st.BytesDone, st.BytesTotal, st.Error)
	}
	if st.Error != "" {
		t.Fatalf("ready request carries an error: %s", st.Error)
	}
}

// An invalid request does not stop a selection that wants the model
// whole, nor a good request.
func TestInvalidRequestWithWholeSelection(t *testing.T) {
	f := newRequestFleet(t)
	d := f.node(t.TempDir())
	d.cfg.Selection = &catalog.Selection{Wanted: []string{"glm"}}
	if _, err := d.PutRequest(rankRequest("bad", 95, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.PutRequest(rankRequest("good", 0, 10)); err != nil {
		t.Fatal(err)
	}
	if n := f.reconcile(d); n != 12 { // all of w.bin and config.json
		t.Fatalf("fetched %d segments, want the whole model's 12", n)
	}
	f.ready(d, "good")
	if st, _ := d.RequestStatus("bad"); st.State != server.StateInvalid {
		t.Fatalf("bad request: %s, want invalid", st.State)
	}
	if ms := d.Status().Models; len(ms) != 1 || ms[0].State != server.StateReady {
		t.Fatalf("status = %+v, want the whole model ready", ms)
	}
}

// A root admits queued pull-through fetches by the priority the asking
// node sends, highest first.
func TestPullThroughPriority(t *testing.T) {
	var mu sync.Mutex
	var order []string
	pf := newPullFleet(t, map[string][]byte{"a.bin": pattern(40, 'p')}, nil, func(o *fakeOrigin) {
		o.onRange = func(_, rng string) {
			mu.Lock()
			order = append(order, rng)
			mu.Unlock()
		}
	})
	if _, err := pf.root.Manifest(context.Background(), "glm", "r1"); err != nil {
		t.Fatal(err)
	}
	pf.root.pullLimit = transfer.NewLimiter(1)
	pf.root.pullLimit.Acquire(context.Background(), 0) // every slot busy
	var wg sync.WaitGroup
	get := func(start int64, prio string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("GET", pf.rootTS.URL+"/blob/glm/r1/a.bin", nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+9))
			req.Header.Set(server.HeaderPriority, prio)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}()
		time.Sleep(50 * time.Millisecond) // queue in this order
	}
	get(0, "0")
	get(20, "10")
	pf.root.pullLimit.Release()
	wg.Wait()
	if len(order) != 2 || order[0] != "bytes=20-29" {
		t.Fatalf("origin fetches in order %v, want the priority-10 range first", order)
	}
}
