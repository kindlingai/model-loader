package daemon

import (
	"bytes"
	"context"
	"encoding/json"
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
