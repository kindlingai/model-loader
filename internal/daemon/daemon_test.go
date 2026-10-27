package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

type fakeStatus struct{ resp server.StatusResponse }

func (f fakeStatus) Status() server.StatusResponse { return f.resp }

// newWANTestServer serves a single file "a.bin" as a plain HTTP source,
// matching the contract internal/source/http.go expects: a manifest.json at
// the root and the file itself at its path, with Range support.
func newWANTestServer(t *testing.T, content []byte) *httptest.Server {
	t.Helper()
	sum := sha256.Sum256(content)
	hexSum := hex.EncodeToString(sum[:])

	mux := http.NewServeMux()
	mux.HandleFunc("/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"files": []map[string]any{
				{"path": "a.bin", "size": len(content), "sha256": hexSum},
			},
		})
	})
	mux.HandleFunc("/a.bin", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func wanModel(name, url string) catalog.Model {
	return catalog.Model{
		Name:     name,
		Revision: "r1",
		Source:   catalog.Source{Type: catalog.SourceHTTP, URL: url},
	}
}

func allSelection() *catalog.Selection { return &catalog.Selection{Wanted: []string{"*"}} }

func TestDaemonWANOnlyDownloadReachesReady(t *testing.T) {
	content := []byte("this is a model file fetched straight from WAN")
	ts := newWANTestServer(t, content)
	m := wanModel("demo", ts.URL)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID:     "root-1",
		Roles:      []string{"root"},
		Store:      st,
		Catalog:    &catalog.Catalog{Models: []catalog.Model{m}},
		Selection:  allSelection(),
		Sources:    source.New(nil, ""),
		Discoverer: discovery.NewStatic(nil),
	})

	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	resp := d.Status()
	if len(resp.Models) != 1 {
		t.Fatalf("len(Models) = %d, want 1", len(resp.Models))
	}
	ms := resp.Models[0]
	if ms.State != server.StateReady {
		t.Fatalf("state = %q, want ready (err: %s)", ms.State, ms.Error)
	}
	if ms.BytesDone != int64(len(content)) || ms.BytesTotal != int64(len(content)) {
		t.Fatalf("bytes = %d/%d, want %d/%d", ms.BytesDone, ms.BytesTotal, len(content), len(content))
	}

	got, err := os.ReadFile(st.FilePath("demo", "r1", "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Fatalf("file content = %q, want %q", got, content)
	}
}

// seedPeerModel puts a complete, verified copy of model name@r1, one file
// "a.bin" holding content, into st, as if that node had finished syncing it.
func seedPeerModel(t *testing.T, st *store.Store, name string, content []byte) {
	t.Helper()
	manifest := &store.Manifest{Model: name, Revision: "r1", Files: []store.FileManifest{{Path: "a.bin", Size: int64(len(content))}}}
	if err := manifest.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return io.NopCloser(bytesReaderAt(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(manifest); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile(name, "r1", manifest.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rs := store.NewRevisionState(manifest)
	fs := rs.FileState(manifest.Files[0])
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = true
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonPeerOnlyPullsFromPeer(t *testing.T) {
	content := []byte("already synced to one peer, waiting to be mirrored")

	peerStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedPeerModel(t, peerStore, "demo", content)

	peerSrv := server.New(peerStore, fakeStatus{server.StatusResponse{NodeID: "peer-1"}})
	ts := httptest.NewServer(peerSrv)
	t.Cleanup(ts.Close)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "demo", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID:     "peer-2",
		Roles:      []string{"peer"},
		Store:      st,
		Catalog:    &catalog.Catalog{Models: []catalog.Model{m}},
		Selection:  allSelection(),
		Sources:    source.New(nil, ""),
		Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}),
	})

	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	resp := d.Status()
	if len(resp.Models) != 1 || resp.Models[0].State != server.StateReady {
		t.Fatalf("status = %+v, want one ready model", resp.Models)
	}

	got, err := os.ReadFile(st.FilePath("demo", "r1", "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Fatalf("file content = %q, want %q", got, content)
	}
}

// A model whose fetch failed (its source peer restarted) must retry on the
// next tick even while another model's transfer is still running, rather
// than sit in error until every other model finishes. See issue #5.
func TestDaemonRunRetriesFailedModelWhileAnotherIsBusy(t *testing.T) {
	content := []byte("served by a peer that restarts once")
	peerStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedPeerModel(t, peerStore, "slow", content)
	seedPeerModel(t, peerStore, "flaky", content)

	peerSrv := server.New(peerStore, fakeStatus{server.StatusResponse{NodeID: "peer-1"}})
	release := make(chan struct{})
	var flakyCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/blob/slow/"):
			select { // a long transfer that outlasts the test
			case <-release:
			case <-r.Context().Done():
			}
			return
		case strings.HasPrefix(r.URL.Path, "/blob/flaky/") && flakyCalls.Add(1) == 1:
			http.Error(w, "restarting", http.StatusBadGateway)
			return
		}
		peerSrv.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(func() { close(release) })

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	src := catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}
	d := New(Config{
		NodeID: "peer-2",
		Roles:  []string{"peer"},
		Store:  st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{
			{Name: "slow", Revision: "r1", Source: src},
			{Name: "flaky", Revision: "r1", Source: src},
		}},
		Selection:  allSelection(),
		Sources:    source.New(nil, ""),
		Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx, 10*time.Millisecond, nil); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(5 * time.Second)
	for {
		states := map[string]string{}
		for _, ms := range d.Status().Models {
			states[ms.Model] = ms.State
		}
		if states["flaky"] == server.StateReady {
			if states["slow"] != server.StateDownloading {
				t.Fatalf("slow = %q, want it still downloading", states["slow"])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("flaky never recovered: states %v, %d blob requests", states, flakyCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if flakyCalls.Load() < 2 {
		t.Fatalf("flaky blob requests = %d, want a failure and a retry", flakyCalls.Load())
	}
}

func TestDaemonPeerOnlyWithNoSourceStaysPending(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "demo", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID:     "peer-1",
		Roles:      []string{"peer"},
		Store:      st,
		Catalog:    &catalog.Catalog{Models: []catalog.Model{m}},
		Selection:  allSelection(),
		Sources:    source.New(nil, ""),
		Discoverer: discovery.NewStatic(nil),
	})

	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile should not fail hard when nobody has the model yet: %v", err)
	}

	resp := d.Status()
	if len(resp.Models) != 1 || resp.Models[0].State != server.StatePending {
		t.Fatalf("status = %+v, want one pending model", resp.Models)
	}
}

func TestDaemonReloadsSelectionFile(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	selPath := filepath.Join(t.TempDir(), "node.yaml")
	m := catalog.Model{Name: "demo", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID:        "peer-1",
		Roles:         []string{"peer"},
		Store:         st,
		Catalog:       &catalog.Catalog{Models: []catalog.Model{m}},
		Selection:     &catalog.Selection{},
		SelectionPath: selPath,
		Sources:       source.New(nil, ""),
		Discoverer:    discovery.NewStatic(nil),
	})
	reconcile := func(wantModels int) {
		t.Helper()
		if err := d.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if got := d.Status().Models; got == nil || len(got) != wantModels {
			t.Fatalf("status models = %#v, want %d (non-nil)", got, wantModels)
		}
	}
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(selPath, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reconcile(0) // missing file: empty selection, not an error.
	write("wanted: [demo]\n")
	reconcile(1)

	write("wanted: [\n")
	if err := d.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile with a malformed selection: want an error")
	}
	if got := len(d.Status().Models); got != 1 {
		t.Fatalf("malformed selection should keep the last good one; status models = %d", got)
	}

	write("wanted: []\n")
	reconcile(0) // deselected models drop out of /status.
}

func TestDaemonAlreadyCompleteSkipsTransfer(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("already fully present locally")
	manifest := &store.Manifest{Model: "demo", Revision: "r1", Files: []store.FileManifest{{Path: "a.bin", Size: int64(len(content))}}}
	if err := manifest.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return io.NopCloser(bytesReaderAt(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(manifest); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(manifest)
	fs := rs.FileState(manifest.Files[0])
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = true
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}

	m := catalog.Model{Name: "demo", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID:    "peer-1",
		Roles:     []string{"peer"},
		Store:     st,
		Catalog:   &catalog.Catalog{Models: []catalog.Model{m}},
		Selection: allSelection(),
		Sources:   source.New(nil, ""),
		// A Discoverer that always errors proves Reconcile never needed to
		// consult peers or WAN for an already-complete model.
		Discoverer: errorDiscoverer{},
	})

	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	resp := d.Status()
	if len(resp.Models) != 1 || resp.Models[0].State != server.StateReady {
		t.Fatalf("status = %+v, want one ready model", resp.Models)
	}
}

// newCountingWANServer serves files as a plain HTTP source, with Range
// support, and counts GETs per file, so a test can tell which files were
// actually downloaded.
func newCountingWANServer(t *testing.T, files map[string][]byte) (*httptest.Server, *sync.Map) {
	t.Helper()
	var entries []map[string]any
	for name, content := range files {
		sum := sha256.Sum256(content)
		entries = append(entries, map[string]any{"path": name, "size": len(content), "sha256": hex.EncodeToString(sum[:])})
	}
	counts := &sync.Map{}
	mux := http.NewServeMux()
	mux.HandleFunc("/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"files": entries})
	})
	for name, content := range files {
		mux.HandleFunc("/"+name, func(w http.ResponseWriter, r *http.Request) {
			n, _ := counts.LoadOrStore(name, new(atomic.Int32))
			n.(*atomic.Int32).Add(1)
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
		})
	}
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, counts
}

func getCount(counts *sync.Map, name string) int32 {
	n, ok := counts.Load(name)
	if !ok {
		return 0
	}
	return n.(*atomic.Int32).Load()
}

// newRangeWANServer serves content as "a.bin" from a plain HTTP source whose
// manifest lists digest, recording each request's Range header. serve writes
// the response to the nth request for the file, counting from 1.
func newRangeWANServer(t *testing.T, content []byte, digest string, serve func(n int, w http.ResponseWriter, r *http.Request)) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var ranges []string
	mux := http.NewServeMux()
	mux.HandleFunc("/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"files": []map[string]any{
			{"path": "a.bin", "size": len(content), "sha256": digest},
		}})
	})
	mux.HandleFunc("/a.bin", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		n := len(ranges)
		mu.Unlock()
		serve(n, w, r)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), ranges...)
	}
}

// twoSegments is content spanning two segments, so a download can be cut off
// after the first one is saved.
func twoSegments() []byte {
	content := make([]byte, store.DefaultSegmentSize+1000)
	for i := range content {
		content[i] = byte(i * 7)
	}
	return content
}

// A root cut off partway through a model has published its manifest
// anyway, and once restarted asks the origin only for the segment it does
// not hold.
func TestDaemonWANResumesCutOffFile(t *testing.T) {
	content := twoSegments()
	sum := sha256.Sum256(content)
	first := "bytes=0-" + strconv.FormatInt(store.DefaultSegmentSize-1, 10)
	second := "bytes=" + strconv.FormatInt(store.DefaultSegmentSize, 10) + "-" + strconv.Itoa(len(content)-1)
	var cut atomic.Bool
	ts, ranges := newRangeWANServer(t, content, hex.EncodeToString(sum[:]), func(n int, w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == second && cut.CompareAndSwap(false, true) {
			w.Header().Set("Content-Length", "1000") // cut off partway through
			_, _ = w.Write(content[store.DefaultSegmentSize : store.DefaultSegmentSize+100])
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(content))
	})
	m := wanModel("demo", ts.URL)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := func() *Daemon {
		return New(Config{
			NodeID: "root-1", Roles: []string{"root"}, Store: st,
			Catalog:   &catalog.Catalog{Models: []catalog.Model{m}},
			Selection: allSelection(), Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
		})
	}

	d := root()
	if err := d.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile after the cut-off: no error")
	}
	if ms := d.Status().Models[0]; ms.State != server.StateError || ms.BytesDone != store.DefaultSegmentSize {
		t.Fatalf("after the cut-off: %s with %d bytes, want error with the first segment", ms.State, ms.BytesDone)
	}
	if _, err := st.LoadManifest("demo", "r1"); err != nil {
		t.Fatalf("manifest not published before the download is done: %v", err)
	}

	d = root()
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if ms := d.Status().Models[0]; ms.State != server.StateReady {
		t.Fatalf("state = %q, want ready (err: %s)", ms.State, ms.Error)
	}
	got := ranges()
	slices.Sort(got)
	if want := []string{first, second, second}; !slices.Equal(got, want) {
		t.Fatalf("Range headers = %q, want %q", got, want)
	}
	file, err := os.ReadFile(st.FilePath("demo", "r1", "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(file, content) {
		t.Fatal("a.bin does not match the origin's content")
	}
}

type errorDiscoverer struct{}

func (errorDiscoverer) Peers(ctx context.Context) ([]discovery.Peer, error) {
	return nil, context.DeadlineExceeded
}

// bytesReaderAt adapts a []byte to io.Reader for BuildSegmentHashes, which
// only needs sequential reads.
func bytesReaderAt(b []byte) *os.File {
	f, err := os.CreateTemp("", "daemon-test-*")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(b); err != nil {
		panic(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	_ = os.Remove(f.Name())
	return f
}
