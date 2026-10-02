package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

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
// the root and the file itself at its path.
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
		_, _ = w.Write(content)
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
		Sources:    source.New(nil),
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

func TestDaemonPeerOnlyPullsFromPeer(t *testing.T) {
	content := []byte("already synced to one peer, waiting to be mirrored")

	peerStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := &store.Manifest{Model: "demo", Revision: "r1", Files: []store.FileManifest{{Path: "a.bin", Size: int64(len(content))}}}
	if err := manifest.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return io.NopCloser(bytesReaderAt(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := peerStore.SaveManifest(manifest); err != nil {
		t.Fatal(err)
	}
	f, err := peerStore.EnsureFile("demo", "r1", manifest.Files[0])
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
	if err := peerStore.SaveState(rs); err != nil {
		t.Fatal(err)
	}

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
		Sources:    source.New(nil),
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
		Sources:    source.New(nil),
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
		Sources:   source.New(nil),
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
