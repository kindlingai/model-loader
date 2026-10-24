package daemon

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// seedFiles puts a complete, verified copy of name@r1 holding files into
// st, with segments of segSize bytes.
func seedFiles(t *testing.T, st *store.Store, name string, segSize int64, files map[string][]byte, order ...string) *store.Manifest {
	t.Helper()
	m := &store.Manifest{Model: name, Revision: "r1"}
	for _, p := range order {
		m.Files = append(m.Files, store.FileManifest{Path: p, Size: int64(len(files[p])), SegmentSize: segSize})
	}
	if err := m.BuildSegmentHashes(func(p string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(files[p])), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)
	for _, fm := range m.Files {
		f, err := st.EnsureFile(name, "r1", fm)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(files[fm.Path], 0); err != nil {
			t.Fatal(err)
		}
		f.Close()
		fs := rs.FileState(fm)
		for i := range fs.SegmentsDone {
			fs.SegmentsDone[i] = true
		}
		fs.Complete = true
		rs.SetFileState(fs)
	}
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	return m
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%31)
	}
	return b
}

// countingPeer serves st over HTTP and counts blob requests.
func countingPeer(t *testing.T, st *store.Store) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	srv := server.New(st, fakeStatus{server.StatusResponse{NodeID: "peer-1"}})
	var blobs atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/blob/") {
			blobs.Add(1)
		}
		srv.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts, &blobs
}

// A node with a partial selection fetches only the files and ranges it
// names, into sparse files; /status reports it as partial, never ready. A
// restart with a wider selection fetches only what is new, and a restart
// with -verify-on-start fetches again a held segment that went bad on disk.
func TestDaemonPartialSelection(t *testing.T) {
	files := map[string][]byte{
		"config.json":           pattern(7, 'a'),
		"dense/vision.bin":      pattern(40, 'v'),
		"experts/e.safetensors": pattern(55, 'e'), // six segments of 10
	}
	peerStore, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := seedFiles(t, peerStore, "glm", 10, files, "config.json", "dense/vision.bin", "experts/e.safetensors")
	ts, blobs := countingPeer(t, peerStore)

	dir := t.TempDir()
	local, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "glm", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	newDaemon := func(sel *catalog.Selection, verify bool) *Daemon {
		st, err := store.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		return New(Config{
			NodeID: "peer-2", Roles: []string{"peer"}, Store: st,
			Catalog: &catalog.Catalog{Models: []catalog.Model{m}}, Selection: sel,
			Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}),
			VerifyOnStart: verify,
		})
	}
	ranges := func(rs ...catalog.ByteRange) *catalog.Selection {
		return &catalog.Selection{Partial: []catalog.Pieces{{
			Model: "glm", Include: []string{"*.json"},
			Ranges: []catalog.FileRanges{{Path: "experts/e.safetensors", Ranges: rs}},
		}}}
	}
	read := func(path string) []byte {
		t.Helper()
		b, err := os.ReadFile(local.FilePath("glm", "r1", path))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	status := func(d *Daemon) server.ModelStatus {
		t.Helper()
		ms := d.Status().Models
		if len(ms) != 1 {
			t.Fatalf("status models = %+v, want one", ms)
		}
		return ms[0]
	}

	// Segments 0 and 3 of the expert file, and config.json whole.
	d := newDaemon(ranges(catalog.ByteRange{Offset: 2, Length: 3}, catalog.ByteRange{Offset: 31, Length: 9}), false)
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ms := status(d)
	if ms.State != server.StatePartial || ms.Selection != server.SelectionPartial {
		t.Fatalf("status = %+v, want state partial", ms)
	}
	if ms.BytesTotal != 7+20 || ms.BytesDone != ms.BytesTotal {
		t.Fatalf("bytes = %d/%d, want 27/27 (the wanted part only)", ms.BytesDone, ms.BytesTotal)
	}
	if got := blobs.Load(); got != 3 {
		t.Fatalf("blob requests = %d, want 3 (config.json, two expert segments)", got)
	}
	if !bytes.Equal(read("config.json"), files["config.json"]) {
		t.Fatal("config.json content mismatch")
	}
	if _, err := os.Stat(local.FilePath("glm", "r1", "dense/vision.bin")); !os.IsNotExist(err) {
		t.Fatalf("dense/vision.bin was created (%v); it is not selected", err)
	}
	e := read("experts/e.safetensors")
	want := files["experts/e.safetensors"]
	if len(e) != len(want) {
		t.Fatalf("expert file is %d bytes, want %d (sparse, full size)", len(e), len(want))
	}
	for i := range e {
		held := i < 10 || (i >= 30 && i < 40)
		if held && e[i] != want[i] || !held && e[i] != 0 {
			t.Fatalf("expert byte %d = %d; held=%v", i, e[i], held)
		}
	}

	// A restart with a wider selection: segment 4 too. Only it is fetched.
	blobs.Store(0)
	d = newDaemon(ranges(catalog.ByteRange{Offset: 2, Length: 3}, catalog.ByteRange{Offset: 31, Length: 15}), false)
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (wider): %v", err)
	}
	if got := blobs.Load(); got != 1 {
		t.Fatalf("blob requests after widening = %d, want 1", got)
	}
	if ms := status(d); ms.State != server.StatePartial || ms.BytesDone != 7+30 {
		t.Fatalf("status = %+v, want partial with 37 bytes", ms)
	}

	// A held segment goes bad on disk. -verify-on-start finds it and
	// fetches it again; without it the state is trusted.
	f, err := os.OpenFile(local.FilePath("glm", "r1", "experts/e.safetensors"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 35); err != nil {
		t.Fatal(err)
	}
	f.Close()
	blobs.Store(0)
	d = newDaemon(ranges(catalog.ByteRange{Offset: 2, Length: 3}, catalog.ByteRange{Offset: 31, Length: 15}), true)
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (verify): %v", err)
	}
	if got := blobs.Load(); got != 1 {
		t.Fatalf("blob requests after re-verify = %d, want 1 (the bad segment)", got)
	}
	if e := read("experts/e.safetensors"); !bytes.Equal(e[30:50], want[30:50]) {
		t.Fatal("bad segment not repaired")
	}

	// Selecting the whole model fills in the rest and reports ready.
	d = newDaemon(&catalog.Selection{Wanted: []string{"glm"}, Partial: ranges().Partial}, false)
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (whole): %v", err)
	}
	if ms := status(d); ms.State != server.StateReady || ms.Selection != "" || ms.BytesDone != manifest.TotalSize() {
		t.Fatalf("status = %+v, want ready with every byte", ms)
	}
	for p, c := range files {
		if !bytes.Equal(read(p), c) {
			t.Fatalf("%s content mismatch", p)
		}
	}
}

// A partial entry for a model the catalog does not have is an error for
// that entry only.
func TestDaemonPartialSelectionUnknownModel(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := catalog.Model{Name: "demo", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID: "peer-1", Roles: []string{"peer"}, Store: st,
		Catalog:   &catalog.Catalog{Models: []catalog.Model{m}},
		Selection: &catalog.Selection{Wanted: []string{"demo"}, Partial: []catalog.Pieces{{Model: "nope"}}},
		Sources:   source.New(nil, ""), Discoverer: discovery.NewStatic(nil),
	})
	err = d.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("Reconcile = %v, want an error naming the unknown model", err)
	}
	if got := len(d.Status().Models); got != 1 {
		t.Fatalf("status models = %d, want 1 (demo is still wanted)", got)
	}
}
