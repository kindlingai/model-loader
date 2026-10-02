package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

type fakeStatus struct{ resp StatusResponse }

func (f fakeStatus) Status() StatusResponse { return f.resp }

func setupStore(t *testing.T) (*store.Store, *store.Manifest) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world, this is a test file")
	m := &store.Manifest{
		Model: "m", Revision: "r1",
		Files: []store.FileManifest{{Path: "a.txt", Size: int64(len(content))}},
	}
	if err := m.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return io.NopCloser(newBytesReader(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile("m", "r1", m.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	rs, err := st.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	fs := rs.FileState(m.Files[0])
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = true
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	return st, m
}

func newBytesReader(b []byte) *os.File {
	f, err := os.CreateTemp("", "bytes-*")
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(b); err != nil {
		panic(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	_ = os.Remove(f.Name()) // unlink now; fd stays valid until Close
	return f
}

func TestHandleStatus(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resp := StatusResponse{NodeID: "n1", Roles: []string{"peer"}}
	srv := New(st, fakeStatus{resp})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.StatusCode)
	}
}

func TestHandleManifestFound(t *testing.T) {
	st, m := setupStore(t)
	srv := New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := http.Get(ts.URL + "/manifest/" + m.Model + "/" + m.Revision)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.StatusCode)
	}
}

func TestHandleManifestNotFound(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := http.Get(ts.URL + "/manifest/nope/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", r.StatusCode)
	}
}

func TestHandleBlobFullFile(t *testing.T) {
	st, m := setupStore(t)
	srv := New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := http.Get(ts.URL + "/blob/" + m.Model + "/" + m.Revision + "/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.StatusCode)
	}
	body, _ := io.ReadAll(r.Body)
	if string(body) != "hello world, this is a test file" {
		t.Fatalf("body = %q", body)
	}
}

func TestHandleBlobRange(t *testing.T) {
	st, m := setupStore(t)
	srv := New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/blob/"+m.Model+"/"+m.Revision+"/a.txt", nil)
	req.Header.Set("Range", "bytes=0-4")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", r.StatusCode)
	}
	body, _ := io.ReadAll(r.Body)
	if string(body) != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func TestHandleBlobUnreadyRangeRejected(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789")
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{{Path: "a.bin", Size: int64(len(content))}}}
	if err := m.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return io.NopCloser(newBytesReader(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile("m", "r1", m.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	// Deliberately leave state at "nothing downloaded yet" (LoadState
	// returns fresh empty state when none has been saved).

	srv := New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	r, err := http.Get(ts.URL + "/blob/m/r1/a.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", r.StatusCode)
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		header            string
		size              int64
		start, end        int64
		hasRange, wantErr bool
	}{
		{"", 100, 0, 99, false, false},
		{"bytes=0-9", 100, 0, 9, true, false},
		{"bytes=10-", 100, 10, 99, true, false},
		{"bytes=-10", 100, 90, 99, true, false},
		{"bytes=90-200", 100, 90, 99, true, false},
		{"bytes=abc-def", 100, 0, 0, false, true},
		{"items=0-9", 100, 0, 0, false, true},
		{"bytes=0-9,20-29", 100, 0, 0, false, true},
	}
	for _, tc := range cases {
		start, end, hasRange, err := parseRange(tc.header, tc.size)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseRange(%q): err = %v, wantErr %v", tc.header, err, tc.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		if start != tc.start || end != tc.end || hasRange != tc.hasRange {
			t.Errorf("parseRange(%q) = (%d, %d, %v), want (%d, %d, %v)", tc.header, start, end, hasRange, tc.start, tc.end, tc.hasRange)
		}
	}
}

func TestFilePathParentDirsExist(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := store.FileManifest{Path: "nested/dir/file.bin", Size: 1}
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := os.Stat(filepath.Join(st.FilePath("m", "r1", "nested/dir/file.bin"))); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}
}
