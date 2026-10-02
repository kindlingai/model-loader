package transfer

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
)

type fakeStatus struct{ resp server.StatusResponse }

func (f fakeStatus) Status() server.StatusResponse { return f.resp }

func newTestPeer(t *testing.T) (addr string, st *store.Store, m *store.Manifest) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world, this is a peer-served test file")
	fm := buildManifestFile("a.txt", content, int64(len(content)))
	m = &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = true
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}

	srv := server.New(st, fakeStatus{server.StatusResponse{NodeID: "peer-1", Roles: []string{"peer"}}})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts.Listener.Addr().String(), st, m
}

func TestPeerClientStatus(t *testing.T) {
	addr, _, _ := newTestPeer(t)
	c := NewPeerClient(nil)
	resp, err := c.Status(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != "peer-1" {
		t.Fatalf("NodeID = %q, want peer-1", resp.NodeID)
	}
}

func TestPeerClientManifest(t *testing.T) {
	addr, _, m := newTestPeer(t)
	c := NewPeerClient(nil)
	got, err := c.Manifest(context.Background(), addr, m.Model, m.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != m.Model || got.Revision != m.Revision {
		t.Fatalf("got %+v, want model/revision %s/%s", got, m.Model, m.Revision)
	}
}

func TestPeerClientManifestNotFound(t *testing.T) {
	addr, _, _ := newTestPeer(t)
	c := NewPeerClient(nil)
	_, err := c.Manifest(context.Background(), addr, "nope", "nope")
	if err != ErrRangeUnavailable {
		t.Fatalf("err = %v, want ErrRangeUnavailable", err)
	}
}

func TestPeerClientOpenBlobFullFile(t *testing.T) {
	addr, _, m := newTestPeer(t)
	c := NewPeerClient(nil)
	rc, err := c.OpenBlob(context.Background(), addr, m.Model, m.Revision, "a.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello world, this is a peer-served test file" {
		t.Fatalf("body = %q", body)
	}
}

func TestPeerClientOpenBlobRange(t *testing.T) {
	addr, _, m := newTestPeer(t)
	c := NewPeerClient(nil)
	rc, err := c.OpenBlob(context.Background(), addr, m.Model, m.Revision, "a.txt", 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func TestPeerClientOpenBlobUnreadyRange(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789")
	fm := buildManifestFile("a.bin", content, 4)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnsureFile("m", "r1", fm); err != nil {
		t.Fatal(err)
	}
	// Deliberately leave state empty: nothing has been verified yet.

	srv := server.New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := NewPeerClient(nil)
	_, err = c.OpenBlob(context.Background(), ts.Listener.Addr().String(), "m", "r1", "a.bin", 0, 4)
	if err != ErrRangeUnavailable {
		t.Fatalf("err = %v, want ErrRangeUnavailable", err)
	}
}

func TestPeerClientOpenBlobEscapesPath(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("data")
	fm := buildManifestFile("dir with space/file.bin", content, int64(len(content)))
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	for i := range fs.SegmentsDone {
		fs.SegmentsDone[i] = true
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}

	srv := server.New(st, fakeStatus{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := NewPeerClient(nil)
	rc, err := c.OpenBlob(context.Background(), ts.Listener.Addr().String(), "m", "r1", "dir with space/file.bin", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "data" {
		t.Fatalf("body = %q, want %q", body, "data")
	}
}
