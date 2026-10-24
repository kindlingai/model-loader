package server

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

// A request of several whole small segments is answered with a sha256
// and a trust for each, from the side file; one not aligned to segments
// gets none.
func TestBlobClaimsRun(t *testing.T) {
	old := store.ChunkSize
	store.ChunkSize = 1 << 20
	t.Cleanup(func() { store.ChunkSize = old })
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 4 << 10
	content := []byte(strings.Repeat("abcdefgh", 8*seg/8))
	fm := store.FileManifest{Path: "w", Size: int64(len(content)), SegmentSize: seg}
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	f, _ := st.EnsureFile("m", "r1", fm)
	f.WriteAt(content, 0)
	sums, _ := store.HashSegments(f, fm, 0, 8)
	f.Close()
	if err := st.WriteSegmentHashes("m", "r1", fm, 0, sums); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)
	for i := range 8 {
		rs.MarkSegment(fm, i, "", store.TrustOrigin)
	}
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	srv := New(st, fakeStatus{})
	get := func(rng string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/blob/m/r1/w", nil)
		req.Header.Set("Range", rng)
		srv.ServeHTTP(rec, req)
		return rec
	}
	rec := get("bytes=8192-20479") // segments 2-4
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status %d", rec.Code)
	}
	got := strings.Split(rec.Header().Get(HeaderSegmentSHA256), ",")
	if len(got) != 3 || got[0] != hex.EncodeToString(sums[2][:]) || got[2] != hex.EncodeToString(sums[4][:]) {
		t.Fatalf("claims %q", got)
	}
	if tr := rec.Header().Get(HeaderSegmentTrust); tr != "origin,origin,origin" {
		t.Fatalf("trust %q", tr)
	}
	if rec := get("bytes=8193-20479"); rec.Header().Get(HeaderSegmentSHA256) != "" {
		t.Fatal("a range not aligned to segments got claims")
	}
	h := sha256.Sum256(content[2*seg : 3*seg])
	if got[0] != hex.EncodeToString(h[:]) {
		t.Fatal("claim is not the segment's sha256")
	}
}
