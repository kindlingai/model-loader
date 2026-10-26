package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/server"
)

// Regression (review of #30): a root that downloads a file of small
// segments whole records each segment's sha256 then, from the bytes it
// checked whole. After eviction makes the file partial it still vouches
// for every held segment, a re-verify keeps them, and bytes that rot
// later are vouched for with the sha256 they had, so a copier rejects
// them.
func TestWholeDownloadSmallSegmentsVouched(t *testing.T) {
	withChunk(t, 16<<10)
	const seg = 4 << 10
	content := pattern(64<<10, 'w') // 16 segments, 4 chunks
	f := newPullFleet(t, map[string][]byte{"w.k4.safetensors": content}, &catalog.Selection{Wanted: []string{"glm"}})
	f.model.SegmentSizes = []catalog.SegmentSizeRule{{Path: "*.k4.safetensors", Size: seg}}
	f.root.cfg.Catalog = &catalog.Catalog{Models: []catalog.Model{f.model}}
	if err := f.root.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	mf, err := f.rootSt.LoadManifest("glm", "r1")
	if err != nil {
		t.Fatal(err)
	}
	fm, _ := mf.Find("w.k4.safetensors")
	if fm.SegmentSize != seg || fm.Hashed() {
		t.Fatalf("manifest entry %+v", fm)
	}
	sums := f.rootSt.SegmentSums("glm", "r1", fm, 0, 16)
	for i, h := range sums {
		want := sha256.Sum256(content[i*seg : (i+1)*seg])
		if h != hex.EncodeToString(want[:]) {
			t.Fatalf("side-file sha256 of segment %d = %q", i, h)
		}
	}

	rs, _ := f.root.revState(mf)
	if ok, err := f.root.evict(heldChunks(mf, rs, fm, nil)[0]); !ok || err != nil {
		t.Fatalf("evict: %v %v", ok, err)
	}
	if dropped, err := f.rootSt.Reverify(mf, rs); err != nil || dropped != 0 {
		t.Fatalf("Reverify after evicting a chunk: dropped %d, %v; want none", dropped, err)
	}

	// Segment 6 rots on disk after the whole-file check.
	fh, _ := os.OpenFile(f.rootSt.FilePath("glm", "r1", fm.Path), os.O_RDWR, 0)
	fh.WriteAt([]byte("ROT"), 6*seg+9)
	fh.Close()
	srv := server.New(f.rootSt, f.root)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/blob/glm/r1/w.k4.safetensors", nil)
	req.Header.Set("Range", "bytes=16384-32767") // segments 4-7
	srv.ServeHTTP(rec, req)
	claims := strings.Split(rec.Header().Get(server.HeaderSegmentSHA256), ",")
	if rec.Code != http.StatusPartialContent || len(claims) != 4 {
		t.Fatalf("status %d, claims %q: a partial whole-downloaded file must still vouch", rec.Code, claims)
	}
	if claims[2] != sums[6] {
		t.Fatal("the claim for the rotten segment is not the sha256 recorded at download")
	}
	if got := sha256.Sum256(rec.Body.Bytes()[2*seg : 3*seg]); hex.EncodeToString(got[:]) == claims[2] || bytes.Equal(rec.Body.Bytes()[2*seg:3*seg], content[6*seg:7*seg]) {
		t.Fatal("expected the served bytes to be rotten, and not to match the claim")
	}
}
