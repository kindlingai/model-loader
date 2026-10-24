package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

func withChunk(t *testing.T, n int64) {
	t.Helper()
	old := store.ChunkSize
	store.ChunkSize = n
	t.Cleanup(func() { store.ChunkSize = old })
}

// claimingFetcher serves content and vouches for each segment of what it
// is asked for, as a peer does, unless single, when it vouches only for
// a request of one segment (a peer too old for runs).
type claimingFetcher struct {
	content []byte
	seg     int64
	single  bool
	calls   atomic.Int32
	lengths []int64
}

func (c *claimingFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	c.lengths = append(c.lengths, length)
	body := io.NopCloser(bytes.NewReader(c.content[offset : offset+length]))
	if c.single && length > c.seg {
		return body, nil
	}
	var sums, trusts []string
	for off := offset; off < offset+length; off += c.seg {
		h := sha256.Sum256(c.content[off:min(off+c.seg, int64(len(c.content)))])
		sums, trusts = append(sums, hex.EncodeToString(h[:])), append(trusts, store.TrustOrigin)
	}
	return claimed{ReadCloser: body, sums: sums, trusts: trusts}, nil
}

// Consecutive small segments of one chunk are fetched as one range; a
// chunk boundary starts a new one.
func TestFetchFileRuns(t *testing.T) {
	withChunk(t, 16<<10)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 4 << 10
	content := bytes.Repeat([]byte("0123456789abcdef"), 64<<10/16) // 16 segments, 4 chunks
	fm := store.FileManifest{Path: "w", Size: int64(len(content)), SegmentSize: seg}
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	need := make([]bool, 16)
	for _, i := range []int{1, 2, 3, 4, 5, 9} { // chunk 0: 1-3, chunk 1: 4-5, chunk 2: 9
		need[i] = true
	}
	src := &claimingFetcher{content: content, seg: seg}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{src}, Options{Need: need}); err != nil {
		t.Fatal(err)
	}
	if got := src.calls.Load(); got != 3 {
		t.Fatalf("%d requests (%v), want 3: one per chunk", got, src.lengths)
	}
	fs := rs.FileState(fm)
	for i, n := range need {
		if fs.SegmentsDone[i] != n {
			t.Fatalf("segment %d held %v, want %v", i, fs.SegmentsDone[i], n)
		}
	}
	sums := st.SegmentSums("m", "r1", fm, 0, 16)
	for i, n := range need {
		if (sums[i] != "") != n {
			t.Fatalf("segment %d side-file sha256 %q, held %v", i, sums[i], n)
		}
	}
}

// A source that vouches only for one segment at a time is fetched from
// segment by segment, after the run it could not vouch for.
func TestFetchFileRunFallsBackToSegments(t *testing.T) {
	withChunk(t, 16<<10)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 4 << 10
	content := bytes.Repeat([]byte("x"), 16<<10)
	fm := store.FileManifest{Path: "w", Size: int64(len(content)), SegmentSize: seg}
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	src := &claimingFetcher{content: content, seg: seg, single: true}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{src}, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := src.calls.Load(); got != 5 {
		t.Fatalf("%d requests (%v), want 5: the run, then each of 4 segments", got, src.lengths)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}

// One bad segment in a run fails only that segment, which is fetched from
// the next source.
func TestFetchFileRunBadSegment(t *testing.T) {
	withChunk(t, 16<<10)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 4 << 10
	content := bytes.Repeat([]byte("y"), 16<<10)
	bad := append([]byte(nil), content...)
	bad[2*seg+7] = 'z'
	fm := store.FileManifest{Path: "w", Size: int64(len(content)), SegmentSize: seg}
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	liar := &claimingFetcher{content: bad, seg: seg}
	// The liar's claims are of the good bytes.
	good := &claimingFetcher{content: content, seg: seg}
	lying := fetcherFunc(func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
		rc, _ := good.Open(ctx, path, offset, length)
		body, _ := liar.Open(ctx, path, offset, length)
		c := rc.(claimed)
		c.ReadCloser = body
		return c, nil
	})
	honest := &claimingFetcher{content: content, seg: seg}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{lying, honest}, Options{}); err != nil {
		t.Fatal(err)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
	if honest.calls.Load() == 0 {
		t.Fatal("the honest source was never asked for the bad segment")
	}
}
