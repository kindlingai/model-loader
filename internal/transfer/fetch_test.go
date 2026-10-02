package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

// buildManifestFile computes whole-file and per-segment hashes for content
// using segSize as the chunk size, mirroring store.FileManifest.SegmentBounds
// without going through store.Manifest.BuildSegmentHashes (which is hardcoded
// to store.DefaultSegmentSize, too large to exercise multi-segment behavior
// in a test).
func buildManifestFile(path string, content []byte, segSize int64) store.FileManifest {
	whole := sha256.Sum256(content)
	fm := store.FileManifest{
		Path:        path,
		Size:        int64(len(content)),
		SHA256:      hex.EncodeToString(whole[:]),
		SegmentSize: segSize,
	}
	for start := int64(0); start < int64(len(content)); start += segSize {
		end := start + segSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		h := sha256.Sum256(content[start:end])
		fm.SegmentSHA256 = append(fm.SegmentSHA256, hex.EncodeToString(h[:]))
	}
	return fm
}

// fakeFetcher serves byte ranges out of an in-memory buffer, reporting
// ErrRangeUnavailable for any range outside [0, ready). It also counts how
// many times Open was called, so tests can assert a fetcher further down the
// fallback chain isn't consulted for segments an earlier one already served.
type fakeFetcher struct {
	content []byte
	ready   int64
	calls   int
}

func (f *fakeFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	f.calls++
	end := offset + length
	if offset < 0 || end > f.ready {
		return nil, ErrRangeUnavailable
	}
	return io.NopCloser(bytes.NewReader(f.content[offset:end])), nil
}

func TestFetchFileSingleFetcher(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world")
	fm := buildManifestFile("a.bin", content, int64(len(content)))
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)

	fetcher := &fakeFetcher{content: content, ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}

	got, err := os.ReadFile(st.FilePath("m", "r1", "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Fatalf("file content = %q, want %q", got, content)
	}
	fs := rs.FileState(fm)
	if !fs.Complete {
		t.Fatalf("expected file state to be marked complete")
	}
}

func TestFetchFileFallsBackOnRangeUnavailable(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789abcdef")
	fm := buildManifestFile("a.bin", content, 4)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)

	empty := &fakeFetcher{content: content, ready: 0}
	full := &fakeFetcher{content: content, ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{empty, full}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if empty.calls == 0 {
		t.Fatalf("expected the first fetcher to be tried at least once")
	}
	if full.calls != fm.NumSegments() {
		t.Fatalf("full.calls = %d, want %d", full.calls, fm.NumSegments())
	}

	fs := rs.FileState(fm)
	if !fs.Complete {
		t.Fatalf("expected file state to be marked complete")
	}
}

func TestFetchFileResumesWithoutRefetchingDoneSegments(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789abcdef")
	fm := buildManifestFile("a.bin", content, 4)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}

	// Pre-populate segment 0 on disk and mark it done, as if a previous run
	// had already fetched and verified it.
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content[0:4], 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	fs.SegmentsDone[0] = true
	rs.SetFileState(fs)

	fetcher := &fakeFetcher{content: content, ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if fetcher.calls != fm.NumSegments()-1 {
		t.Fatalf("fetcher.calls = %d, want %d (segment 0 should be skipped)", fetcher.calls, fm.NumSegments()-1)
	}
}

func TestFetchFileFailsWhenNoFetcherHasRange(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world")
	fm := buildManifestFile("a.bin", content, int64(len(content)))
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)

	empty := &fakeFetcher{content: content, ready: 0}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{empty}); err == nil {
		t.Fatal("expected an error when no fetcher has the range")
	}
}

func TestFetchFileDetectsCorruption(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world")
	fm := buildManifestFile("a.bin", content, int64(len(content)))
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)

	corrupt := &fakeFetcher{content: []byte("goodbye worl"), ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{corrupt}); err == nil {
		t.Fatal("expected an error when the fetched bytes fail hash verification")
	}
}

func TestFetchFileAlreadyComplete(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("hello world")
	fm := buildManifestFile("a.bin", content, int64(len(content)))
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	fs.Complete = true
	rs.SetFileState(fs)

	// No fetchers at all: FetchFile must return immediately without trying
	// to open anything, since the file is already marked complete.
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, nil); err != nil {
		t.Fatalf("FetchFile on already-complete file: %v", err)
	}
}
