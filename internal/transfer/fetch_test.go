package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{}); err != nil {
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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{empty, full}, Options{}); err != nil {
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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{}); err != nil {
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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{empty}, Options{}); err == nil {
		t.Fatal("expected an error when no fetcher has the range")
	}
}

// deadFetcher stands in for a peer whose pod went away: every Open fails
// outright, the way dialing a deleted pod's IP does.
type deadFetcher struct{ calls int }

func (d *deadFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	d.calls++
	return nil, errors.New("dial tcp 10.244.10.68:7762: connect: no route to host")
}

// A source that fails outright must not fail the file while another source
// has the segment, and must not be dialed again for every later segment.
// See issue #5.
func TestFetchFileFailsOverFromDeadSource(t *testing.T) {
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

	dead := &deadFetcher{}
	root := &fakeFetcher{content: content, ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{dead, root}, Options{}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if dead.calls != 1 {
		t.Fatalf("dead.calls = %d, want 1", dead.calls)
	}
	if root.calls != fm.NumSegments() {
		t.Fatalf("root.calls = %d, want %d", root.calls, fm.NumSegments())
	}
	if !rs.FileState(fm).Complete {
		t.Fatalf("expected file state to be marked complete")
	}
}

// A zero-length file has no segments (the root hashes it to an empty
// SegmentSHA256), so a peer must create it empty without asking any source
// for a segment the root will never report as ready. See issue #1.
func TestFetchFileZeroLength(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := buildManifestFile("pkg/__init__.py", nil, store.DefaultSegmentSize)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	if err := st.SaveManifest(m); err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)

	fetcher := &fakeFetcher{}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if fetcher.calls != 0 {
		t.Fatalf("fetcher.calls = %d, want 0 for a zero-length file", fetcher.calls)
	}
	info, err := os.Stat(st.FilePath("m", "r1", "pkg/__init__.py"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("file size = %d, want 0", info.Size())
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("expected file state to be marked complete")
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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{corrupt}, Options{}); err == nil {
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
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, nil, Options{}); err != nil {
		t.Fatalf("FetchFile on already-complete file: %v", err)
	}
}

// A partial fetch of non-adjacent segments leaves a sparse file at full
// size with each fetched segment at its own offset, and the rest unwritten.
// Fetching more segments later resumes without refetching what is held,
// and the file is checked whole and marked complete only once every
// segment is held.
func TestFetchFilePartialNonAdjacentSegments(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, 50)
	for i := range content {
		content[i] = byte('a' + i%26)
	}
	fm := buildManifestFile("w.bin", content, 10) // five segments
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	fetcher := &fakeFetcher{content: content, ready: int64(len(content))}

	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{Need: []bool{true, false, false, true, false}}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if fetcher.calls != 2 {
		t.Fatalf("fetcher called %d times, want 2 (one per needed segment)", fetcher.calls)
	}
	got, err := os.ReadFile(st.FilePath("m", "r1", "w.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(content) {
		t.Fatalf("file is %d bytes, want the full %d", len(got), len(content))
	}
	for i := range got {
		inHeld := i < 10 || (i >= 30 && i < 40)
		if inHeld && got[i] != content[i] {
			t.Fatalf("byte %d = %q, want %q", i, got[i], content[i])
		}
		if !inHeld && got[i] != 0 {
			t.Fatalf("byte %d = %q, want a hole (0)", i, got[i])
		}
	}
	fs := rs.FileState(fm)
	if fs.Complete || !reflect.DeepEqual(fs.SegmentsDone, []bool{true, false, false, true, false}) {
		t.Fatalf("state = %+v, want segments 0 and 3 done, not complete", fs)
	}

	// A restart: state comes back from disk, and the rest is needed now.
	rs, err = st.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	fetcher.calls = 0
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{}); err != nil {
		t.Fatalf("FetchFile (rest): %v", err)
	}
	if fetcher.calls != 3 {
		t.Fatalf("fetcher called %d times on resume, want 3 (only the missing segments)", fetcher.calls)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete once every segment is held")
	}
	if got, _ := os.ReadFile(st.FilePath("m", "r1", "w.bin")); !bytes.Equal(got, content) {
		t.Fatal("file content mismatch after resume")
	}
}

// A need that is already held fetches nothing, even with no sources.
func TestFetchFilePartialAlreadyHeld(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("z"), 30)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	fs.SegmentsDone[1] = true
	rs.SetFileState(fs)
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, nil, Options{Need: []bool{false, true, false}}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
}

// Every segment held but the file not yet marked complete (a crash before
// the whole-file check was saved): the check runs with no sources at all.
func TestFetchFileAllHeldChecksWholeWithoutSources(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("k"), 25)
	fm := buildManifestFile("w.bin", content, 10)
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	fs := rs.FileState(fm)
	fs.SegmentsDone = []bool{true, true, true}
	rs.SetFileState(fs)
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, nil, Options{Need: []bool{false, true, false}}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not marked complete")
	}
}

// overSender sends one byte more than asked for.
type overSender struct{ content []byte }

func (o overSender) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(append(append([]byte(nil), o.content[offset:offset+length]...), 'X'))), nil
}

// A source that sends more than the segment must not write past it into a
// neighbouring segment, and is dropped for the next source.
func TestFetchFileSourceSendsTooMuch(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789abcdefghij")
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	good := &fakeFetcher{content: content, ready: int64(len(content))}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{overSender{content}, good}, Options{Need: []bool{true, false}}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	got, _ := os.ReadFile(st.FilePath("m", "r1", "w.bin"))
	if got[10] != 0 {
		t.Fatalf("byte 10 = %q: the extra byte landed in the next segment", got[10])
	}
	if good.calls != 1 {
		t.Fatalf("good source called %d times, want 1 after the bad one was dropped", good.calls)
	}
}

// Order picks the sources per segment, and a source it leaves out is not
// tried for that segment.
func TestFetchFileOrderPerSegment(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("o"), 40)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	a := &countingFetcher{content: content}
	b := &countingFetcher{content: content}
	order := func(i int) []int {
		if i%2 == 0 {
			return []int{0}
		}
		return []int{1}
	}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{a, b}, Options{Order: order, Slots: make(chan struct{}, 2)}); err != nil {
		t.Fatalf("FetchFile: %v", err)
	}
	if a.calls.Load() != 2 || b.calls.Load() != 2 {
		t.Fatalf("calls = %d, %d; want 2 each", a.calls.Load(), b.calls.Load())
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}

// countingFetcher is a concurrency-safe fakeFetcher holding everything.
type countingFetcher struct {
	content []byte
	calls   atomic.Int32
}

func (c *countingFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	c.calls.Add(1)
	return io.NopCloser(bytes.NewReader(c.content[offset : offset+length])), nil
}

// A file queued behind a full transfer limit has no descriptor open.
func TestFetchFileOpensOnlyWithASlot(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("s"), 20)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	slots := make(chan struct{}, 1)
	slots <- struct{}{} // taken by another transfer
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = FetchFile(ctx, st, "m", "r1", fm, store.NewRevisionState(m), []Fetcher{&countingFetcher{content: content}}, Options{Slots: slots})
	if err == nil {
		t.Fatal("FetchFile = nil, want the context error")
	}
	if _, err := os.Stat(st.FilePath("m", "r1", "w.bin")); !os.IsNotExist(err) {
		t.Fatalf("destination exists (%v) while waiting for a slot", err)
	}
}

// Between segments, while waiting for a slot again, a file holds no open
// descriptor: segment transfers close it as they finish.
func TestFetchFileClosesBetweenSegments(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("c"), 20)
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	before := openFDs(t)
	slots := make(chan struct{}, 1)
	stolen := make(chan struct{})
	var once sync.Once
	steal := fetcherFunc(func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
		// After the first segment, take the slot as soon as the worker
		// gives it back, so the worker waits for it.
		once.Do(func() {
			go func() { slots <- struct{}{}; close(stolen) }()
		})
		return io.NopCloser(bytes.NewReader(content[offset : offset+length])), nil
	})
	done := make(chan error, 1)
	go func() {
		done <- FetchFile(context.Background(), st, "m", "r1", fm, store.NewRevisionState(m), []Fetcher{steal}, Options{Slots: slots})
	}()
	<-stolen
	time.Sleep(50 * time.Millisecond) // let the worker reach the slot wait
	got := openFDs(t)
	<-slots
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got > before {
		t.Fatalf("%d descriptors open while waiting for a slot, want at most %d", got, before)
	}
}

type fetcherFunc func(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)

func (f fetcherFunc) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return f(ctx, path, offset, length)
}

func openFDs(t *testing.T) int {
	t.Helper()
	n := 0
	for fd := 0; fd < 4096; fd++ {
		var st syscall.Stat_t
		if syscall.Fstat(fd, &st) == nil {
			n++
		}
	}
	return n
}
