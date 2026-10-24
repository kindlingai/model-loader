package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// Fetcher opens a byte range of one file. length <= 0 means to EOF.
// Implementations should return ErrRangeUnavailable when they don't have the
// requested range yet, so FetchFile can fall back to the next source instead
// of failing outright.
type Fetcher interface {
	Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
}

// WANFetcher adapts a WAN source.Source, bound to one catalog model, to the
// Fetcher interface.
type WANFetcher struct {
	Source source.Source
	Model  catalog.Model
}

func (w WANFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return w.Source.Open(ctx, w.Model, path, offset, length)
}

// PeerFetcher adapts a PeerClient bound to one peer address and
// model/revision to the Fetcher interface.
type PeerFetcher struct {
	Client          *PeerClient
	Addr            string
	Model, Revision string
}

func (p PeerFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return p.Client.OpenBlob(ctx, p.Addr, p.Model, p.Revision, path, offset, length)
}

// Options tunes FetchFile. The zero value fetches every segment, trying
// every fetcher in order, one segment at a time.
type Options struct {
	// Need marks the segments to fetch. nil means every segment.
	Need []bool
	// Order, if set, returns the indices into the fetchers to try for
	// segment i, in order. A fetcher it leaves out is not tried for that
	// segment.
	Order func(i int) []int
	// Slots, if set, is a semaphore shared with every other transfer on
	// the node: each segment transfer holds one slot while it runs. Its
	// capacity is also how many segments of this file are fetched at once,
	// so different segments can come from different sources in parallel.
	Slots chan struct{}
}

// FetchFile downloads the not-yet-done segments of fm that opts.Need marks
// into the store. For each segment it tries the fetchers opts.Order names,
// falling back to the next one when a fetcher doesn't have it yet or
// fails. Each segment is verified against the manifest as soon as it
// lands. Once every segment of the file is done, needed or not, the whole
// file is verified once more and marked complete; until then the file is
// partial, a sparse file with every held segment at its own offset. rs is
// mutated and persisted to st as progress is made, so a restart resumes
// from the last saved segment rather than from scratch.
func FetchFile(ctx context.Context, st *store.Store, model, revision string, fm store.FileManifest, rs *store.RevisionState, fetchers []Fetcher, opts Options) error {
	need := opts.Need
	fileState := rs.FileState(fm)
	if fileState.Complete {
		return nil
	}
	if len(fileState.SegmentsDone) != fm.NumSegments() {
		// State that does not match the manifest's segments: keep what
		// lines up, and fetch the rest.
		done := make([]bool, fm.NumSegments())
		copy(done, fileState.SegmentsDone)
		fileState.SegmentsDone = done
	}
	if need != nil && len(need) != fm.NumSegments() {
		return fmt.Errorf("fetch %s: need has %d segments, manifest %d", fm.Path, len(need), fm.NumSegments())
	}
	var todo []int
	for i, done := range fileState.SegmentsDone {
		if !done && (need == nil || need[i]) {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 && !allDone(fileState.SegmentsDone) {
		return nil // every needed segment is already held.
	}
	if len(todo) > 0 && len(fetchers) == 0 {
		return fmt.Errorf("fetch %s: no sources available", fm.Path)
	}

	// The destination is opened only once a segment transfer has a slot
	// (or there is only the whole-file check left), so files queued
	// behind the transfer limit hold no descriptors.
	var opened *os.File
	openFile := sync.OnceValues(func() (*os.File, error) {
		f, err := st.EnsureFile(model, revision, fm)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", fm.Path, err)
		}
		opened = f
		return f, nil
	})
	defer func() {
		if opened != nil {
			opened.Close()
		}
	}()

	if opts.Slots != nil && cap(opts.Slots) == 0 {
		opts.Slots = nil // an unbuffered channel would block every send.
	}
	order := opts.Order
	if order == nil {
		all := make([]int, len(fetchers))
		for k := range all {
			all[k] = k
		}
		order = func(int) []int { return all }
	}
	workers := 1
	if opts.Slots != nil {
		workers = max(1, cap(opts.Slots))
	}
	workers = min(workers, max(1, len(todo)))

	var (
		mu       sync.Mutex // guards fileState, rs, dead and firstErr
		dead     = make([]bool, len(fetchers))
		firstErr error
		wg       sync.WaitGroup
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	queue := make(chan int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range queue {
				if opts.Slots != nil {
					select {
					case opts.Slots <- struct{}{}:
					case <-ctx.Done():
						fail(ctx.Err())
						continue
					}
				}
				f, err := openFile()
				if err == nil {
					err = fetchSegment(ctx, f, fm, i, fetchers, order(i), dead, &mu)
				}
				if opts.Slots != nil {
					<-opts.Slots
				}
				if err != nil {
					fail(fmt.Errorf("fetch %s segment %d: %w", fm.Path, i, err))
					continue
				}
				mu.Lock()
				fileState.SegmentsDone[i] = true
				rs.SetFileState(fileState)
				err = st.SaveState(rs)
				mu.Unlock()
				if err != nil {
					fail(fmt.Errorf("fetch %s: save state: %w", fm.Path, err))
				}
			}
		}()
	}
	for _, i := range todo {
		queue <- i
	}
	close(queue)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	f, err := openFile()
	if err != nil {
		return err
	}
	if err := syncAndUncache(f); err != nil {
		return fmt.Errorf("fetch %s: %w", fm.Path, err)
	}
	if !allDone(fileState.SegmentsDone) {
		return nil // partial: the rest of the file is not needed.
	}

	ok, err := store.VerifyWholeFile(f, fm)
	if err != nil {
		return fmt.Errorf("fetch %s: verify whole file: %w", fm.Path, err)
	}
	if !ok {
		return fmt.Errorf("fetch %s: whole-file verification failed after every segment passed", fm.Path)
	}
	fileState.Complete = true
	rs.SetFileState(fileState)
	return st.SaveState(rs)
}

func allDone(segments []bool) bool {
	for _, done := range segments {
		if !done {
			return false
		}
	}
	return true
}

// syncAndUncache fsyncs f, then advises the kernel to drop its page cache.
// Only data that's durable on disk should ever be dropped from cache, hence
// the fsync first.
func syncAndUncache(f *os.File) error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if err := store.Uncache(f, 0, info.Size()); err != nil {
		return fmt.Errorf("uncache: %w", err)
	}
	return nil
}

// fetchSegment tries the fetchers order names, in turn, for segment i,
// returning at the first success. A fetcher reporting ErrRangeUnavailable
// is skipped for this segment only. One that fails outright (a peer that
// went away mid-copy, a dropped connection, data that fails verification)
// is marked in dead and skipped for the rest of the file, so the copy fails
// over to the next peer or the root instead of failing, and doesn't wait on
// a dead address again for every segment. mu guards dead.
//
// At most the segment's own bytes are written, so a source that sends too
// much cannot overwrite a neighbouring segment another worker holds. The
// segment is synced before it is verified, so verifying drops its pages
// from page cache.
func fetchSegment(ctx context.Context, f *os.File, fm store.FileManifest, i int, fetchers []Fetcher, order []int, dead []bool, mu *sync.Mutex) error {
	start, end := fm.SegmentBounds(i)
	length := end - start
	isDead := func(k int) bool {
		mu.Lock()
		defer mu.Unlock()
		return dead[k]
	}
	kill := func(k int) {
		mu.Lock()
		dead[k] = true
		mu.Unlock()
	}
	var lastErr error
	for _, k := range order {
		if k < 0 || k >= len(fetchers) || isDead(k) {
			continue
		}
		rc, err := fetchers[k].Open(ctx, fm.Path, start, length)
		if errors.Is(err, ErrRangeUnavailable) {
			lastErr = err
			continue
		}
		if err != nil {
			kill(k)
			lastErr = err
			continue
		}

		// Write at most the segment's own bytes, so a source that sends
		// too much cannot overwrite a neighbouring segment that is held.
		copied, copyErr := io.Copy(io.NewOffsetWriter(f, start), io.LimitReader(rc, length))
		extra := 0
		if copyErr == nil && copied == length {
			// Only a full, good copy is probed for more: a failed one is
			// dropped at once, not held open waiting on the source.
			extra, _ = rc.Read(make([]byte, 1))
		}
		closeErr := rc.Close()
		var bad error
		switch {
		case copyErr != nil:
			bad = copyErr
		case copied != length:
			bad = fmt.Errorf("short read: wanted %d bytes, got %d", length, copied)
		case extra > 0:
			bad = fmt.Errorf("source sent more than the %d bytes asked for", length)
		case closeErr != nil:
			bad = closeErr
		}
		if bad != nil {
			kill(k)
			lastErr = bad
			continue
		}

		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		verified, err := store.VerifySegment(f, fm, i)
		if err != nil {
			return err
		}
		if !verified {
			kill(k)
			lastErr = errors.New("segment failed hash verification")
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no source left to try")
	}
	return lastErr
}
