package transfer

import (
	"context"
	"fmt"
	"io"
	"os"

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

// uncacheEvery is how many bytes to write to a destination file between
// fsync + page-cache drops, so a large download doesn't balloon resident
// page cache. It matches the segment size so each verified segment's pages
// are dropped once they're durable.
const uncacheEvery = store.DefaultSegmentSize

// FetchFile downloads every not-yet-complete segment of fm into the store,
// trying fetchers in order for each segment and falling back to the next
// one on ErrRangeUnavailable. Each segment is verified against the
// manifest as soon as it lands, and the whole file is verified once more
// when every segment is done. rs is mutated and persisted to st as progress
// is made, so a restart resumes from the last saved segment rather than
// from scratch.
func FetchFile(ctx context.Context, st *store.Store, model, revision string, fm store.FileManifest, rs *store.RevisionState, fetchers []Fetcher) error {
	fileState := rs.FileState(fm)
	if fileState.Complete {
		return nil
	}
	if len(fetchers) == 0 {
		return fmt.Errorf("fetch %s: no sources available", fm.Path)
	}

	f, err := st.EnsureFile(model, revision, fm)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", fm.Path, err)
	}
	defer f.Close()

	var sinceSync int64
	for i := 0; i < fm.NumSegments(); i++ {
		if i < len(fileState.SegmentsDone) && fileState.SegmentsDone[i] {
			continue
		}
		start, end := fm.SegmentBounds(i)
		length := end - start

		n, err := fetchSegment(ctx, f, fm, i, start, length, fetchers)
		if err != nil {
			return fmt.Errorf("fetch %s segment %d: %w", fm.Path, i, err)
		}

		fileState.SegmentsDone[i] = true
		rs.SetFileState(fileState)
		if err := st.SaveState(rs); err != nil {
			return fmt.Errorf("fetch %s: save state: %w", fm.Path, err)
		}

		sinceSync += n
		if sinceSync >= uncacheEvery {
			if err := syncAndUncache(f); err != nil {
				return fmt.Errorf("fetch %s: %w", fm.Path, err)
			}
			sinceSync = 0
		}
	}

	if err := syncAndUncache(f); err != nil {
		return fmt.Errorf("fetch %s: %w", fm.Path, err)
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

// fetchSegment tries each fetcher in order for one segment, returning the
// first success. A fetcher reporting ErrRangeUnavailable is skipped in
// favor of the next one; any other error is returned immediately since it
// likely indicates a real problem (network failure, corrupted data) rather
// than "this source just doesn't have it yet."
func fetchSegment(ctx context.Context, f *os.File, fm store.FileManifest, i int, start, length int64, fetchers []Fetcher) (int64, error) {
	var lastErr error
	for _, fetcher := range fetchers {
		rc, err := fetcher.Open(ctx, fm.Path, start, length)
		if err == ErrRangeUnavailable {
			lastErr = err
			continue
		}
		if err != nil {
			return 0, err
		}

		w := io.NewOffsetWriter(f, start)
		copied, copyErr := io.Copy(w, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			return 0, copyErr
		}
		if closeErr != nil {
			return 0, closeErr
		}
		if copied != length {
			return 0, fmt.Errorf("short read: wanted %d bytes, got %d", length, copied)
		}

		verified, err := store.VerifySegment(f, fm, i)
		if err != nil {
			return 0, err
		}
		if !verified {
			lastErr = fmt.Errorf("segment failed hash verification")
			continue
		}
		return copied, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no source had this segment ready")
	}
	return 0, lastErr
}
