package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// errStalled is the cancel cause when a download from a model's origin
// receives no bytes for Config.StallTimeout.
var errStalled = errors.New("download stalled: no bytes received")

// downloadFromWAN resolves m against its configured source and fetches every
// file in full, then builds and saves the manifest other nodes will mirror
// from this one over LAN.
//
// A restart resumes at file granularity: a file a previous run finished is
// kept if it matches the origin's sha256, and a file that was cut off is
// downloaded again from the start. The manifest is published only once the
// whole model is downloaded and verified. Segment-level resume and
// serve-while-downloading apply to LAN transfer once it is published; see
// transfer.FetchFile.
func (d *Daemon) downloadFromWAN(ctx context.Context, m catalog.Model) (*store.Manifest, error) {
	src, err := d.cfg.Sources.For(m)
	if err != nil {
		return nil, err
	}
	resolved, err := src.Resolve(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", m.Name, err)
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("source for %s resolved no files", m.Name)
	}

	manifest := &store.Manifest{Model: m.Name, Revision: m.Revision}
	for _, rf := range resolved.Files {
		manifest.Files = append(manifest.Files, store.FileManifest{Path: rf.Path, Size: rf.Size, SHA256: rf.SHA256})
	}

	total := manifest.TotalSize()
	var done int64
	report := func(inFile int64) {
		d.setStatus(server.ModelStatus{
			Model: m.Name, Revision: m.Revision, State: server.StateDownloading,
			BytesTotal: total, BytesDone: done + inFile,
		})
	}
	report(0)

	for _, fm := range manifest.Files {
		have, err := d.alreadyDownloaded(m, fm)
		if err != nil {
			return nil, fmt.Errorf("check %s: %w", fm.Path, err)
		}
		if !have {
			if err := d.downloadWholeFile(ctx, src, m, fm, report); err != nil {
				return nil, fmt.Errorf("download %s: %w", fm.Path, err)
			}
		}
		done += fm.Size
		report(0)
	}

	if err := manifest.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return os.Open(d.cfg.Store.FilePath(m.Name, m.Revision, relPath))
	}); err != nil {
		return nil, fmt.Errorf("hash downloaded files: %w", err)
	}

	for i, rf := range resolved.Files {
		if rf.SHA256 != "" && rf.SHA256 != manifest.Files[i].SHA256 {
			return nil, fmt.Errorf("%s: downloaded content does not match origin-provided digest", rf.Path)
		}
	}

	if err := d.cfg.Store.SaveManifest(manifest); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	rs := store.NewRevisionState(manifest)
	for i := range rs.Files {
		for j := range rs.Files[i].SegmentsDone {
			rs.Files[i].SegmentsDone[j] = true
		}
		rs.Files[i].Complete = true
	}
	if err := d.cfg.Store.SaveState(rs); err != nil {
		return nil, fmt.Errorf("save state: %w", err)
	}

	return manifest, nil
}

// alreadyDownloaded reports whether a previous run left a complete copy of
// fm on disk. Only a file with an origin-provided sha256 can be trusted
// without downloading it again. A file cut off mid-download has its full
// size (store.EnsureFile pre-truncates it), so the hash is what tells it
// apart.
func (d *Daemon) alreadyDownloaded(m catalog.Model, fm store.FileManifest) (bool, error) {
	if fm.SHA256 == "" {
		return false, nil
	}
	f, err := os.Open(d.cfg.Store.FilePath(m.Name, m.Revision, fm.Path))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() != fm.Size {
		return false, nil
	}
	return store.VerifyWholeFile(f, fm)
}

// downloadWholeFile fetches fm from its origin, reporting bytes written to
// progress as they are flushed. It gives up with errStalled if no bytes
// arrive for Config.StallTimeout, including while waiting for the response.
func (d *Daemon) downloadWholeFile(ctx context.Context, src source.Source, m catalog.Model, fm store.FileManifest, progress func(int64)) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stall := time.AfterFunc(d.cfg.StallTimeout, func() { cancel(errStalled) })
	defer stall.Stop()

	rc, err := src.Open(ctx, m, fm.Path, 0, 0)
	if err != nil {
		return stallCause(ctx, err)
	}
	defer rc.Close()

	f, err := d.cfg.Store.EnsureFile(m.Name, m.Revision, fm)
	if err != nil {
		return err
	}
	defer f.Close()

	w := &periodicFlushWriter{
		f:          f,
		flushEvery: store.DefaultSegmentSize,
		onWrite:    func() { stall.Reset(d.cfg.StallTimeout) },
		onFlush:    progress,
	}
	written, err := io.Copy(w, rc)
	if err != nil {
		return stallCause(ctx, err)
	}
	if written != fm.Size {
		return fmt.Errorf("short download: got %d bytes, want %d", written, fm.Size)
	}
	return w.flush()
}

// stallCause reports errStalled instead of the generic context error when
// the stall timer is what canceled ctx.
func stallCause(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, errStalled) {
		return cause
	}
	return err
}

// periodicFlushWriter fsyncs and drops page cache for the bytes written so
// far every flushEvery bytes, so a single multi-hundred-GB WAN download
// doesn't leave the whole file resident in page cache, which would compete
// with GPU-visible RAM on unified-memory hardware.
type periodicFlushWriter struct {
	f          *os.File
	flushEvery int64
	written    int64
	sinceFlush int64
	onWrite    func()
	onFlush    func(written int64)
}

func (w *periodicFlushWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	w.sinceFlush += int64(n)
	w.onWrite()
	if err == nil && w.sinceFlush >= w.flushEvery {
		if ferr := w.flush(); ferr != nil {
			return n, ferr
		}
	}
	return n, err
}

func (w *periodicFlushWriter) flush() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := store.Uncache(w.f, 0, w.written); err != nil {
		return fmt.Errorf("uncache: %w", err)
	}
	w.sinceFlush = 0
	w.onFlush(w.written)
	return nil
}
