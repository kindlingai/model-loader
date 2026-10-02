package daemon

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// downloadFromWAN resolves m against its configured source and fetches every
// file in full, then builds and saves the manifest other nodes will mirror
// from this one over LAN.
//
// V1 deliberately does not resume a WAN download that was interrupted
// mid-file, and does not publish the manifest until the whole model is
// downloaded and verified: both are the same simplification the design doc
// already makes for LAN transfer (one source at a time, no swarm). Segment-
// level resumability and serve-while-downloading remain fully available for
// LAN peer-to-peer transfer of a model once this step has published it --
// see transfer.FetchFile.
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

	for _, fm := range manifest.Files {
		if err := d.downloadWholeFile(ctx, src, m, fm); err != nil {
			return nil, fmt.Errorf("download %s: %w", fm.Path, err)
		}
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

func (d *Daemon) downloadWholeFile(ctx context.Context, src source.Source, m catalog.Model, fm store.FileManifest) error {
	rc, err := src.Open(ctx, m, fm.Path, 0, 0)
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := d.cfg.Store.EnsureFile(m.Name, m.Revision, fm)
	if err != nil {
		return err
	}
	defer f.Close()

	w := &periodicFlushWriter{f: f, flushEvery: store.DefaultSegmentSize}
	written, err := io.Copy(w, rc)
	if err != nil {
		return err
	}
	if written != fm.Size {
		return fmt.Errorf("short download: got %d bytes, want %d", written, fm.Size)
	}
	return w.flush()
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
}

func (w *periodicFlushWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	w.sinceFlush += int64(n)
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
	return nil
}
