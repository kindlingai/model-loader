package daemon

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
// Files are hashed segment by segment as they arrive, and each file's
// progress is saved to the revision's state at every segment boundary. A
// restart resumes a file that was cut off from its last saved segment with a
// Range request, and keeps a file a previous run finished, without reading
// back anything already on disk. Only a file with an origin-provided sha256
// is resumed or kept; any other is downloaded again from the start. The
// manifest is published only once the whole model is downloaded and matches
// the origin's digests. Serve-while-downloading applies to LAN transfer once
// it is published; see transfer.FetchFile.
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

	// With no manifest saved yet, the revision's state holds only origin
	// progress.
	rs, err := d.cfg.Store.LoadState(&store.Manifest{Model: m.Name, Revision: m.Revision})
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
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

	for i, fm := range manifest.Files {
		of, err := d.originProgress(m, rs, fm, report)
		if err != nil {
			return nil, fmt.Errorf("check %s: %w", fm.Path, err)
		}
		if !of.Complete {
			report(of.BytesDone)
			if err := d.downloadFile(ctx, src, m, rs, of, report); err != nil {
				return nil, fmt.Errorf("download %s: %w", fm.Path, err)
			}
		}
		manifest.Files[i] = of.FileManifest
		done += fm.Size
		report(0)
	}

	if err := d.cfg.Store.SaveManifest(manifest); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	rs = store.NewRevisionState(manifest)
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

// originProgress returns fm's entry in rs.Origin, starting it over unless the
// progress a previous run saved still holds: it was made against the
// origin's current sha256 and the file is still its full size on disk. A
// file a version without origin progress left on disk has no entry; it is
// hashed once, reporting progress, to tell whether it is complete.
func (d *Daemon) originProgress(m catalog.Model, rs *store.RevisionState, fm store.FileManifest, progress func(int64)) (*store.OriginFile, error) {
	path := d.cfg.Store.FilePath(m.Name, m.Revision, fm.Path)
	info, err := os.Stat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	onDisk := err == nil && info.Size() == fm.Size

	for i := range rs.Origin {
		of := &rs.Origin[i]
		if of.Path != fm.Path {
			continue
		}
		if !onDisk || fm.SHA256 == "" || of.SHA256 != fm.SHA256 || of.Size != fm.Size {
			*of = freshOrigin(fm)
		}
		return of, nil
	}

	of := freshOrigin(fm)
	if onDisk && fm.SHA256 != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		segments, whole, err := store.HashFile(f, fm.Size, progress)
		f.Close()
		if err != nil {
			return nil, err
		}
		if whole == fm.SHA256 {
			of.SegmentSHA256, of.BytesDone, of.Complete = segments, fm.Size, true
		}
	}
	rs.Origin = append(rs.Origin, of)
	return &rs.Origin[len(rs.Origin)-1], nil
}

// freshOrigin is progress on fm, as its origin describes it, with nothing
// downloaded yet.
func freshOrigin(fm store.FileManifest) store.OriginFile {
	return store.OriginFile{FileManifest: store.FileManifest{
		Path: fm.Path, Size: fm.Size, SHA256: fm.SHA256, SegmentSize: store.DefaultSegmentSize,
	}}
}

// downloadFile fetches the rest of of's file from its origin, from its last
// saved segment, saving rs at every segment boundary and reporting the
// file's bytes on disk to progress. An origin that can't serve a range gets
// the file from the start. It gives up with errStalled if no bytes arrive
// for Config.StallTimeout, including while waiting for the response.
func (d *Daemon) downloadFile(ctx context.Context, src source.Source, m catalog.Model, rs *store.RevisionState, of *store.OriginFile, progress func(int64)) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stall := time.AfterFunc(d.cfg.StallTimeout, func() { cancel(errStalled) })
	defer stall.Stop()

	rc, err := src.Open(ctx, m, of.Path, of.BytesDone, 0)
	if errors.Is(err, source.ErrRangeIgnored) {
		*of = freshOrigin(of.FileManifest)
		rc, err = src.Open(ctx, m, of.Path, 0, 0)
	}
	if err != nil {
		return stallCause(ctx, err)
	}
	defer rc.Close()

	whole := sha256.New()
	if of.BytesDone > 0 {
		if err := whole.(encoding.BinaryUnmarshaler).UnmarshalBinary(of.SHA256State); err != nil {
			return fmt.Errorf("restore hash state: %w", err)
		}
	}

	f, err := d.cfg.Store.EnsureFile(m.Name, m.Revision, of.FileManifest)
	if err != nil {
		return err
	}
	defer f.Close()
	if of.BytesDone == 0 {
		// A file the origin has since replaced may have been longer.
		if err := f.Truncate(of.Size); err != nil {
			return err
		}
		// Saved before any bytes land, so a restart knows this file is
		// partial instead of hashing it to find out.
		if err := d.cfg.Store.SaveState(rs); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}

	w := &originWriter{
		f: f, of: of, pos: of.BytesDone, whole: whole, seg: sha256.New(),
		onWrite: func() { stall.Reset(d.cfg.StallTimeout) },
		save: func() error {
			progress(of.BytesDone)
			return d.cfg.Store.SaveState(rs)
		},
	}
	if _, err := io.Copy(w, rc); err != nil {
		return stallCause(ctx, err)
	}
	if w.pos != of.Size {
		return fmt.Errorf("short download: got %d bytes, want %d", w.pos, of.Size)
	}

	sum := hex.EncodeToString(whole.Sum(nil))
	if of.SHA256 != "" && sum != of.SHA256 {
		*of = freshOrigin(of.FileManifest)
		if err := d.cfg.Store.SaveState(rs); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		return errors.New("downloaded content does not match origin-provided digest")
	}
	of.SHA256, of.SHA256State, of.Complete = sum, nil, true
	return d.cfg.Store.SaveState(rs)
}

// stallCause reports errStalled instead of the generic context error when
// the stall timer is what canceled ctx.
func stallCause(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, errStalled) {
		return cause
	}
	return err
}

// originWriter writes a file's bytes from its origin in order, hashing them
// as they pass. At each segment boundary it syncs the file and drops the
// segment from page cache, so a single multi-hundred-GB download doesn't
// leave the whole file resident in page cache, which would compete with
// GPU-visible RAM on unified-memory hardware. It then saves progress, so a
// restart loses at most one segment.
type originWriter struct {
	f   *os.File
	of  *store.OriginFile
	pos int64 // bytes written; of.BytesDone catches up at each segment boundary

	whole, seg hash.Hash
	onWrite    func()
	save       func() error
}

func (w *originWriter) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := min(w.of.BytesDone+store.DefaultSegmentSize, w.of.Size)
		chunk := p[written:min(len(p), written+int(end-w.pos))]
		if len(chunk) == 0 {
			return written, fmt.Errorf("origin sent more than %d bytes", w.of.Size)
		}
		n, err := w.f.WriteAt(chunk, w.pos)
		w.whole.Write(chunk[:n])
		w.seg.Write(chunk[:n])
		w.pos += int64(n)
		written += n
		if err != nil {
			return written, err
		}
		w.onWrite()
		if w.pos == end {
			if err := w.endSegment(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// endSegment records the segment just written. Progress is saved after every
// segment but the last; the caller checks the whole file and saves it as
// complete.
func (w *originWriter) endSegment() error {
	start := w.of.BytesDone
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := store.Uncache(w.f, start, w.pos-start); err != nil {
		return fmt.Errorf("uncache: %w", err)
	}
	w.of.SegmentSHA256 = append(w.of.SegmentSHA256, hex.EncodeToString(w.seg.Sum(nil)))
	w.seg.Reset()
	w.of.BytesDone = w.pos
	if w.pos == w.of.Size {
		return nil
	}
	state, err := w.whole.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return fmt.Errorf("save hash state: %w", err)
	}
	w.of.SHA256State = state
	return w.save()
}
