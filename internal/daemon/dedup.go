package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/want"
)

// Content dedup (kindlingai/model-loader#38).
//
// With Config.Dedup, a node about to fetch a whole file first looks for a
// file of the same size and sha256 that a revision in its store holds
// complete, and copies that instead: a root before it downloads the file
// from the origin, a peer before it fetches it over the LAN. The
// copy is a file of its own (see store.CopyFile), cloned where the
// filesystem can, so eviction, repair and removal of one revision never
// touch the other, as they would through a hard link.
//
// The copy is not hashed again: its twin was checked whole when it
// landed, and the sha256 of each segment is carried over from it where it
// has them at the segment size the new file needs. Otherwise the copy is
// hashed once.

// twin is a file a revision in the store has, a candidate to copy.
type twin struct {
	mf *store.Manifest
	fm store.FileManifest
}

// twins indexes by sha256 every file with an origin digest in the store's
// manifests. Whether one is still held whole is checked when it is used.
func (d *Daemon) twins() map[string][]twin {
	out := make(map[string][]twin)
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		log.Printf("model-loader: dedup: %v", err)
		return out
	}
	for _, ref := range refs {
		mf, err := d.cfg.Store.LoadManifest(ref.Model, ref.Revision)
		if err != nil {
			continue // a revision with no manifest
		}
		for _, fm := range mf.Files {
			if fm.SHA256 != "" && fm.Size > 0 {
				out[fm.SHA256] = append(out[fm.SHA256], twin{mf, fm})
			}
		}
	}
	return out
}

// errNotHeld is a twin that is no longer held whole.
var errNotHeld = errors.New("no longer held whole")

// reuse copies into fm's place in m's revision a twin held whole with
// fm's size and sha256, if there is one, and returns the sha256 of each
// of fm's segments: nil for a file of small segments, whose side file it
// writes instead. ok is false when no twin could be copied; the caller
// fetches the file as before.
func (d *Daemon) reuse(ctx context.Context, m catalog.Model, fm store.FileManifest, twins map[string][]twin, progress func(int64)) (sums []string, ok bool) {
	for _, t := range twins[fm.SHA256] {
		if t.fm.Size != fm.Size || (t.mf.Model == m.Name && t.mf.Revision == m.Revision && t.fm.Path == fm.Path) {
			continue
		}
		// The cheap re-check: segment hashes of the same size agree too.
		if fm.Hashed() && t.fm.Hashed() && fm.SegmentSize == t.fm.SegmentSize && !slices.Equal(fm.SegmentSHA256, t.fm.SegmentSHA256) {
			continue
		}
		sums, err := d.reuseFrom(ctx, m, fm, t, progress)
		if err != nil {
			if ctx.Err() != nil {
				return nil, false
			}
			if !errors.Is(err, errNotHeld) {
				log.Printf("model-loader: dedup %s@%s %s from %s@%s: %v", m.Name, m.Revision, fm.Path, t.mf.Model, t.mf.Revision, err)
			}
			continue
		}
		log.Printf("model-loader: %s@%s %s: copied %d bytes from %s@%s %s instead of fetching", m.Name, m.Revision, fm.Path, fm.Size, t.mf.Model, t.mf.Revision, t.fm.Path)
		d.mu.Lock()
		d.reused[want.Key(m.Name, m.Revision)] += fm.Size
		d.mu.Unlock()
		return sums, true
	}
	return nil, false
}

func (d *Daemon) reuseFrom(ctx context.Context, m catalog.Model, fm store.FileManifest, t twin, progress func(int64)) ([]string, error) {
	// Marked as being written, so eviction and retiring leave the twin
	// alone; checked whole only after, so one evicted just before is seen.
	defer d.writing(t.mf.Model, t.mf.Revision, t.fm)()
	// Retired since twins read it: revState would cache an empty state
	// for it, which a later download of the same revision would use.
	if !d.cfg.Store.HasManifest(t.mf.Model, t.mf.Revision) {
		return nil, errNotHeld
	}
	rs, err := d.revState(t.mf)
	if err != nil {
		return nil, err
	}
	whole, lost := rs.Whole(t.fm)
	if !whole {
		return nil, errNotHeld
	}
	from := store.RevisionRef{Model: t.mf.Model, Revision: t.mf.Revision}
	if err := d.cfg.Store.CopyFile(ctx, m.Name, m.Revision, fm, from, t.fm.Path, progress); err != nil {
		return nil, err
	}
	sums, err := d.reusedSums(m, fm, t, rs)
	if err != nil {
		return nil, err
	}
	// A re-verify may have found part of the twin bad while it was copied,
	// and it may have been fetched again since: the copy read it as it was.
	if whole, now := rs.Whole(t.fm); !whole || now != lost {
		return nil, errNotHeld
	}
	return sums, nil
}

// reusedSums returns the sha256 of each of fm's segments, the copy of t
// having just been made: fm's own, if its manifest entry has them; for a
// file of small segments nil, its side file written; else t's, if it has
// them at fm's segment size. Failing those it hashes the copy.
func (d *Daemon) reusedSums(m catalog.Model, fm store.FileManifest, t twin, rs *store.RevisionState) ([]string, error) {
	switch {
	case fm.Hashed():
		return fm.SegmentSHA256, nil
	case fm.Small():
		if t.fm.Small() && t.fm.SegmentSize == fm.SegmentSize &&
			d.cfg.Store.CopySegmentHashes(m.Name, m.Revision, fm, store.RevisionRef{Model: t.mf.Model, Revision: t.mf.Revision}, t.fm.Path) == nil {
			return nil, nil
		}
		return nil, d.hashSmallFile(m, fm)
	case t.fm.SegmentSize == fm.SegmentSize && !t.fm.Small():
		fs := rs.FileState(t.fm)
		sums := make([]string, fm.NumSegments())
		for i := range sums {
			if sums[i] = d.cfg.Store.SegmentHashOf(t.mf.Model, t.mf.Revision, t.fm, fs, i); sums[i] == "" {
				sums = nil
				break
			}
		}
		if sums != nil {
			return sums, nil
		}
	}
	f, err := os.Open(d.cfg.Store.FilePath(m.Name, m.Revision, fm.Path))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sums, whole, err := store.HashFile(f, fm.Size, fm.SegmentSize, func(int64) {})
	if err != nil {
		return nil, err
	}
	if whole != fm.SHA256 {
		return nil, fmt.Errorf("the copy's sha256 is %s, not %s", whole, fm.SHA256)
	}
	return sums, nil
}

// reuseHeld is reuse for a file of a revision this node has the manifest
// of and holds none of: once copied, every segment of fm is marked held
// and the file complete, and rs saved. A file it holds part of fetches
// the rest: the copy writes the whole file, so one that failed would
// leave segments marked held with the twin's bytes in them. Every segment
// is claimed as a root's pull claims those it fetches (see
// claimSegments), so a peer's pull into the file waits for the copy
// rather than writes under it; one already pulling part of it leaves the
// file to be fetched.
func (d *Daemon) reuseHeld(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, twins map[string][]twin) (bool, error) {
	file := claimKey(m, fm)
	mine, theirs := d.claimSegments(file, fm, rs, slices.Repeat([]bool{true}, fm.NumSegments()))
	defer d.releaseSegments(file, mine)
	if theirs != nil || mine == nil || slices.Contains(mine, false) {
		return false, nil // held or being pulled, in part
	}
	sums, ok := d.reuse(ctx, m, fm, twins, func(int64) {})
	if !ok {
		return false, nil
	}
	for i := range fm.NumSegments() {
		sum := ""
		if sums != nil {
			sum = sums[i]
		}
		rs.MarkSegment(fm, i, sum, store.TrustManifest)
	}
	if !rs.SetComplete(fm) {
		return false, nil // a segment was dropped meanwhile: fetch it.
	}
	return true, d.cfg.Store.SaveState(rs)
}

// hashSmallFile writes the sha256 of every segment of fm, a file of small
// segments held whole, to its side file, a chunk at a time.
func (d *Daemon) hashSmallFile(m catalog.Model, fm store.FileManifest) error {
	f, err := os.Open(d.cfg.Store.FilePath(m.Name, m.Revision, fm.Path))
	if err != nil {
		return err
	}
	defer f.Close()
	for c := range fm.NumChunks() {
		first, end := fm.ChunkBounds(c)
		sums, err := store.HashSegments(f, fm, first, end-first)
		if err != nil {
			return err
		}
		if err := d.cfg.Store.WriteSegmentHashes(m.Name, m.Revision, fm, first, sums); err != nil {
			return err
		}
	}
	return nil
}
