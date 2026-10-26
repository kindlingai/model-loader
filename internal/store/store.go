package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Store is the local on-disk content store rooted at a directory:
//
//	<root>/manifests/<model>/<revision>.json
//	<root>/models/<model>/<revision>/<relative-path>
//	<root>/state/<model>/<revision>.json
//
// The models tree holds the materialized files consumers read directly;
// manifests and state are small JSON side files. What a consumer may rely
// on in all three is the store contract in docs/design.md.
type Store struct {
	root string

	unsavedMu sync.Mutex
	unsaved   map[string]bool // state paths whose last save failed
}

// Open prepares a store rooted at dir, creating it if necessary.
func Open(dir string) (*Store, error) {
	s := &Store{root: dir, unsaved: make(map[string]bool)}
	for _, sub := range []string{"manifests", "models", "state"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
	}
	return s, nil
}

func (s *Store) manifestPath(model, revision string) string {
	return filepath.Join(s.root, "manifests", model, revision+".json")
}

func (s *Store) statePath(model, revision string) string {
	return filepath.Join(s.root, "state", model, revision+".json")
}

// hashesDir is where a revision's segment hash side files are (see
// WriteSegmentHashes). The ".d" keeps a revision with a slash in its name
// ("refs/pr/1") apart from the files of another ("refs").
func (s *Store) hashesDir(model, revision string) string {
	return filepath.Join(s.root, "hashes", model, revision+".d")
}

func (s *Store) hashesPath(model, revision, relPath string) string {
	return filepath.Join(s.hashesDir(model, revision), filepath.FromSlash(relPath)+".sha256")
}

// Dir returns the directory the store is rooted at.
func (s *Store) Dir() string { return s.root }

// ModelDir returns the directory a revision's files are materialized under.
func (s *Store) ModelDir(model, revision string) string {
	return filepath.Join(s.root, "models", model, revision)
}

// FilePath returns the on-disk path for one file within a revision.
func (s *Store) FilePath(model, revision, relPath string) string {
	return filepath.Join(s.ModelDir(model, revision), filepath.FromSlash(relPath))
}

// SaveManifest writes a manifest, replacing any existing one atomically.
func (s *Store) SaveManifest(m *Manifest) error {
	path := s.manifestPath(m.Model, m.Revision)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return marshalJSONAtomic(path, m)
}

// LoadManifest reads a previously saved manifest.
func (s *Store) LoadManifest(model, revision string) (*Manifest, error) {
	var m Manifest
	if err := unmarshalJSONFile(s.manifestPath(model, revision), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// HasManifest reports whether a manifest has been saved for this revision.
func (s *Store) HasManifest(model, revision string) bool {
	_, err := os.Stat(s.manifestPath(model, revision))
	return err == nil
}

// SaveState writes revision state, replacing any existing one atomically.
//
// If it fails, StateSaved reports false for the revision until a save
// succeeds: what is on disk may still mark held bytes this node found bad,
// and must not be served. The old file is removed then, so a consumer
// reading the store does not use those bytes either; a missing state file
// marks nothing held.
func (s *Store) SaveState(rs *RevisionState) error {
	rs.saveMu.Lock()
	defer rs.saveMu.Unlock()
	return s.saveLocked(rs)
}

// stateSaveEvery is how often SaveStateSoon writes a revision's state.
const stateSaveEvery = time.Second

// SaveStateSoon saves rs as SaveState does, unless it was saved less than
// a second ago and the last save succeeded: a fetch of many small
// segments marks one held every few milliseconds, and the state of a
// revision with millions of segments takes longer than that to write.
// Until the next save the state on disk holds less than rs, never more,
// so a consumer reading the store and a restart see fewer segments held,
// not bytes that are not there. The caller saves with SaveState when it
// is done.
func (s *Store) SaveStateSoon(rs *RevisionState) error {
	rs.saveMu.Lock()
	defer rs.saveMu.Unlock()
	if wait := stateSaveEvery - time.Since(rs.lastSave); wait > 0 && s.StateSaved(rs.Model, rs.Revision) {
		// Saved just now: save again once the second is up, so what was
		// marked held since reaches the disk without another caller.
		if rs.flush == nil && !rs.closed {
			rs.flush = time.AfterFunc(wait, func() {
				rs.saveMu.Lock()
				defer rs.saveMu.Unlock()
				rs.flush = nil
				if !rs.closed {
					_ = s.saveLocked(rs)
				}
			})
		}
		return nil
	}
	return s.saveLocked(rs)
}

// Close stops rs being saved again: a save SaveStateSoon has put off is
// dropped, and later saves do nothing. Its revision is about to be
// removed from the store.
func (rs *RevisionState) Close() {
	rs.saveMu.Lock()
	defer rs.saveMu.Unlock()
	rs.closed = true
	if rs.flush != nil {
		rs.flush.Stop()
		rs.flush = nil
	}
}

// saveLocked is SaveState. rs.saveMu is held.
func (s *Store) saveLocked(rs *RevisionState) error {
	if rs.closed {
		return nil
	}
	snap, small := rs.snapshot()
	path := s.statePath(snap.Model, snap.Revision)
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	if err == nil {
		// Compact when there may be many held ranges.
		err = writeJSONAtomic(path, snap, !small)
	}
	if err == nil {
		rs.lastSave = time.Now()
	}
	if err != nil {
		if rmErr := os.Remove(path); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove stale state: %w", rmErr))
		}
	}
	s.unsavedMu.Lock()
	defer s.unsavedMu.Unlock()
	if err != nil {
		s.unsaved[path] = true
	} else {
		delete(s.unsaved, path)
	}
	return err
}

// StateSaved reports whether the last SaveState of the revision, if any,
// succeeded.
func (s *Store) StateSaved(model, revision string) bool {
	s.unsavedMu.Lock()
	defer s.unsavedMu.Unlock()
	return !s.unsaved[s.statePath(model, revision)]
}

// LoadState reads previously saved revision state, or returns fresh empty
// state derived from the manifest if none has been saved yet.
func (s *Store) LoadState(m *Manifest) (*RevisionState, error) {
	rs := &RevisionState{}
	err := unmarshalJSONFile(s.statePath(m.Model, m.Revision), rs)
	if os.IsNotExist(err) {
		return NewRevisionState(m), nil
	}
	if err != nil {
		return nil, err
	}
	// Each file's segments as m has them: a file of small segments lists
	// only what it holds.
	for i := range rs.Files {
		if fm, ok := m.Find(rs.Files[i].Path); ok {
			rs.Files[i].fit(fm)
		}
	}
	return rs, nil
}

// WriteSegmentHashes records the sha256 of segments first, first+1, ...
// of fm, a file of small segments (FileManifest.Small), in its side file
// <store>/hashes/<model>/<revision>.d/<path>.sha256: 32 bytes per segment
// at its index, zero for one not recorded. They are synced before it
// returns, so the segments may be marked held then. This is model-loader's
// own file, not part of the store contract.
func (s *Store) WriteSegmentHashes(model, revision string, fm FileManifest, first int, sums [][sha256.Size]byte) error {
	if len(sums) == 0 {
		return nil
	}
	path := s.hashesPath(model, revision, fm.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return err
	}
	buf := make([]byte, 0, len(sums)*sha256.Size)
	for _, h := range sums {
		buf = append(buf, h[:]...)
	}
	_, err = f.WriteAt(buf, int64(first)*sha256.Size)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// SegmentSums returns the sha256 WriteSegmentHashes recorded for
// segments first to first+n-1 of fm, in hex; "" for one not recorded.
func (s *Store) SegmentSums(model, revision string, fm FileManifest, first, n int) []string {
	out := make([]string, n)
	f, err := os.Open(s.hashesPath(model, revision, fm.Path))
	if err != nil {
		return out
	}
	defer f.Close()
	buf := make([]byte, n*sha256.Size)
	got, _ := f.ReadAt(buf, int64(first)*sha256.Size)
	var zero [sha256.Size]byte
	for i := 0; i < n && (i+1)*sha256.Size <= got; i++ {
		h := buf[i*sha256.Size : (i+1)*sha256.Size]
		if !bytes.Equal(h, zero[:]) {
			out[i] = hex.EncodeToString(h)
		}
	}
	return out
}

// SegmentHashOf returns the sha256 segment i of fm must match: the
// manifest's, the one in its state, or, for a file of small segments, the
// one in its side file. "" if none is known.
func (s *Store) SegmentHashOf(model, revision string, fm FileManifest, fs FileState, i int) string {
	if h := fs.SegmentHash(fm, i); h != "" || fm.Hashed() {
		return h
	}
	if fm.Small() {
		return s.SegmentSums(model, revision, fm, i, 1)[0]
	}
	return ""
}

// EnsureFile makes sure the destination file for fm exists (creating parent
// directories and a zero-length file if needed) and is truncated to at least
// its final size, so writes at arbitrary offsets (resuming a range fetch)
// are always valid.
func (s *Store) EnsureFile(model, revision string, fm FileManifest) (*os.File, error) {
	path := s.FilePath(model, revision, fm.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.Size() < fm.Size {
		if err := f.Truncate(fm.Size); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

// CopyFile writes the file at path of from, fm.Size bytes, into fm's place
// in model@revision, a segment at a time, syncing each and dropping it
// from page cache, and reports the bytes copied so far to progress. On
// Linux the copy is copy_file_range: a filesystem that can clone blocks
// (btrfs, XFS, ZFS block cloning) shares them copy-on-write, and NFS 4.2
// copies on the server. The two are separate files either way, so what
// later punches holes in or rewrites one never touches the other. What
// the destination held before is written over, and it is cut to fm.Size.
func (s *Store) CopyFile(ctx context.Context, model, revision string, fm FileManifest, from RevisionRef, path string, progress func(int64)) error {
	src, err := os.OpenFile(s.FilePath(from.Model, from.Revision, path), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer src.Close()
	if info, err := src.Stat(); err != nil || !info.Mode().IsRegular() || info.Size() != fm.Size {
		return fmt.Errorf("%s@%s %s: not a regular file of %d bytes", from.Model, from.Revision, path, fm.Size)
	}
	dstPath := s.FilePath(model, revision, fm.Path)
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	// Write-only, as a download from the origin writes (see daemon's
	// downloadFile).
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return err
	}
	defer dst.Close()
	for off := int64(0); off < fm.Size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := io.CopyN(dst, src, min(DefaultSegmentSize, fm.Size-off))
		if err != nil {
			return err
		}
		if err := dst.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		if err := errors.Join(Uncache(dst, off, n), Uncache(src, off, n)); err != nil {
			return fmt.Errorf("uncache: %w", err)
		}
		off += n
		progress(off)
	}
	if err := dst.Truncate(fm.Size); err != nil { // it may have been longer
		return err
	}
	return dst.Close()
}

// CopySegmentHashes writes the side file of the file at path of from (see
// WriteSegmentHashes) as fm's in model@revision: the two files hold the
// same bytes in segments of the same size. It is synced before it returns.
func (s *Store) CopySegmentHashes(model, revision string, fm FileManifest, from RevisionRef, path string) error {
	src, err := os.Open(s.hashesPath(from.Model, from.Revision, path))
	if err != nil {
		return err
	}
	defer src.Close()
	dstPath := s.hashesPath(model, revision, fm.Path)
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	if err := dst.Sync(); err != nil {
		return err
	}
	return dst.Close()
}

// hashUncached writes bytes [start, start+length) of f to w in chunks of
// DefaultSegmentSize, dropping each chunk from page cache as soon as it is
// read. Hashing is the last read of these bytes until a peer asks for them,
// and on unified-memory hardware resident cache competes with GPU-visible
// RAM, so at most one chunk stays resident however large the file is.
func hashUncached(f *os.File, start, length int64, w io.Writer) error {
	for off, end := start, start+length; off < end; {
		n := min(DefaultSegmentSize, end-off)
		copied, err := io.Copy(w, io.NewSectionReader(f, off, n))
		if err != nil {
			return err
		}
		if copied != n {
			return fmt.Errorf("short read at offset %d: wanted %d bytes, got %d", off, n, copied)
		}
		if err := Uncache(f, off, n); err != nil {
			return fmt.Errorf("uncache: %w", err)
		}
		off += n
	}
	return nil
}

// VerifySegment reads segment i of fm from disk and reports whether it
// matches the manifest's hash for that segment, dropping the segment's pages
// from page cache afterwards.
func VerifySegment(f *os.File, fm FileManifest, i int) (bool, error) {
	if i >= len(fm.SegmentSHA256) {
		return false, fmt.Errorf("segment %d out of range for %s (%d segments)", i, fm.Path, len(fm.SegmentSHA256))
	}
	start, end := fm.SegmentBounds(i)
	h := sha256.New()
	if err := hashUncached(f, start, end-start, h); err != nil {
		return false, fmt.Errorf("%s segment %d: %w", fm.Path, i, err)
	}
	return hex.EncodeToString(h.Sum(nil)) == fm.SegmentSHA256[i], nil
}

// openRegular opens path read-write, failing if it is a symlink or not a
// regular file.
func openRegular(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s: not a regular file", path)
	}
	return f, nil
}

// Reverify hashes every segment rs records as done against the sha256 it
// must match (see FileState.SegmentHash), and marks any that no longer
// match (or whose file is gone or cannot be opened) as not done, along
// with its file's completeness, so the next fetch gets it again. A file
// that is not exactly its manifest size is put back to that size and is no
// longer complete, so it is checked whole again once every segment is
// held. It saves rs, even when it fails part way, so nothing it found bad
// stays marked held, and returns how many segments it dropped.
func (s *Store) Reverify(m *Manifest, rs *RevisionState) (int, error) {
	dropped := 0
	var errs []error
	for _, fm := range m.Files {
		fs := rs.FileState(fm)
		var sums []string // a file of small segments: its side file, read once.
		if fm.Small() && !fm.Hashed() {
			sums = s.SegmentSums(m.Model, m.Revision, fm, 0, fm.NumSegments())
		}
		held := fs.Complete
		for _, done := range fs.SegmentsDone {
			held = held || done
		}
		if !held {
			continue
		}
		// The file is opened without following a symlink, and its size is
		// put back through the same descriptor, so nothing outside the
		// store is read or truncated. Anything but a regular file is gone.
		f, err := openRegular(s.FilePath(m.Model, m.Revision, fm.Path))
		if err != nil {
			f = nil // gone or unreadable: nothing in it can be trusted.
		}
		if f != nil {
			if info, err := f.Stat(); err != nil || info.Size() != fm.Size {
				rs.update(fm, func(fs *FileState) { fs.Complete = false })
				if err == nil {
					if err := f.Truncate(fm.Size); err != nil {
						errs = append(errs, fmt.Errorf("%s: restore size: %w", fm.Path, err))
					}
				}
			}
		}
		for i, done := range fs.SegmentsDone {
			if !done || i >= fm.NumSegments() {
				continue
			}
			ok := false
			want := fs.SegmentHash(fm, i)
			if sums != nil {
				want = sums[i]
			}
			if want == "" && fs.Complete && fm.SHA256 != "" && f != nil {
				// Verified whole, and no segment hash recorded (a root's
				// whole download of a file of small segments): the
				// whole-file check below speaks for it.
				continue
			}
			if f != nil && want != "" {
				sum, err := HashSegment(f, fm, i)
				ok = err == nil && sum == want // an error is a short file: the segment is gone.
			}
			if !ok {
				rs.DropSegment(fm, i)
				dropped++
			}
		}
		if fs.Complete && (f == nil || ((fm.NumSegments() == 0 || (sums != nil && hasEmpty(sums))) && !wholeFileOK(f, fm))) {
			if sums != nil && f != nil {
				for i := range fm.NumSegments() {
					rs.DropSegment(fm, i)
					dropped++
				}
			}
			rs.update(fm, func(fs *FileState) { fs.Complete = false })
		}
		if f != nil {
			f.Close()
		}
	}
	if err := s.SaveState(rs); err != nil {
		errs = append(errs, err)
	}
	return dropped, errors.Join(errs...)
}

func hasEmpty(sums []string) bool {
	for _, h := range sums {
		if h == "" {
			return true
		}
	}
	return false
}

func wholeFileOK(f *os.File, fm FileManifest) bool {
	ok, err := VerifyWholeFile(f, fm)
	return err == nil && ok
}

// HashSegment returns the sha256 of segment i of fm as it is on disk,
// dropping its pages from page cache afterwards.
func HashSegment(f *os.File, fm FileManifest, i int) (string, error) {
	start, end := fm.SegmentBounds(i)
	h := sha256.New()
	if err := hashUncached(f, start, end-start, h); err != nil {
		return "", fmt.Errorf("%s segment %d: %w", fm.Path, i, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// HashSegments returns the sha256 of segments first to first+n-1 of fm
// as they are on disk, streaming each through the hash (no segment is
// held in memory) and dropping its pages from page cache afterwards.
func HashSegments(f *os.File, fm FileManifest, first, n int) ([][sha256.Size]byte, error) {
	out := make([][sha256.Size]byte, n)
	h := sha256.New()
	for k := range n {
		start, end := fm.SegmentBounds(first + k)
		h.Reset()
		if err := hashUncached(f, start, end-start, h); err != nil {
			return nil, fmt.Errorf("%s segment %d: %w", fm.Path, first+k, err)
		}
		h.Sum(out[k][:0])
	}
	return out, nil
}

// VerifyWholeFile reports whether the file is exactly fm.Size bytes and
// matches the manifest's whole-file hash. It is the final authority on
// correctness, used once every segment is believed done. Pages are dropped
// from page cache chunk by chunk as they are hashed.
func VerifyWholeFile(f *os.File, fm FileManifest) (bool, error) {
	if fm.SHA256 == "" {
		// A file the origin gave no digest for (only in a manifest a root
		// published for pull-through): every segment was checked as it
		// landed, and there is nothing more to check it against.
		info, err := f.Stat()
		return err == nil && info.Size() == fm.Size, err
	}
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() != fm.Size {
		return false, nil
	}
	h := sha256.New()
	if err := hashUncached(f, 0, fm.Size, h); err != nil {
		return false, fmt.Errorf("%s: %w", fm.Path, err)
	}
	return hex.EncodeToString(h.Sum(nil)) == fm.SHA256, nil
}
