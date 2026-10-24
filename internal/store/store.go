package store

import (
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
	snap := rs.snapshot()
	path := s.statePath(snap.Model, snap.Revision)
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	if err == nil {
		err = marshalJSONAtomic(path, snap)
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
	return rs, nil
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
			if want := fs.SegmentHash(fm, i); f != nil && want != "" {
				sum, err := HashSegment(f, fm, i)
				ok = err == nil && sum == want // an error is a short file: the segment is gone.
			}
			if !ok {
				rs.DropSegment(fm, i)
				dropped++
			}
		}
		if fs.Complete && (f == nil || (fm.NumSegments() == 0 && !wholeFileOK(f, fm))) {
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
