package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Store is the local on-disk content store rooted at a directory:
//
//	<root>/manifests/<model>/<revision>.json
//	<root>/models/<model>/<revision>/<relative-path>
//	<root>/state/<model>/<revision>.json
//
// The models tree holds the materialized files consumers read directly;
// manifests and state are small JSON side files, not meant for consumers.
type Store struct {
	root string
}

// Open prepares a store rooted at dir, creating it if necessary.
func Open(dir string) (*Store, error) {
	s := &Store{root: dir}
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
func (s *Store) SaveState(rs *RevisionState) error {
	rs.UpdatedAt = time.Now().UTC()
	path := s.statePath(rs.Model, rs.Revision)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return marshalJSONAtomic(path, rs)
}

// LoadState reads previously saved revision state, or returns fresh empty
// state derived from the manifest if none has been saved yet.
func (s *Store) LoadState(m *Manifest) (*RevisionState, error) {
	var rs RevisionState
	err := unmarshalJSONFile(s.statePath(m.Model, m.Revision), &rs)
	if os.IsNotExist(err) {
		return NewRevisionState(m), nil
	}
	if err != nil {
		return nil, err
	}
	return &rs, nil
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

// VerifySegment reads segment i of fm from disk and reports whether it
// matches the manifest's hash for that segment. The segment's pages are
// dropped from page cache afterwards: verification is the last read until a
// peer asks for the bytes, and on unified-memory hardware resident cache
// competes with GPU-visible RAM.
func VerifySegment(f *os.File, fm FileManifest, i int) (bool, error) {
	if i >= len(fm.SegmentSHA256) {
		return false, fmt.Errorf("segment %d out of range for %s (%d segments)", i, fm.Path, len(fm.SegmentSHA256))
	}
	start, end := fm.SegmentBounds(i)
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, start, end-start)); err != nil {
		return false, err
	}
	if err := Uncache(f, start, end-start); err != nil {
		return false, fmt.Errorf("uncache %s segment %d: %w", fm.Path, i, err)
	}
	return hex.EncodeToString(h.Sum(nil)) == fm.SegmentSHA256[i], nil
}

// VerifyWholeFile reads the entire file and reports whether it matches the
// manifest's whole-file hash. It is the final authority on correctness,
// used once every segment is believed done. Like VerifySegment, it drops the
// file's pages from page cache once hashed.
func VerifyWholeFile(f *os.File, fm FileManifest) (bool, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	if err := Uncache(f, 0, fm.Size); err != nil {
		return false, fmt.Errorf("uncache %s: %w", fm.Path, err)
	}
	return hex.EncodeToString(h.Sum(nil)) == fm.SHA256, nil
}
