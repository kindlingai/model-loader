package store

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// RevisionRef names one model revision in the store.
type RevisionRef struct {
	Model, Revision string
}

// Revisions lists every model revision the store has a manifest or state
// for.
func (s *Store) Revisions() ([]RevisionRef, error) {
	seen := make(map[RevisionRef]bool)
	var out []RevisionRef
	for _, sub := range []string{"manifests", "state"} {
		dir := filepath.Join(s.root, sub)
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".json") || strings.HasPrefix(d.Name(), ".") {
				return nil
			}
			rel, err := filepath.Rel(dir, strings.TrimSuffix(path, ".json"))
			if err != nil {
				return err
			}
			model, rev := filepath.Split(filepath.ToSlash(rel))
			ref := RevisionRef{strings.TrimSuffix(model, "/"), rev}
			if ref.Model != "" && !seen[ref] {
				seen[ref] = true
				out = append(out, ref)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("list revisions: %w", err)
		}
	}
	return out, nil
}

// OriginBytes returns how many bytes a root has downloaded of a revision
// it has no manifest for yet (see RevisionState.Origin).
func (s *Store) OriginBytes(model, revision string) (int64, error) {
	var rs RevisionState
	err := unmarshalJSONFile(s.statePath(model, revision), &rs)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n int64
	for _, of := range rs.Origin {
		n += of.BytesDone
	}
	return n, nil
}

// RemoveRevision deletes a revision's files, state and manifest.
func (s *Store) RemoveRevision(model, revision string) error {
	errs := []error{os.RemoveAll(s.ModelDir(model, revision))}
	for _, p := range []string{s.statePath(model, revision), s.manifestPath(model, revision)} {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// FreeSegment gives the disk space of segment i of fm back to the
// filesystem: it punches a hole over the segment, so the file keeps its
// size and its other segments, or removes the file if none is left held.
// The caller drops the segment from the state, and saves it, first, so
// nothing serves the bytes once they are gone.
func (s *Store) FreeSegment(model, revision string, fm FileManifest, i int, lastHeld bool) error {
	path := s.FilePath(model, revision, fm.Path)
	if lastHeld {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	f, err := openRegular(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	start, end := fm.SegmentBounds(i)
	return punchHole(f, start, end-start)
}

// ParseBudget reads a disk budget: a byte count with an optional K, M, G
// or T suffix (powers of 1024, with or without a trailing B or iB), or a
// percentage of the size of the filesystem dir is on, such as "80%". An
// empty string or "0" is no budget, and returns 0.
func ParseBudget(s, dir string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	if pct, ok := strings.CutSuffix(s, "%"); ok {
		p, err := strconv.ParseFloat(pct, 64)
		if err != nil || p <= 0 || p > 100 {
			return 0, fmt.Errorf("disk budget %q: want a percentage above 0 and at most 100", s)
		}
		var st unix.Statfs_t
		if err := unix.Statfs(dir, &st); err != nil {
			return 0, fmt.Errorf("disk budget: size of the filesystem of %s: %w", dir, err)
		}
		total := float64(st.Blocks) * float64(st.Bsize)
		return int64(total * p / 100), nil
	}
	num := strings.TrimSuffix(strings.TrimSuffix(strings.ToUpper(s), "B"), "I")
	mult := int64(1)
	if n := len(num); n > 0 {
		if i := strings.IndexByte("KMGT", num[n-1]); i >= 0 {
			mult = int64(1) << (10 * (i + 1))
			num = num[:n-1]
		}
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
	if err != nil || v < 0 || v*float64(mult) >= math.MaxInt64 {
		return 0, fmt.Errorf("disk budget %q: want bytes (such as 500G) or a percentage (such as 80%%)", s)
	}
	return int64(v * float64(mult)), nil
}
