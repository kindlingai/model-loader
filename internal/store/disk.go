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
// for. Each is named as its manifest or state file names it, not by its
// path: a revision may itself contain a slash.
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
			var id struct {
				Model    string `json:"model"`
				Revision string `json:"revision"`
			}
			if err := unmarshalJSONFile(path, &id); err != nil || id.Model == "" || id.Revision == "" {
				return nil // not ours, or half written: leave it be.
			}
			ref := RevisionRef{id.Model, id.Revision}
			if !seen[ref] {
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

// Allocated returns the disk space the store's model files take: the
// blocks the filesystem has allocated to them, so sparse files count only
// what is written, and bytes written but never verified (a failed
// transfer) count too.
func (s *Store) Allocated() (int64, error) {
	var total int64
	err := filepath.WalkDir(filepath.Join(s.root, "models"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.Type().IsRegular() {
			total += allocatedPath(path)
		}
		return nil
	})
	return total, err
}

// FileAllocated returns the disk space one file of a revision takes.
func (s *Store) FileAllocated(model, revision, relPath string) int64 {
	return allocatedPath(s.FilePath(model, revision, relPath))
}

func allocatedPath(path string) int64 {
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return 0
	}
	return int64(st.Blocks) * 512
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
// nothing serves the bytes once they are gone, and makes sure nothing is
// writing the file.
func (s *Store) FreeSegment(model, revision string, fm FileManifest, i int, lastHeld bool) error {
	path := s.FilePath(model, revision, fm.Path)
	if lastHeld {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	f, err := openRegularRW(path)
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

// openRegularRW opens path for writing without following a symlink, and
// only if it is a regular file.
func openRegularRW(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return f, nil
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
