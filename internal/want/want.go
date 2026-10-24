// Package want turns what a node is asked to hold (its selection file, and
// later its consumers' requests) into the segments of each file it must
// fetch and keep. A whole model needs every segment of every file; a
// partial selection needs whole files and byte ranges rounded out to whole
// segments, so the manifest's segment hashes still verify every byte held.
package want

import (
	"fmt"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/store"
)

// Want is one catalog model revision a node must hold, whole or in part.
type Want struct {
	Model catalog.Model
	// Whole means every file of the revision.
	Whole bool
	// Pieces, when not Whole, are the parts of the revision wanted.
	Pieces []catalog.Pieces
}

// Key identifies a model revision, as name@revision.
func Key(name, revision string) string { return name + "@" + revision }

// FromSelection returns the wants s names, in catalog order. A model that
// s.Wanted matches is whole, whatever s.Partial says about it. It returns an
// error for a partial entry that names no catalog model, alongside the
// wants for every other entry.
func FromSelection(c *catalog.Catalog, s *catalog.Selection) ([]Want, error) {
	var out []Want
	named := make([]bool, len(s.Partial))
	for _, m := range c.Models {
		w := Want{Model: m, Whole: s.Matches(m.Name)}
		for i, p := range s.Partial {
			if p.Names(m) {
				named[i] = true
				if !w.Whole {
					w.Pieces = append(w.Pieces, p)
				}
			}
		}
		if w.Whole || len(w.Pieces) > 0 {
			out = append(out, w)
		}
	}
	for i, ok := range named {
		if !ok {
			p := s.Partial[i]
			return out, fmt.Errorf("selection: partial entry %d: no catalog model %s", i, Key(p.Model, p.Revision))
		}
	}
	return out, nil
}

// Need is the part of one revision a Want resolves to, against its manifest.
type Need struct {
	// Whole means the Want named the whole revision.
	Whole bool
	// Files maps a manifest path to what is needed of that file. A file
	// with no entry is not needed.
	Files map[string]*FileNeed
}

// FileNeed is what is needed of one file.
type FileNeed struct {
	// Whole means the whole file, checked against its whole-file sha256.
	Whole bool
	// Segments[i] is true when segment i is needed. Every segment is
	// needed when Whole.
	Segments []bool
}

// Resolve works out which segments of which files w needs, from manifest
// m. It fails for a range that names a file m does not have, or that runs
// past the end of its file.
func Resolve(m *store.Manifest, w Want) (*Need, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	n := &Need{Whole: w.Whole, Files: make(map[string]*FileNeed)}
	for _, fm := range m.Files {
		if w.Whole || wholeFile(w.Pieces, fm.Path) {
			n.Files[fm.Path] = wholeNeed(fm)
		}
	}
	if w.Whole {
		return n, nil
	}
	for _, p := range w.Pieces {
		for _, fr := range p.Ranges {
			fm, ok := m.Find(fr.Path)
			if !ok {
				return nil, fmt.Errorf("%s: no file %s", Key(m.Model, m.Revision), fr.Path)
			}
			fn := n.Files[fm.Path]
			if fn == nil {
				fn = &FileNeed{Segments: make([]bool, fm.NumSegments())}
				n.Files[fm.Path] = fn
			}
			for _, r := range fr.Ranges {
				if r.Offset+r.Length > fm.Size {
					return nil, fmt.Errorf("%s: %s: range offset %d length %d runs past the file's %d bytes",
						Key(m.Model, m.Revision), fm.Path, r.Offset, r.Length, fm.Size)
				}
				first, last := fm.SegmentIndex(r.Offset), fm.SegmentIndex(r.Offset+r.Length-1)
				if first < 0 || last >= len(fn.Segments) {
					return nil, fmt.Errorf("%s: %s: range offset %d length %d is outside the file's segments", Key(m.Model, m.Revision), fm.Path, r.Offset, r.Length)
				}
				for i := first; i <= last; i++ {
					fn.Segments[i] = true
				}
			}
		}
	}
	return n, nil
}

func wholeNeed(fm store.FileManifest) *FileNeed {
	segs := make([]bool, fm.NumSegments())
	for i := range segs {
		segs[i] = true
	}
	return &FileNeed{Whole: true, Segments: segs}
}

// wholeFile reports whether any of ps names the file at path whole.
func wholeFile(ps []catalog.Pieces, path string) bool {
	for _, p := range ps {
		if matchesAny(p.Exclude, path) {
			continue
		}
		if p.AllFiles() || matchesAny(p.Include, path) {
			return true
		}
	}
	return false
}

func matchesAny(globs []string, path string) bool {
	for _, g := range globs {
		if catalog.MatchPath(g, path) {
			return true
		}
	}
	return false
}

// Bytes is the size of every needed segment.
func (n *Need) Bytes(m *store.Manifest) int64 {
	var total int64
	for _, fm := range m.Files {
		if fn := n.Files[fm.Path]; fn != nil {
			for i, need := range fn.Segments {
				if need {
					start, end := fm.SegmentBounds(i)
					total += end - start
				}
			}
		}
	}
	return total
}

// BytesDone is the size of every needed segment rs holds.
func (n *Need) BytesDone(m *store.Manifest, rs *store.RevisionState) int64 {
	var total int64
	for _, fm := range m.Files {
		fn := n.Files[fm.Path]
		if fn == nil {
			continue
		}
		fs := rs.FileState(fm)
		for i, need := range fn.Segments {
			if need && i < len(fs.SegmentsDone) && fs.SegmentsDone[i] {
				start, end := fm.SegmentBounds(i)
				total += end - start
			}
		}
	}
	return total
}

// Satisfied reports whether rs holds everything n needs: every needed
// segment verified, and every whole file checked against its sha256.
func (n *Need) Satisfied(m *store.Manifest, rs *store.RevisionState) bool {
	for _, fm := range m.Files {
		fn := n.Files[fm.Path]
		if fn == nil {
			continue
		}
		fs := rs.FileState(fm)
		if fs.Complete {
			continue
		}
		if fn.Whole {
			return false
		}
		all := len(fs.SegmentsDone) == fm.NumSegments()
		for i := range fn.Segments {
			held := i < len(fs.SegmentsDone) && fs.SegmentsDone[i]
			if fn.Segments[i] && !held {
				return false
			}
			all = all && held
		}
		if all {
			// Every segment is held, so the file must pass its whole-file
			// check before it counts; a failed or unfinished check is not
			// hidden behind the segments that are held.
			return false
		}
	}
	return true
}
