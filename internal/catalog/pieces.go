package catalog

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

// Pieces names part of one catalog model: some files, whole, and some byte
// ranges of other files. A node holds a partial selection as sparse files,
// with every range at its original offset.
//
// The files a Pieces names are every file Include matches and Exclude does
// not, plus every file in Ranges. With no Include and no Ranges, Include
// matches every file, so a Pieces with only Exclude names the whole model
// minus some files. A file in Ranges is never excluded, and a file that
// Include also matches is held whole.
type Pieces struct {
	// Model is a catalog model name.
	Model string `yaml:"model" json:"model"`
	// Revision is one of Model's catalog revisions. Empty means every
	// revision of Model in the catalog.
	Revision string `yaml:"revision,omitempty" json:"revision,omitempty"`
	// Include and Exclude are globs over manifest paths. "*" matches
	// within one path segment and "**" matches any number of segments.
	Include []string `yaml:"include,omitempty" json:"include,omitempty"`
	Exclude []string `yaml:"exclude,omitempty" json:"exclude,omitempty"`
	// Ranges names byte ranges of single files.
	Ranges []FileRanges `yaml:"ranges,omitempty" json:"ranges,omitempty"`
}

// FileRanges names byte ranges of one file.
type FileRanges struct {
	Path   string      `yaml:"path" json:"path"`
	Ranges []ByteRange `yaml:"ranges" json:"ranges"`
}

// ByteRange is Length bytes of a file starting at Offset.
type ByteRange struct {
	Offset int64 `yaml:"offset" json:"offset"`
	Length int64 `yaml:"length" json:"length"`
}

// AllFiles reports whether p names every file of its model, before Exclude.
func (p Pieces) AllFiles() bool {
	return len(p.Include) == 0 && len(p.Ranges) == 0
}

// Validate checks that p is well formed. It does not check p against a
// catalog or a manifest.
func (p Pieces) Validate() error {
	if p.Model == "" {
		return errors.New("model is required")
	}
	for _, g := range append(append([]string(nil), p.Include...), p.Exclude...) {
		if err := ValidateGlob(g); err != nil {
			return err
		}
	}
	for _, fr := range p.Ranges {
		if fr.Path == "" {
			return errors.New("ranges: path is required")
		}
		if len(fr.Ranges) == 0 {
			return fmt.Errorf("ranges: %s: no ranges", fr.Path)
		}
		for _, r := range fr.Ranges {
			if r.Offset < 0 || r.Length <= 0 || r.Offset+r.Length < r.Offset {
				return fmt.Errorf("ranges: %s: invalid range offset %d length %d", fr.Path, r.Offset, r.Length)
			}
		}
	}
	return nil
}

// Names reports whether p applies to catalog model m.
func (p Pieces) Names(m Model) bool {
	return p.Model == m.Name && (p.Revision == "" || p.Revision == m.Revision)
}

// ValidateGlob checks that g is a valid path glob for MatchPath.
func ValidateGlob(g string) error {
	if g == "" {
		return errors.New("empty glob")
	}
	for _, seg := range strings.Split(g, "/") {
		if seg == "**" {
			continue
		}
		if _, err := path.Match(seg, ""); err != nil {
			return fmt.Errorf("glob %q: %w", g, err)
		}
	}
	return nil
}

// MatchPath reports whether the slash-separated path p matches glob g. Each
// segment of g matches one segment of p with path.Match semantics, except
// "**", which matches zero or more whole segments. An invalid glob matches
// nothing.
func MatchPath(g, p string) bool {
	return matchSegments(strings.Split(g, "/"), strings.Split(p, "/"))
}

func matchSegments(g, p []string) bool {
	for len(g) > 0 {
		if g[0] == "**" {
			for i := 0; i <= len(p); i++ {
				if matchSegments(g[1:], p[i:]) {
					return true
				}
			}
			return false
		}
		if len(p) == 0 {
			return false
		}
		if ok, err := path.Match(g[0], p[0]); !ok || err != nil {
			return false
		}
		g, p = g[1:], p[1:]
	}
	return len(p) == 0
}
