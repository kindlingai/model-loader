// Package catalog parses the fleet-wide desired-state file: every model the
// fleet might want, pinned to a source and revision. model-loader does not
// sync this file itself — it is handed to each node by whatever deploys it
// (a ConfigMap, a bind mount, a file dropped by a site layer).
package catalog

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

// SourceType identifies which source plugin resolves and fetches a model.
type SourceType string

const (
	SourceHTTP        SourceType = "http"
	SourceHuggingFace SourceType = "huggingface"
)

// Source describes where a model's files originate.
type Source struct {
	Type SourceType `yaml:"type"`
	// Repo is the Hugging Face repo id ("org/name"), used when Type is
	// SourceHuggingFace.
	Repo string `yaml:"repo,omitempty"`
	// URL is the base URL of the file tree, used when Type is SourceHTTP.
	URL string `yaml:"url,omitempty"`
}

// Model is one catalog entry: a named model pinned to a specific revision of
// a source.
type Model struct {
	// Name is a stable identifier for the model, independent of the
	// source's own naming (e.g. "nemotron-3-nano"). It is what nodes
	// reference in their local Selection and what the server's endpoints
	// key on.
	Name     string `yaml:"name"`
	Source   Source `yaml:"source"`
	Revision string `yaml:"revision"`
	// SizeHint is an advisory total size in bytes, used for disk planning
	// and progress reporting before a manifest has been fetched. It is
	// not authoritative; the manifest's per-file sizes are.
	SizeHint int64  `yaml:"size_hint,omitempty"`
	Notes    string `yaml:"notes,omitempty"`
}

// Catalog is the full fleet-wide desired state.
type Catalog struct {
	Models []Model `yaml:"models"`
}

// Parse reads a catalog.yaml document.
func Parse(r io.Reader) (*Catalog, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var c Catalog
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse catalog: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Load reads and parses a catalog.yaml file from disk.
func Load(path string) (*Catalog, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("load catalog: %w", err)
	}
	defer f.Close()
	return Parse(f)
}

// Validate checks that every entry is well formed and each (name, revision)
// pair appears once. A catalog is desired state shared across the whole
// fleet, so a mistake here should be caught before it reaches any node, not
// discovered mid-download. One name may be pinned at several revisions;
// every node keys models by name@revision.
func (c *Catalog) Validate() error {
	seen := make(map[string]bool, len(c.Models))
	for i, m := range c.Models {
		if m.Name == "" {
			return fmt.Errorf("catalog: entry %d: name is required", i)
		}
		if strings.Contains(m.Name, "/") {
			return fmt.Errorf("catalog: model %q: name must not contain '/' (it is a URL path segment)", m.Name)
		}
		key := m.Name + "@" + m.Revision
		if seen[key] {
			return fmt.Errorf("catalog: duplicate model %s", key)
		}
		seen[key] = true

		if m.Revision == "" {
			return fmt.Errorf("catalog: model %q: revision is required (pin it)", m.Name)
		}

		switch m.Source.Type {
		case SourceHuggingFace:
			if m.Source.Repo == "" {
				return fmt.Errorf("catalog: model %q: huggingface source requires repo", m.Name)
			}
		case SourceHTTP:
			if m.Source.URL == "" {
				return fmt.Errorf("catalog: model %q: http source requires url", m.Name)
			}
		case "":
			return fmt.Errorf("catalog: model %q: source.type is required", m.Name)
		default:
			return fmt.Errorf("catalog: model %q: unknown source type %q", m.Name, m.Source.Type)
		}
	}
	return nil
}

// Selection is a node's local choice of which catalog entries to actually
// materialize. The catalog is fleet-wide; disk is not, so a node opts in to
// a subset (or "*" for all of it).
type Selection struct {
	Wanted []string `yaml:"wanted"`
}

// LoadSelection reads a node's local selection file.
func LoadSelection(path string) (*Selection, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("load selection: %w", err)
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	var s Selection
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse selection: %w", err)
	}
	return &s, nil
}

// Matches reports whether name is selected, supporting shell-style globs
// (path.Match semantics) and "*" meaning everything.
func (s *Selection) Matches(name string) bool {
	for _, pattern := range s.Wanted {
		if pattern == "*" {
			return true
		}
		if ok, err := path.Match(pattern, name); ok && err == nil {
			return true
		}
	}
	return false
}

// Wanted returns the subset of the catalog this selection matches.
func (c *Catalog) Wanted(s *Selection) []Model {
	var out []Model
	for _, m := range c.Models {
		if s.Matches(m.Name) {
			out = append(out, m)
		}
	}
	return out
}
