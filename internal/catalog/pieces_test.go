package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMatchPath(t *testing.T) {
	for _, tc := range []struct {
		glob, path string
		want       bool
	}{
		{"*.json", "config.json", true},
		{"*.json", "dense/config.json", false},
		{"dense/*", "dense/a.safetensors", true},
		{"dense/*", "dense/sub/a.safetensors", false},
		{"dense/vision-*", "dense/vision-000.safetensors", true},
		{"**/*.json", "config.json", true},
		{"**/*.json", "a/b/c.json", true},
		{"experts/**", "experts/exlr8-layer-003.k4.safetensors", true},
		{"experts/**", "experts", true},
		{"experts/**", "dense/x", false},
		{"a/**/z", "a/z", true},
		{"a/**/z", "a/b/c/z", true},
		{"a/**/z", "a/b/c/y", false},
		{"[", "[", false},
	} {
		if got := MatchPath(tc.glob, tc.path); got != tc.want {
			t.Errorf("MatchPath(%q, %q) = %v, want %v", tc.glob, tc.path, got, tc.want)
		}
	}
}

func TestLoadSelectionPartial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "selection.yaml")
	body := `
wanted: [small]
partial:
  - model: glm
    revision: r1
    include: ["*.json", "dense/**"]
    exclude: ["dense/vision-*"]
    ranges:
      - path: experts/exlr8-layer-003.k4.safetensors
        ranges:
          - {offset: 0, length: 4096}
          - {offset: 1048576, length: 65536}
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := LoadSelection(path)
	if err != nil {
		t.Fatalf("LoadSelection: %v", err)
	}
	if len(s.Partial) != 1 {
		t.Fatalf("Partial = %+v, want one entry", s.Partial)
	}
	p := s.Partial[0]
	if p.Model != "glm" || p.Revision != "r1" || len(p.Include) != 2 || len(p.Exclude) != 1 {
		t.Fatalf("partial entry = %+v", p)
	}
	if len(p.Ranges) != 1 || len(p.Ranges[0].Ranges) != 2 || p.Ranges[0].Ranges[1] != (ByteRange{Offset: 1048576, Length: 65536}) {
		t.Fatalf("ranges = %+v", p.Ranges)
	}
	if !s.Matches("small") || s.Matches("glm") {
		t.Fatal("wanted globs must still select whole models, and only those")
	}
}

func TestLoadSelectionRejectsBadPartial(t *testing.T) {
	for name, body := range map[string]string{
		"no model":      "partial:\n  - include: ['*']\n",
		"bad glob":      "partial:\n  - model: m\n    include: ['[']\n",
		"zero length":   "partial:\n  - model: m\n    ranges: [{path: a, ranges: [{offset: 0, length: 0}]}]\n",
		"negative":      "partial:\n  - model: m\n    ranges: [{path: a, ranges: [{offset: -1, length: 4}]}]\n",
		"no path":       "partial:\n  - model: m\n    ranges: [{ranges: [{offset: 0, length: 4}]}]\n",
		"no ranges":     "partial:\n  - model: m\n    ranges: [{path: a}]\n",
		"unknown field": "partial:\n  - model: m\n    files: [a]\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "selection.yaml")
			if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadSelection(path); err == nil {
				t.Fatal("LoadSelection: want an error")
			} else if !strings.Contains(err.Error(), "selection") {
				t.Fatalf("error %q does not mention the selection", err)
			}
		})
	}
}
