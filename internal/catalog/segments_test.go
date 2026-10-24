package catalog

import (
	"strings"
	"testing"
)

func TestSegmentSizes(t *testing.T) {
	c, err := Parse(strings.NewReader(`
models:
  - name: glm
    revision: main
    source: {type: huggingface, repo: kindlingai/glm}
    segment_sizes:
      - {path: "experts/*.k?.safetensors", size: 65536}
      - {path: "experts/**", size: 1048576}
`))
	if err != nil {
		t.Fatal(err)
	}
	m := c.Models[0]
	for path, want := range map[string]int64{
		"experts/exlr8-layer-003.k4.safetensors":    65536,
		"experts/exlr8-layer-003.index.safetensors": 1 << 20,
		"dense/a.safetensors":                       0,
	} {
		if got := m.SegmentSizeFor(path); got != want {
			t.Errorf("%s: %d, want %d", path, got, want)
		}
	}
	for name, rule := range map[string]string{
		"not a block multiple": `{path: "a", size: 5000}`,
		"too small":            `{path: "a", size: 0}`,
		"too large":            `{path: "a", size: 134217728}`,
		"bad glob":             `{path: "[", size: 65536}`,
	} {
		_, err := Parse(strings.NewReader(`
models:
  - name: glm
    revision: main
    source: {type: huggingface, repo: kindlingai/glm}
    segment_sizes: [` + rule + `]
`))
		if err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}
