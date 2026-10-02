package catalog

import (
	"strings"
	"testing"
)

const validDoc = `
models:
  - name: nemotron-3-nano
    source:
      type: huggingface
      repo: nvidia/nemotron-3-nano-30b-a3b
    revision: abc1234
    size_hint: 19400000000
  - name: tiny-http-model
    source:
      type: http
      url: https://example.com/models/tiny
    revision: v1
`

func TestParseValid(t *testing.T) {
	c, err := Parse(strings.NewReader(validDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(c.Models) != 2 {
		t.Fatalf("got %d models, want 2", len(c.Models))
	}
	m, ok := c.Find("nemotron-3-nano")
	if !ok {
		t.Fatalf("Find: missing nemotron-3-nano")
	}
	if m.Source.Type != SourceHuggingFace || m.Source.Repo != "nvidia/nemotron-3-nano-30b-a3b" {
		t.Fatalf("unexpected source: %+v", m.Source)
	}
	if m.Revision != "abc1234" {
		t.Fatalf("unexpected revision: %q", m.Revision)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	doc := `
models:
  - name: x
    source:
      type: http
      url: https://example.com
    revision: v1
    typo_field: oops
`
	if _, err := Parse(strings.NewReader(doc)); err == nil {
		t.Fatalf("expected error for unknown field, got nil")
	}
}

func TestValidateRequiresRevision(t *testing.T) {
	doc := `
models:
  - name: x
    source:
      type: http
      url: https://example.com
`
	if _, err := Parse(strings.NewReader(doc)); err == nil {
		t.Fatalf("expected error for missing revision, got nil")
	}
}

func TestValidateRejectsDuplicateNames(t *testing.T) {
	doc := `
models:
  - name: dup
    source: {type: http, url: https://example.com/a}
    revision: v1
  - name: dup
    source: {type: http, url: https://example.com/b}
    revision: v2
`
	if _, err := Parse(strings.NewReader(doc)); err == nil {
		t.Fatalf("expected error for duplicate name, got nil")
	}
}

func TestValidateRequiresSourceFields(t *testing.T) {
	cases := []string{
		`models: [{name: x, source: {type: huggingface}, revision: v1}]`,
		`models: [{name: x, source: {type: http}, revision: v1}]`,
		`models: [{name: x, source: {type: bogus, url: https://example.com}, revision: v1}]`,
	}
	for _, doc := range cases {
		if _, err := Parse(strings.NewReader(doc)); err == nil {
			t.Errorf("expected error for doc %q, got nil", doc)
		}
	}
}

func TestSelectionMatches(t *testing.T) {
	cases := []struct {
		wanted []string
		name   string
		want   bool
	}{
		{[]string{"*"}, "anything", true},
		{[]string{"nemotron-*"}, "nemotron-3-nano", true},
		{[]string{"nemotron-*"}, "qwen-image", false},
		{[]string{"exact-name"}, "exact-name", true},
		{nil, "anything", false},
	}
	for _, tc := range cases {
		s := &Selection{Wanted: tc.wanted}
		if got := s.Matches(tc.name); got != tc.want {
			t.Errorf("Matches(%q) with wanted=%v = %v, want %v", tc.name, tc.wanted, got, tc.want)
		}
	}
}

func TestCatalogWanted(t *testing.T) {
	c, err := Parse(strings.NewReader(validDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel := &Selection{Wanted: []string{"nemotron-*"}}
	got := c.Wanted(sel)
	if len(got) != 1 || got[0].Name != "nemotron-3-nano" {
		t.Fatalf("Wanted = %+v, want just nemotron-3-nano", got)
	}
}
