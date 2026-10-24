package request

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
)

func testCatalog() *catalog.Catalog {
	return &catalog.Catalog{Models: []catalog.Model{
		{Name: "glm", Revision: "r1"},
		{Name: "qwen", Revision: "a"}, {Name: "qwen", Revision: "b"},
	}}
}

func TestResolve(t *testing.T) {
	r := Request{Name: "rank0", Pieces: catalog.Pieces{Model: "glm"}}
	if err := Resolve(testCatalog(), &r); err != nil || r.Revision != "r1" {
		t.Fatalf("Resolve = %v, revision %q; want r1 filled in", err, r.Revision)
	}
	for name, r := range map[string]Request{
		"bad name":      {Name: "../x", Pieces: catalog.Pieces{Model: "glm"}},
		"empty name":    {Pieces: catalog.Pieces{Model: "glm"}},
		"no model":      {Name: "a"},
		"unknown model": {Name: "a", Pieces: catalog.Pieces{Model: "nope"}},
		"ambiguous":     {Name: "a", Pieces: catalog.Pieces{Model: "qwen"}},
		"bad revision":  {Name: "a", Pieces: catalog.Pieces{Model: "qwen", Revision: "c"}},
		"bad range":     {Name: "a", Pieces: catalog.Pieces{Model: "glm", Ranges: []catalog.FileRanges{{Path: "w", Ranges: []catalog.ByteRange{{Offset: -1, Length: 1}}}}}},
		"bad glob":      {Name: "a", Pieces: catalog.Pieces{Model: "glm", Include: []string{"["}}},
	} {
		if err := Resolve(testCatalog(), &r); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Resolve = %v, want ErrInvalid", name, err)
		}
	}
}

func TestRegistryPersists(t *testing.T) {
	dir := t.TempDir()
	reg, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reg.Put(Request{Name: "b", Pieces: catalog.Pieces{Model: "glm", Revision: "r1"}, Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Put(Request{Name: "a", Pieces: catalog.Pieces{Model: "glm", Revision: "r1"}, Priority: 1}); err != nil {
		t.Fatal(err)
	}
	replaced, err := reg.Put(Request{Name: "b", Pieces: catalog.Pieces{Model: "glm", Revision: "r1", Include: []string{"*.json"}}, Priority: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !replaced.Created.Equal(first.Created) {
		t.Fatal("replacing a request changed its creation time")
	}
	os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o644)

	again, err := Open(dir)
	if err == nil {
		t.Fatal("Open = nil error with a broken file")
	}
	list := again.List()
	if len(list) != 2 || list[0].Name != "b" || list[1].Name != "a" {
		t.Fatalf("List = %+v, want b (priority 2) then a", list)
	}
	if got, _ := again.Get("b"); len(got.Include) != 1 {
		t.Fatalf("Get(b) = %+v, want the replacement", got)
	}
	if found, err := again.Delete("a"); !found || err != nil {
		t.Fatalf("Delete = %v, %v", found, err)
	}
	if found, _ := again.Delete("a"); found {
		t.Fatal("Delete found a released request")
	}
	if _, err := os.Stat(filepath.Join(dir, "a.json")); !os.IsNotExist(err) {
		t.Fatal("released request still on disk")
	}
}
