package want

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/store"
)

func testManifest() *store.Manifest {
	return &store.Manifest{Model: "glm", Revision: "r1", Files: []store.FileManifest{
		{Path: "config.json", Size: 10, SegmentSize: 100, SegmentSHA256: []string{"c"}},
		{Path: "dense/a.safetensors", Size: 250, SegmentSize: 100, SegmentSHA256: []string{"a0", "a1", "a2"}},
		{Path: "dense/vision-0.safetensors", Size: 150, SegmentSize: 100, SegmentSHA256: []string{"v0", "v1"}},
		{Path: "experts/e.safetensors", Size: 500, SegmentSize: 100, SegmentSHA256: []string{"e0", "e1", "e2", "e3", "e4"}},
		{Path: "empty.txt", Size: 0, SegmentSize: 100},
	}}
}

func segments(n *Need, path string) []bool {
	if fn := n.Files[path]; fn != nil {
		return fn.Segments
	}
	return nil
}

func TestResolveWhole(t *testing.T) {
	m := testManifest()
	n, err := Resolve(m, Want{Whole: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(n.Files) != len(m.Files) || n.Bytes(m) != m.TotalSize() {
		t.Fatalf("whole need = %d files, %d bytes; want every file", len(n.Files), n.Bytes(m))
	}
}

func TestResolveGlobsAndRanges(t *testing.T) {
	m := testManifest()
	w := Want{Pieces: []catalog.Pieces{{
		Model:   "glm",
		Include: []string{"*.json", "dense/**"},
		Exclude: []string{"dense/vision-*"},
		Ranges: []catalog.FileRanges{{Path: "experts/e.safetensors", Ranges: []catalog.ByteRange{
			{Offset: 10, Length: 5},   // inside segment 0
			{Offset: 290, Length: 20}, // straddles segments 2 and 3
		}}},
	}}}
	n, err := Resolve(m, w)
	if err != nil {
		t.Fatal(err)
	}
	if fn := n.Files["config.json"]; fn == nil || !fn.Whole {
		t.Fatalf("config.json: %+v, want whole", fn)
	}
	if fn := n.Files["dense/a.safetensors"]; fn == nil || !fn.Whole {
		t.Fatalf("dense/a.safetensors: %+v, want whole", fn)
	}
	if fn := n.Files["dense/vision-0.safetensors"]; fn != nil {
		t.Fatalf("dense/vision-0.safetensors: %+v, want excluded", fn)
	}
	if _, ok := n.Files["empty.txt"]; ok {
		t.Fatal("empty.txt: matched no include glob, want not needed")
	}
	if got, want := segments(n, "experts/e.safetensors"), []bool{true, false, true, true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("experts segments = %v, want %v (ranges round out to whole segments)", got, want)
	}
	if n.Files["experts/e.safetensors"].Whole {
		t.Fatal("experts/e.safetensors: want partial, not whole")
	}
	if got := n.Bytes(m); got != 10+250+300 {
		t.Fatalf("Bytes = %d, want %d", got, 10+250+300)
	}
}

// With no include and no ranges, a partial entry names every file but its
// excludes; a ranged file is never excluded.
func TestResolveExcludeOnly(t *testing.T) {
	m := testManifest()
	n, err := Resolve(m, Want{Pieces: []catalog.Pieces{{Model: "glm", Exclude: []string{"dense/vision-*", "experts/**"}}}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, fm := range m.Files {
		if n.Files[fm.Path] != nil {
			got = append(got, fm.Path)
		}
	}
	if want := []string{"config.json", "dense/a.safetensors", "empty.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}

	n, err = Resolve(m, Want{Pieces: []catalog.Pieces{{
		Model: "glm", Include: []string{"*.json"}, Exclude: []string{"experts/**"},
		Ranges: []catalog.FileRanges{{Path: "experts/e.safetensors", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1}}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if n.Files["experts/e.safetensors"] == nil {
		t.Fatal("a ranged file must not be excluded")
	}
}

func TestResolveRejectsBadRanges(t *testing.T) {
	m := testManifest()
	for name, fr := range map[string]catalog.FileRanges{
		"missing file":  {Path: "nope", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1}}},
		"past the end":  {Path: "config.json", Ranges: []catalog.ByteRange{{Offset: 5, Length: 6}}},
		"zero-len file": {Path: "empty.txt", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Resolve(m, Want{Pieces: []catalog.Pieces{{Model: "glm", Ranges: []catalog.FileRanges{fr}}}})
			if err == nil {
				t.Fatal("Resolve: want an error")
			}
		})
	}
}

func TestNeedSatisfied(t *testing.T) {
	m := testManifest()
	w := Want{Pieces: []catalog.Pieces{{Model: "glm", Include: []string{"config.json"},
		Ranges: []catalog.FileRanges{{Path: "experts/e.safetensors", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1}, {Offset: 450, Length: 1}}}}}}}
	n, err := Resolve(m, w)
	if err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)
	if n.Satisfied(m, rs) {
		t.Fatal("Satisfied with nothing held")
	}
	e := rs.FileState(m.Files[3])
	e.SegmentsDone[0], e.SegmentsDone[4] = true, true
	rs.SetFileState(e)
	if n.Satisfied(m, rs) {
		t.Fatal("Satisfied without the whole file config.json")
	}
	c := rs.FileState(m.Files[0])
	c.SegmentsDone[0] = true
	rs.SetFileState(c)
	if n.Satisfied(m, rs) {
		t.Fatal("a whole file needs its whole-file check, not just its segments")
	}
	c.Complete = true
	rs.SetFileState(c)
	if !n.Satisfied(m, rs) {
		t.Fatal("not Satisfied with every needed piece held")
	}
	if got, want := n.BytesDone(m, rs), int64(10+100+100); got != want {
		t.Fatalf("BytesDone = %d, want %d", got, want)
	}
}

func TestFromSelection(t *testing.T) {
	c := &catalog.Catalog{Models: []catalog.Model{
		{Name: "glm", Revision: "r1"}, {Name: "glm", Revision: "r2"}, {Name: "small", Revision: "r1"},
	}}
	s := &catalog.Selection{
		Wanted: []string{"small"},
		Partial: []catalog.Pieces{
			{Model: "glm", Revision: "r2", Include: []string{"*.json"}},
			{Model: "small", Include: []string{"*.json"}},
		},
	}
	ws, err := FromSelection(c, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(ws) != 2 || ws[0].Model.Revision != "r2" || ws[0].Whole || len(ws[0].Pieces) != 1 || !ws[1].Whole || ws[1].Pieces != nil {
		t.Fatalf("wants = %+v", ws)
	}

	s.Partial = append(s.Partial, catalog.Pieces{Model: "glm", Revision: "r9"})
	ws, err = FromSelection(c, s)
	if err == nil || !strings.Contains(err.Error(), "glm@r9") {
		t.Fatalf("err = %v, want one naming glm@r9", err)
	}
	if len(ws) != 2 {
		t.Fatalf("an unknown entry must not drop the others: %+v", ws)
	}
}

// A ranged file with every segment held but its whole-file check not
// passed is not satisfied, so a failed check is not hidden on the next pass.
func TestNeedSatisfiedRangedFileAllHeld(t *testing.T) {
	m := testManifest()
	n, err := Resolve(m, Want{Pieces: []catalog.Pieces{{Model: "glm",
		Ranges: []catalog.FileRanges{{Path: "dense/vision-0.safetensors", Ranges: []catalog.ByteRange{{Offset: 0, Length: 1}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	rs := store.NewRevisionState(m)
	fs := rs.FileState(m.Files[2])
	fs.SegmentsDone[0] = true
	rs.SetFileState(fs)
	if !n.Satisfied(m, rs) {
		t.Fatal("not Satisfied with the one needed segment held")
	}
	fs.SegmentsDone[1] = true
	rs.SetFileState(fs)
	if n.Satisfied(m, rs) {
		t.Fatal("Satisfied with every segment held but no whole-file check")
	}
	fs.Complete = true
	rs.SetFileState(fs)
	if !n.Satisfied(m, rs) {
		t.Fatal("not Satisfied once complete")
	}
}
