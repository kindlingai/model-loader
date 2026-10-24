package store

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseBudget(t *testing.T) {
	dir := t.TempDir()
	for in, want := range map[string]int64{
		"": 0, "0": 0, "1000": 1000, "500G": 500 << 30, "2T": 2 << 40, "1.5GiB": 3 << 29, "64m": 64 << 20, "10KB": 10 << 10,
	} {
		got, err := ParseBudget(in, dir)
		if err != nil || got != want {
			t.Errorf("ParseBudget(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if got, err := ParseBudget("50%", dir); err != nil || got <= 0 {
		t.Errorf("ParseBudget(50%%) = %d, %v", got, err)
	}
	for _, bad := range []string{"-1", "x", "0%", "101%", "5Q", "%"} {
		if _, err := ParseBudget(bad, dir); err == nil {
			t.Errorf("ParseBudget(%q) = nil error", bad)
		}
	}
}

// Evicting a segment forgets only the checks over it, and frees its
// blocks while the rest of the file reads the same.
func TestEvictSegment(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 64 << 10
	fm := FileManifest{Path: "f", Size: 3 * seg, SegmentSize: seg}
	m := &Manifest{Model: "m", Revision: "refs/pr/1", Files: []FileManifest{fm}}
	rs := NewRevisionState(m)
	for i := range 3 {
		rs.MarkSegment(fm, i, "", TrustManifest)
	}
	rs.AddChecked(fm, RangeCheck{Offset: 0, Length: 5})
	rs.AddChecked(fm, RangeCheck{Offset: seg + 5, Length: 10})
	rs.EvictSegment(fm, 0)
	fs := rs.FileState(fm)
	if fs.SegmentsDone[0] || !fs.SegmentsDone[1] || fs.HasChecked(RangeCheck{Offset: 0, Length: 5}) || !fs.HasChecked(RangeCheck{Offset: seg + 5, Length: 10}) {
		t.Fatalf("state after evicting segment 0 = %+v", fs)
	}
	if fs.LastUsed(1) == 0 {
		t.Fatal("a fetched segment has no use time")
	}

	f, err := st.EnsureFile(m.Model, m.Revision, fm)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3*seg)
	for i := range data {
		data[i] = byte(i%251) + 1
	}
	f.WriteAt(data, 0)
	f.Sync()
	f.Close()
	before := st.FileAllocated(m.Model, m.Revision, "f")
	err = st.FreeSegment(m.Model, m.Revision, fm, 0, false)
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		if err == nil {
			t.Fatal("FreeSegment says it freed space it cannot free here")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(st.FilePath(m.Model, m.Revision, "f"))
	if len(got) != 3*seg || !bytes.Equal(got[seg:], data[seg:]) || !bytes.Equal(got[:seg], make([]byte, seg)) {
		t.Fatal("after freeing segment 0 the file does not read zeros, then the rest")
	}
	if after := st.FileAllocated(m.Model, m.Revision, "f"); after > before-seg {
		t.Fatalf("allocated %d bytes before freeing a %d byte segment, %d after", before, seg, after)
	}
	if err := st.FreeSegment(m.Model, m.Revision, fm, 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.FilePath(m.Model, m.Revision, "f")); !os.IsNotExist(err) {
		t.Fatal("file with nothing held left on disk")
	}
	if n, err := st.Allocated(); err != nil || n != 0 {
		t.Fatalf("Allocated = %d, %v; want 0", n, err)
	}
	refs, err := st.Revisions()
	if err != nil || len(refs) != 0 {
		t.Fatalf("Revisions = %v, %v; want none (no manifest or state saved)", refs, err)
	}
	st.SaveManifest(m)
	st.SaveState(rs)
	// A revision with a slash in it is named as its manifest names it.
	if refs, _ := st.Revisions(); len(refs) != 1 || refs[0] != (RevisionRef{"m", "refs/pr/1"}) {
		t.Fatalf("Revisions = %v", refs)
	}
	// Removing revision "refs" leaves "refs/pr/1", under its directory.
	outer := &Manifest{Model: "m", Revision: "refs", Files: []FileManifest{{Path: "o", Size: 1, SegmentSize: seg}}}
	st.SaveManifest(outer)
	of, _ := st.EnsureFile("m", "refs", outer.Files[0])
	of.Close()
	inner, _ := st.EnsureFile(m.Model, m.Revision, fm)
	inner.Close()
	if err := st.RemoveRevision("m", "refs", []string{"o"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.FilePath(m.Model, m.Revision, "f")); err != nil {
		t.Fatalf("removing m@refs removed m@refs/pr/1: %v", err)
	}
	if refs, _ := st.Revisions(); len(refs) != 1 || refs[0].Revision != "refs/pr/1" {
		t.Fatalf("Revisions = %v, want only refs/pr/1", refs)
	}
	if err := st.RemoveRevision(m.Model, m.Revision, []string{"f"}); err != nil {
		t.Fatal(err)
	}
	if refs, _ := st.Revisions(); len(refs) != 0 {
		t.Fatalf("Revisions after remove = %v", refs)
	}
	if entries, _ := os.ReadDir(filepath.Join(st.root, "models")); len(entries) != 0 {
		t.Fatalf("directories left under models: %v", entries)
	}
}
