package store

import (
	"os"
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

// Evicting a segment forgets only the checks over it, and frees it while
// the rest of the file reads the same.
func TestEvictSegment(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "f", Size: 30, SegmentSize: 10}
	m := &Manifest{Model: "m", Revision: "r", Files: []FileManifest{fm}}
	rs := NewRevisionState(m)
	for i := range 3 {
		rs.MarkSegment(fm, i, "", TrustManifest)
	}
	rs.AddChecked(fm, RangeCheck{Offset: 0, Length: 5})
	rs.AddChecked(fm, RangeCheck{Offset: 15, Length: 10})
	rs.EvictSegment(fm, 0)
	fs := rs.FileState(fm)
	if fs.SegmentsDone[0] || !fs.SegmentsDone[1] || fs.HasChecked(RangeCheck{Offset: 0, Length: 5}) || !fs.HasChecked(RangeCheck{Offset: 15, Length: 10}) {
		t.Fatalf("state after evicting segment 0 = %+v", fs)
	}
	if fs.LastUsed(1) == 0 {
		t.Fatal("a fetched segment has no use time")
	}

	f, err := st.EnsureFile("m", "r", fm)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("aaaaaaaaaabbbbbbbbbbcccccccccc")
	f.WriteAt(data, 0)
	f.Close()
	if err := st.FreeSegment("m", "r", fm, 0, false); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(st.FilePath("m", "r", "f"))
	if len(got) != 30 || string(got[10:]) != string(data[10:]) || string(got[:10]) != string(make([]byte, 10)) {
		t.Fatalf("after freeing segment 0: %q", got)
	}
	if err := st.FreeSegment("m", "r", fm, 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.FilePath("m", "r", "f")); !os.IsNotExist(err) {
		t.Fatal("file with nothing held left on disk")
	}
	refs, err := st.Revisions()
	if err != nil || len(refs) != 0 {
		t.Fatalf("Revisions = %v, %v; want none (no manifest or state saved)", refs, err)
	}
	st.SaveManifest(m)
	st.SaveState(rs)
	if refs, _ := st.Revisions(); len(refs) != 1 || refs[0] != (RevisionRef{"m", "r"}) {
		t.Fatalf("Revisions = %v", refs)
	}
	if err := st.RemoveRevision("m", "r"); err != nil {
		t.Fatal(err)
	}
	if refs, _ := st.Revisions(); len(refs) != 0 {
		t.Fatalf("Revisions after remove = %v", refs)
	}
}
