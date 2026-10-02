package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestBuildSegmentHashesAndVerify(t *testing.T) {
	dir := t.TempDir()
	content := bytes.Repeat([]byte("a"), int(DefaultSegmentSize)+100) // spans two segments
	if err := os.WriteFile(filepath.Join(dir, "weights.bin"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	m := &Manifest{
		Model:    "m",
		Revision: "r",
		Files: []FileManifest{
			{Path: "weights.bin", Size: int64(len(content))},
		},
	}
	err := m.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(dir, relPath))
	})
	if err != nil {
		t.Fatalf("BuildSegmentHashes: %v", err)
	}
	fm := m.Files[0]
	if fm.NumSegments() != 2 {
		t.Fatalf("NumSegments = %d, want 2", fm.NumSegments())
	}
	if len(fm.SegmentSHA256) != 2 {
		t.Fatalf("len(SegmentSHA256) = %d, want 2", len(fm.SegmentSHA256))
	}

	f, err := os.Open(filepath.Join(dir, "weights.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for i := 0; i < fm.NumSegments(); i++ {
		ok, err := VerifySegment(f, fm, i)
		if err != nil {
			t.Fatalf("VerifySegment(%d): %v", i, err)
		}
		if !ok {
			t.Fatalf("VerifySegment(%d) = false, want true", i)
		}
	}
	ok, err := VerifyWholeFile(f, fm)
	if err != nil {
		t.Fatalf("VerifyWholeFile: %v", err)
	}
	if !ok {
		t.Fatalf("VerifyWholeFile = false, want true")
	}
}

func TestVerifySegmentDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	content := bytes.Repeat([]byte("b"), 1024)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Model: "m", Revision: "r", Files: []FileManifest{{Path: "f.bin", Size: int64(len(content))}}}
	if err := m.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(dir, relPath))
	}); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte("X"), 0); err != nil {
		t.Fatal(err)
	}

	ok, err := VerifySegment(f, m.Files[0], 0)
	if err != nil {
		t.Fatalf("VerifySegment: %v", err)
	}
	if ok {
		t.Fatalf("VerifySegment = true for corrupted data, want false")
	}
}

// A file holding the right bytes followed by stale extra bytes is not the
// manifest's file, even though its first fm.Size bytes hash correctly.
func TestVerifyWholeFileRejectsTrailingBytes(t *testing.T) {
	dir := t.TempDir()
	content := bytes.Repeat([]byte("c"), 1024)
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Model: "m", Revision: "r", Files: []FileManifest{{Path: "f.bin", Size: int64(len(content))}}}
	if err := m.BuildSegmentHashes(func(relPath string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(dir, relPath))
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(content, "stale tail"...), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ok, err := VerifyWholeFile(f, m.Files[0])
	if err != nil {
		t.Fatalf("VerifyWholeFile: %v", err)
	}
	if ok {
		t.Fatal("VerifyWholeFile = true for a file with trailing bytes, want false")
	}
}

func TestStoreManifestRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{{Path: "a.bin", Size: 10, SHA256: "deadbeef"}}}
	if s.HasManifest("m", "r1") {
		t.Fatalf("HasManifest true before save")
	}
	if err := s.SaveManifest(m); err != nil {
		t.Fatalf("SaveManifest: %v", err)
	}
	if !s.HasManifest("m", "r1") {
		t.Fatalf("HasManifest false after save")
	}
	got, err := s.LoadManifest("m", "r1")
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if got.Model != m.Model || got.Revision != m.Revision || len(got.Files) != 1 || got.Files[0].SHA256 != "deadbeef" {
		t.Fatalf("round-tripped manifest mismatch: %+v", got)
	}
}

func TestStoreStateRoundTrip(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{
		{Path: "a.bin", Size: DefaultSegmentSize * 2, SegmentSize: DefaultSegmentSize, SegmentSHA256: []string{"h0", "h1"}},
	}}

	rs, err := s.LoadState(m)
	if err != nil {
		t.Fatalf("LoadState (fresh): %v", err)
	}
	if rs.Complete() {
		t.Fatalf("fresh state reports Complete")
	}
	fs := rs.FileState(m.Files[0])
	fs.SegmentsDone[0] = true
	fs.SegmentsDone[1] = true
	fs.Complete = true
	rs.SetFileState(fs)

	if err := s.SaveState(rs); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	got, err := s.LoadState(m)
	if err != nil {
		t.Fatalf("LoadState (saved): %v", err)
	}
	if !got.Complete() {
		t.Fatalf("saved state does not report Complete")
	}
	if got.BytesDone(m) != DefaultSegmentSize*2 {
		t.Fatalf("BytesDone = %d, want %d", got.BytesDone(m), DefaultSegmentSize*2)
	}
}

func TestReadyRangesCoalesces(t *testing.T) {
	fm := FileManifest{
		Path: "a.bin", Size: 300, SegmentSize: 100,
		SegmentSHA256: []string{"h0", "h1", "h2"},
	}
	fs := FileState{Path: "a.bin", SegmentsDone: []bool{true, true, false}}
	ranges := fs.ReadyRanges(fm)
	if len(ranges) != 1 || ranges[0] != (Range{Start: 0, End: 200}) {
		t.Fatalf("ReadyRanges = %+v, want one range [0,200)", ranges)
	}

	fs2 := FileState{Path: "a.bin", SegmentsDone: []bool{true, false, true}}
	ranges2 := fs2.ReadyRanges(fm)
	if len(ranges2) != 2 || ranges2[0] != (Range{Start: 0, End: 100}) || ranges2[1] != (Range{Start: 200, End: 300}) {
		t.Fatalf("ReadyRanges = %+v, want two disjoint ranges", ranges2)
	}
}

func TestEnsureFileTruncatesToSize(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "sub/dir/a.bin", Size: 4096}
	f, err := s.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatalf("EnsureFile: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 4096 {
		t.Fatalf("size = %d, want 4096", info.Size())
	}
}
