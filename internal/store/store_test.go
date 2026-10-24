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

// Reverify drops a held segment whose bytes on disk no longer match, and
// keeps the rest.
func TestReverifyDropsCorruptSegment(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("q"), 30)
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{{Path: "w.bin", Size: 30, SegmentSize: 10}}}
	if err := m.BuildSegmentHashes(func(string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if m.Files[0].NumSegments() != 3 {
		t.Fatalf("NumSegments = %d, want 3 (BuildSegmentHashes keeps a preset SegmentSize)", m.Files[0].NumSegments())
	}
	f, err := s.EnsureFile("m", "r1", m.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(content, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("X"), 15); err != nil { // corrupt segment 1
		t.Fatal(err)
	}
	f.Close()
	rs := NewRevisionState(m)
	fs := rs.FileState(m.Files[0])
	fs.SegmentsDone = []bool{true, true, true}
	fs.Complete = true
	rs.SetFileState(fs)

	dropped, err := s.Reverify(m, rs)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	got, err := s.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	if fs := got.FileState(m.Files[0]); fs.Complete || fs.SegmentsDone[0] != true || fs.SegmentsDone[1] != false || fs.SegmentsDone[2] != true {
		t.Fatalf("saved state = %+v, want only segment 1 dropped and not complete", fs)
	}

	if err := os.Remove(s.FilePath("m", "r1", "w.bin")); err != nil {
		t.Fatal(err)
	}
	if dropped, err := s.Reverify(m, got); err != nil || dropped != 2 {
		t.Fatalf("Reverify of a deleted file = %d, %v; want 2 dropped", dropped, err)
	}
}

func TestManifestValidate(t *testing.T) {
	ok := FileManifest{Path: "dir/a.bin", Size: 25, SegmentSize: 10, SegmentSHA256: []string{"h0", "h1", "h2"}}
	if err := (&Manifest{Files: []FileManifest{ok, {Path: "empty", Size: 0}}}).Validate(); err != nil {
		t.Fatalf("Validate(good) = %v", err)
	}
	for name, fm := range map[string]FileManifest{
		"too few hashes": {Path: "a", Size: 25, SegmentSize: 10, SegmentSHA256: []string{"h0", "h1"}},
		"no seg size":    {Path: "a", Size: 25, SegmentSHA256: []string{"h0"}},
		"negative size":  {Path: "a", Size: -1},
		"dotdot":         {Path: "../etc/passwd", Size: 0},
		"inner dotdot":   {Path: "a/../../b", Size: 0},
		"absolute":       {Path: "/etc/passwd", Size: 0},
		"unclean":        {Path: "a//b", Size: 0},
		"empty path":     {Path: "", Size: 0},
		"backslash":      {Path: "a\\..\\b", Size: 0},
	} {
		if err := (&Manifest{Files: []FileManifest{fm}}).Validate(); err == nil {
			t.Errorf("%s: Validate = nil, want an error", name)
		}
	}
	if err := (&Manifest{Files: []FileManifest{ok, ok}}).Validate(); err == nil {
		t.Error("duplicate path: Validate = nil, want an error")
	}
}

// Sizes near MaxInt64 must not overflow the segment count, and a file may
// not have so many segments that per-segment state has no bound.
func TestManifestValidateOverflow(t *testing.T) {
	const max = int64(^uint64(0) >> 1)
	m := &Manifest{Files: []FileManifest{{Path: "a", Size: max, SegmentSize: max}}}
	if err := m.Validate(); err != nil || m.Files[0].NumSegments() != 1 {
		t.Fatalf("Validate = %v, NumSegments = %d; want one segment", err, m.Files[0].NumSegments())
	}
	m = &Manifest{Files: []FileManifest{{Path: "a", Size: max, SegmentSize: 1}}}
	if err := m.Validate(); err == nil {
		t.Fatal("Validate = nil for a file with MaxInt64 segments")
	}
}

func seedHeld(t *testing.T, s *Store, files map[string][]byte, order ...string) (*Manifest, *RevisionState) {
	t.Helper()
	m := &Manifest{Model: "m", Revision: "r1"}
	for _, p := range order {
		m.Files = append(m.Files, FileManifest{Path: p, Size: int64(len(files[p])), SegmentSize: 10})
	}
	if err := m.BuildSegmentHashes(func(p string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(files[p])), nil
	}); err != nil {
		t.Fatal(err)
	}
	rs := NewRevisionState(m)
	for _, fm := range m.Files {
		f, err := s.EnsureFile("m", "r1", fm)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteAt(files[fm.Path], 0)
		f.Close()
		fs := rs.FileState(fm)
		for i := range fs.SegmentsDone {
			fs.SegmentsDone[i] = true
		}
		fs.Complete = true
		rs.SetFileState(fs)
	}
	return m, rs
}

// A file that cannot be opened must not stop Reverify from saving what it
// found bad in the files before it.
func TestReverifySavesBeforeLaterFailure(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, rs := seedHeld(t, s, map[string][]byte{"a.bin": bytes.Repeat([]byte("a"), 20), "b.bin": bytes.Repeat([]byte("b"), 20)}, "a.bin", "b.bin")
	f, _ := os.OpenFile(s.FilePath("m", "r1", "a.bin"), os.O_RDWR, 0)
	f.WriteAt([]byte("X"), 3)
	f.Close()
	bp := s.FilePath("m", "r1", "b.bin")
	os.Remove(bp)
	if err := os.Symlink(bp, bp); err != nil { // a loop: open fails, not with ENOENT
		t.Fatal(err)
	}
	if _, err := s.Reverify(m, rs); err != nil {
		t.Fatalf("Reverify: %v", err)
	}
	got, _ := s.LoadState(m)
	if fs := got.FileState(m.Files[0]); fs.Complete || fs.SegmentsDone[0] {
		t.Fatalf("a.bin state %+v: the corrupt segment is still held", fs)
	}
	if fs := got.FileState(m.Files[1]); fs.Complete || fs.SegmentsDone[0] || fs.SegmentsDone[1] {
		t.Fatalf("b.bin state %+v: an unreadable file is still held", fs)
	}
}

// A file that grew or shrank on disk is no longer complete, and is put back
// to its manifest size.
func TestReverifyFileSizeChanged(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, rs := seedHeld(t, s, map[string][]byte{"a.bin": bytes.Repeat([]byte("a"), 30)}, "a.bin")
	p := s.FilePath("m", "r1", "a.bin")
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte("extra"))
	f.Close()
	if _, err := s.Reverify(m, rs); err != nil {
		t.Fatal(err)
	}
	if fs := rs.FileState(m.Files[0]); fs.Complete {
		t.Fatal("a grown file is still complete")
	}
	if info, _ := os.Stat(p); info.Size() != 30 {
		t.Fatalf("size = %d, want 30", info.Size())
	}

	os.Truncate(p, 10)
	if _, err := s.Reverify(m, rs); err != nil {
		t.Fatal(err)
	}
	fs := rs.FileState(m.Files[0])
	if !fs.SegmentsDone[0] || fs.SegmentsDone[1] || fs.SegmentsDone[2] {
		t.Fatalf("segments = %v, want only segment 0 kept after truncation", fs.SegmentsDone)
	}
	if info, _ := os.Stat(p); info.Size() != 30 {
		t.Fatalf("size = %d, want 30 (sparse, full size)", info.Size())
	}
}

// Reverify does not follow a symlink out of the store, so it neither reads
// nor truncates what it points to.
func TestReverifyIgnoresSymlink(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, rs := seedHeld(t, s, map[string][]byte{"a.bin": bytes.Repeat([]byte("a"), 30)}, "a.bin")
	outside := filepath.Join(t.TempDir(), "important")
	os.WriteFile(outside, []byte("0123456789IMPORTANT0123456789IMPORTANT"), 0o644)
	p := s.FilePath("m", "r1", "a.bin")
	os.Remove(p)
	if err := os.Symlink(outside, p); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reverify(m, rs); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(outside); string(got) != "0123456789IMPORTANT0123456789IMPORTANT" {
		t.Fatalf("outside file changed: %q", got)
	}
	if fs := rs.FileState(m.Files[0]); fs.Complete || fs.SegmentsDone[0] {
		t.Fatalf("a symlinked file is still held: %+v", fs)
	}
}

// Segment bounds never overflow, even for sizes near MaxInt64.
func TestSegmentBoundsNoOverflow(t *testing.T) {
	const max = int64(^uint64(0) >> 1)
	fm := FileManifest{Size: max, SegmentSize: max - 1, SegmentSHA256: []string{"a", "b"}}
	if s, e := fm.SegmentBounds(1); s != max-1 || e != max {
		t.Fatalf("bounds = (%d, %d), want (%d, %d)", s, e, max-1, max)
	}
}

// A failed state save is remembered until a save succeeds.
func TestStateSaved(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m, rs := seedHeld(t, s, map[string][]byte{"a.bin": []byte("aaaa")}, "a.bin")
	if !s.StateSaved("m", "r1") {
		t.Fatal("StateSaved = false before any failure")
	}
	sd := filepath.Join(dir, "state")
	os.Chmod(sd, 0o500)
	os.RemoveAll(filepath.Join(sd, "m"))
	if err := s.SaveState(rs); err == nil {
		os.Chmod(sd, 0o755)
		t.Skip("state dir still writable (running as root?)")
	}
	if s.StateSaved("m", "r1") {
		t.Fatal("StateSaved = true after a failed save")
	}
	os.Chmod(sd, 0o755)
	if err := s.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	if !s.StateSaved("m", "r1") {
		t.Fatal("StateSaved = false after a good save")
	}
	_ = m
}
