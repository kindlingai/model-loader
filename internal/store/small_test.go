package store

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func withChunk(t *testing.T, n int64) {
	t.Helper()
	old := ChunkSize
	ChunkSize = n
	t.Cleanup(func() { ChunkSize = old })
}

// A file of small segments is saved as held ranges and trust ranges, with
// no segments_done; a file of 64 MiB segments keeps segments_done as
// well. Both load back the same.
func TestStateEncoding(t *testing.T) {
	withChunk(t, 1<<20)
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	small := FileManifest{Path: "experts/w.k4", Size: 100 << 10, SegmentSize: 4 << 10} // 25 segments
	big := FileManifest{Path: "config.json", Size: 10, SegmentSize: DefaultSegmentSize, SegmentSHA256: []string{"x"}}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{small, big}}
	rs := NewRevisionState(m)
	for _, i := range []int{0, 1, 2, 7, 8, 24} {
		rs.MarkSegment(small, i, "", TrustOrigin)
	}
	rs.SetTrust(small, 8, TrustRequester)
	rs.MarkSegment(big, 0, "", TrustManifest)
	if err := s.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(s.Dir(), "state", "m", "r1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Files []map[string]json.RawMessage `json:"files"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	for _, f := range raw.Files {
		var path string
		json.Unmarshal(f["path"], &path)
		var held [][2]int
		json.Unmarshal(f["held"], &held)
		_, done := f["segments_done"]
		switch path {
		case small.Path:
			if done || len(held) != 3 || held[0] != [2]int{0, 3} || held[1] != [2]int{7, 9} || held[2] != [2]int{24, 25} {
				t.Fatalf("small file on disk: held %v, segments_done %v", held, done)
			}
			if _, ok := f["trust"]; ok {
				t.Fatal("small file has a trust entry per segment")
			}
			if !strings.Contains(string(f["trust_runs"]), `"requester":[[8,9]]`) {
				t.Fatalf("trust_runs = %s", f["trust_runs"])
			}
		case big.Path:
			if !done || len(held) != 1 {
				t.Fatalf("64 MiB file on disk: held %v, segments_done %v", held, done)
			}
		}
	}
	got, err := s.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	fs := got.FileState(small)
	if len(fs.SegmentsDone) != 25 {
		t.Fatalf("loaded %d segments, want 25", len(fs.SegmentsDone))
	}
	for i := range 25 {
		want := ""
		switch i {
		case 0, 1, 2, 7, 24:
			want = TrustOrigin
		case 8:
			want = TrustRequester
		}
		if fs.TrustOf(i) != want {
			t.Fatalf("segment %d: trust %q, want %q", i, fs.TrustOf(i), want)
		}
	}
	if fs.LastUsed(0) == 0 {
		t.Fatal("chunk 0 has no use time")
	}
	if len(fs.Used) != 1 {
		t.Fatalf("use times: %d, want one per 1 MiB chunk", len(fs.Used))
	}
}

// State an older version wrote (segments_done, trust per segment, a sha256
// per segment) loads as before.
func TestStateLegacy(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "a", Size: 30, SegmentSize: 10}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{fm}}
	legacy := `{"model":"m","revision":"r1","files":[{"path":"a","complete":false,"segments_done":[true,false,true],` +
		`"segment_sha256":["h0","","h2"],"trust":["origin","","requester"],"used":[5,0,7]}]}`
	if err := os.MkdirAll(filepath.Join(s.Dir(), "state", "m"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Dir(), "state", "m", "r1.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	rs, err := s.LoadState(m)
	if err != nil {
		t.Fatal(err)
	}
	fs := rs.FileState(fm)
	if !fs.SegmentsDone[0] || fs.SegmentsDone[1] || !fs.SegmentsDone[2] || fs.TrustOf(0) != TrustOrigin || fs.TrustOf(2) != TrustRequester ||
		fs.SegmentHash(fm, 2) != "h2" || fs.LastUsed(2) != 7 {
		t.Fatalf("legacy state = %+v", fs)
	}
	// And saves back with what an older consumer reads.
	if err := s.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(s.Dir(), "state", "m", "r1.json"))
	for _, want := range []string{`"segments_done": [`, `"held": [`, `"trust": [`, `"segment_sha256": [`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("saved state has no %s: %s", want, data)
		}
	}
}

// Side-file segment hashes round-trip, read back as "" where none was
// written, and go with the revision.
func TestSegmentHashesSideFile(t *testing.T) {
	withChunk(t, 1<<20)
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "dir/w.k4", Size: 64 << 10, SegmentSize: 4 << 10}
	a, b := sha256.Sum256([]byte("a")), sha256.Sum256([]byte("b"))
	if err := s.WriteSegmentHashes("m", "refs/pr/1", fm, 3, [][sha256.Size]byte{a, b}); err != nil {
		t.Fatal(err)
	}
	got := s.SegmentSums("m", "refs/pr/1", fm, 2, 4)
	if got[0] != "" || got[1] == "" || got[2] == "" || got[3] != "" {
		t.Fatalf("SegmentSums = %q", got)
	}
	if err := s.RemoveRevision("m", "refs/pr/1", []string{fm.Path}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.Dir(), "hashes")); err != nil {
		t.Fatalf("hashes dir: %v", err) // the top directory stays, as for models/
	}
	if entries, _ := os.ReadDir(filepath.Join(s.Dir(), "hashes")); len(entries) != 0 {
		t.Fatalf("hash files left after removing the revision: %v", entries)
	}
}

// Reverify checks each held small segment against its side-file sha256.
func TestReverifySmallSegments(t *testing.T) {
	withChunk(t, 1<<20)
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const seg = 4 << 10
	data := make([]byte, 4*seg)
	for i := range data {
		data[i] = byte(i % 253)
	}
	fm := FileManifest{Path: "w", Size: int64(len(data)), SegmentSize: seg}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{fm}}
	f, err := s.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt(data, 0)
	sums, err := HashSegments(f, fm, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte("bad"), 2*seg+1) // segment 2 goes bad
	f.Close()
	if err := s.WriteSegmentHashes("m", "r1", fm, 0, sums); err != nil {
		t.Fatal(err)
	}
	rs := NewRevisionState(m)
	for i := range 4 {
		rs.MarkSegment(fm, i, "", TrustOrigin)
	}
	dropped, err := s.Reverify(m, rs)
	if err != nil || dropped != 1 {
		t.Fatalf("Reverify = %d, %v; want 1 dropped", dropped, err)
	}
	if fs := rs.FileState(fm); fs.SegmentsDone[2] || !fs.SegmentsDone[3] {
		t.Fatalf("after reverify: %v", fs.SegmentsDone)
	}
}

// SaveStateSoon writes at most once a second, but always after a failed
// save.
func TestSaveStateSoon(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "a", Size: 30, SegmentSize: 10}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{fm}}
	rs := NewRevisionState(m)
	rs.MarkSegment(fm, 0, "", TrustManifest)
	if err := s.SaveStateSoon(rs); err != nil {
		t.Fatal(err)
	}
	rs.MarkSegment(fm, 1, "", TrustManifest)
	if err := s.SaveStateSoon(rs); err != nil {
		t.Fatal(err)
	}
	got, _ := s.LoadState(m)
	if fs := got.FileState(fm); !fs.SegmentsDone[0] || fs.SegmentsDone[1] {
		t.Fatalf("saved %v, want only segment 0 (second save within a second)", fs.SegmentsDone)
	}
	if err := s.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadState(m)
	if fs := got.FileState(fm); !fs.SegmentsDone[1] {
		t.Fatal("SaveState did not write")
	}
}

// A save SaveStateSoon put off is written once the second is up, unless
// the state is closed (its revision removed) first.
func TestSaveStateSoonFlushAndClose(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fm := FileManifest{Path: "a", Size: 30, SegmentSize: 10}
	m := &Manifest{Model: "m", Revision: "r1", Files: []FileManifest{fm}}
	rs := NewRevisionState(m)
	rs.MarkSegment(fm, 0, "", TrustManifest)
	s.SaveStateSoon(rs)
	rs.MarkSegment(fm, 1, "", TrustManifest)
	s.SaveStateSoon(rs) // put off
	time.Sleep(stateSaveEvery + 300*time.Millisecond)
	got, _ := s.LoadState(m)
	if !got.FileState(fm).SegmentsDone[1] {
		t.Fatal("the save put off was not written")
	}

	rs.MarkSegment(fm, 2, "", TrustManifest)
	s.SaveStateSoon(rs) // saved now: the last was over a second ago
	rs.MarkSegment(fm, 0, "", TrustManifest)
	s.SaveStateSoon(rs) // put off
	rs.Close()
	if err := s.RemoveRevision("m", "r1", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(stateSaveEvery + 300*time.Millisecond)
	if _, err := os.Stat(filepath.Join(s.Dir(), "state", "m", "r1.json")); !os.IsNotExist(err) {
		t.Fatalf("state of a removed revision written back: %v", err)
	}
}
