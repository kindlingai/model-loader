package transfer

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

// streamRuns begins a run for each of firsts (one 4-byte segment each) and
// writes its bytes, then ends them in the order of ends.
func streamRuns(w *wholeHash, content []byte, reorder *Reorder, firsts, ends []int) {
	runs := map[int]*wholeRun{}
	for _, i := range firsts {
		r := w.begin(i, 1, 4, reorder, &Stats{})
		r.Write(content[4*i : 4*i+4])
		runs[i] = r
	}
	for _, i := range ends {
		runs[i].done(true)
	}
}

// checkTaken takes path's hash and checks it covers wantFrom bytes and,
// with the rest of content, sums to its sha256.
func checkTaken(t *testing.T, path string, fm store.FileManifest, content []byte, wantFrom int64) {
	t.Helper()
	h, from := takeWhole(path, fm)
	if from != wantFrom {
		t.Fatalf("hashed in stream through byte %d, want %d", from, wantFrom)
	}
	h.Write(content[from:])
	if got, want := h.Sum(nil), sha256.Sum256(content); string(got) != string(want[:]) {
		t.Fatal("in-stream hash plus the rest does not match the file's sha256")
	}
}

func TestWholeHashOutOfOrder(t *testing.T) {
	content := []byte("0123456789abcdef")
	fm := buildManifestFile("a.bin", content, 4)
	all := [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}}

	t.Run("room for every run ahead", func(t *testing.T) {
		path := t.Name()
		reorder := NewReorder(100)
		w := wholeFor(path, fm)
		defer w.expect(all, nil)()
		streamRuns(w, content, reorder, []int{0, 1, 2, 3}, []int{2, 3, 1, 0})
		checkTaken(t, path, fm, content, 16)
		if reorder.free != 100 {
			t.Fatalf("reorder has %d bytes free, want 100", reorder.free)
		}
	})

	t.Run("no room stops at the first run not kept", func(t *testing.T) {
		path := t.Name()
		reorder := NewReorder(4) // room for one run
		w := wholeFor(path, fm)
		defer w.expect(all, nil)()
		streamRuns(w, content, reorder, []int{0, 2, 1, 3}, []int{3, 2, 1, 0})
		checkTaken(t, path, fm, content, 4)
		if reorder.free != 4 {
			t.Fatalf("reorder has %d bytes free, want 4", reorder.free)
		}
	})

	t.Run("a hashed segment written again starts over", func(t *testing.T) {
		path := t.Name()
		w := wholeFor(path, fm)
		defer w.expect(all, nil)()
		streamRuns(w, content, nil, []int{0}, []int{0})
		streamRuns(w, content, nil, []int{1}, []int{1})
		streamRuns(w, content, nil, []int{0}, []int{0})
		checkTaken(t, path, fm, content, 0)
	})

	t.Run("a run ahead of a segment nothing fetches is not kept", func(t *testing.T) {
		path := t.Name()
		reorder := NewReorder(100)
		w := wholeFor(path, fm)
		unexpect := w.expect([][2]int{{2, 3}}, nil)
		streamRuns(w, content, reorder, []int{2}, []int{2})
		if reorder.free != 100 {
			t.Fatalf("reorder has %d bytes free, want 100", reorder.free)
		}
		unexpect()
		if _, from := takeWhole(path, fm); from != 0 {
			t.Fatalf("hashed in stream through byte %d, want 0", from)
		}
	})
}

// FinishFile must not read back what landed in order: bytes changed on
// disk after they streamed in go unseen, which only the in-stream hash
// explains.
func TestFinishFileUsesInStreamHash(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789abcdef")
	fm := buildManifestFile("a.bin", content, 4)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	rs := store.NewRevisionState(m)
	fetcher := &fakeFetcher{content: content, ready: int64(len(content))}
	first := Options{Need: []bool{true, true, true, false}}
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, first); err != nil {
		t.Fatal(err)
	}
	f, err := st.EnsureFile("m", "r1", fm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XXXX"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{fetcher}, Options{}); err != nil {
		t.Fatal(err)
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}
