package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// Content dedup (kindlingai/model-loader#38): a root that holds a model
// downloads another that publishes the same files only for what differs,
// copying the rest, with each file's segment hashes as its manifest needs
// them: carried over from the twin's manifest or side file, or hashed when
// the segment sizes differ.
func TestDedupRootCopiesTwin(t *testing.T) {
	shared := map[string][]byte{
		"w.bin":                    pattern(300_000, 'w'),
		"experts/x.k4.safetensors": pattern(200_000, 'x'), // small segments in both
		"experts/y.k3.safetensors": pattern(150_000, 'y'), // small segments in a only
	}
	filesA := map[string][]byte{"README.md": []byte("a")}
	filesB := map[string][]byte{"README.md": []byte("b"), "steering/v.safetensors": pattern(1000, 's')}
	var sharedBytes int64
	for p, c := range shared {
		filesA[p], filesB[p] = c, c
		sharedBytes += int64(len(c))
	}
	tsA, _ := newCountingWANServer(t, filesA)
	tsB, countsB := newCountingWANServer(t, filesB)
	mA, mB := wanModel("a", tsA.URL), wanModel("b", tsB.URL)
	mA.SegmentSizes = []catalog.SegmentSizeRule{{Path: "experts/*.k?.safetensors", Size: 64 << 10}}
	mB.SegmentSizes = []catalog.SegmentSizeRule{{Path: "experts/*.k4.safetensors", Size: 64 << 10}}

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: "root-1", Roles: []string{"root"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{mA}}, Selection: allSelection(),
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil), Dedup: true,
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile a: %v", err)
	}
	d.cfg.Catalog = &catalog.Catalog{Models: []catalog.Model{mA, mB}}
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile b: %v", err)
	}

	for p := range shared {
		if n := getCount(countsB, p); n != 0 {
			t.Errorf("b's %s downloaded %d times, want copied", p, n)
		}
	}
	for p := range filesB {
		if _, ok := shared[p]; !ok && getCount(countsB, p) != 1 {
			t.Errorf("b's %s downloaded %d times, want once", p, getCount(countsB, p))
		}
	}
	for _, ms := range d.Status().Models {
		if ms.Model == "b" && (ms.State != server.StateReady || ms.BytesReused != sharedBytes) {
			t.Errorf("b: %s, %d bytes reused (%s); want ready, %d", ms.State, ms.BytesReused, ms.Error, sharedBytes)
		}
	}
	for p, c := range filesB {
		got, err := os.ReadFile(st.FilePath("b", "r1", p))
		if err != nil || !bytes.Equal(got, c) {
			t.Errorf("b's %s: %v, content right: %v", p, err, bytes.Equal(got, c))
		}
	}
	a, _ := os.Stat(st.FilePath("a", "r1", "w.bin"))
	b, _ := os.Stat(st.FilePath("b", "r1", "w.bin"))
	if os.SameFile(a, b) {
		t.Error("b's w.bin is a's file: evicting one would punch holes in both")
	}

	sums := func(model, path string) []string {
		mf, _ := st.LoadManifest(model, "r1")
		fm, _ := mf.Find(path)
		rs, _ := d.revState(mf)
		out := make([]string, fm.NumSegments())
		for i := range out {
			out[i] = st.SegmentHashOf(model, "r1", fm, rs.FileState(fm), i)
		}
		return out
	}
	for _, p := range []string{"w.bin", "experts/x.k4.safetensors"} {
		if a, b := sums("a", p), sums("b", p); slices.Contains(b, "") || !slices.Equal(a, b) {
			t.Errorf("b's %s segment hashes %v, want a's %v", p, b, a)
		}
	}
	sum := sha256.Sum256(shared["experts/y.k3.safetensors"])
	if got := sums("b", "experts/y.k3.safetensors"); !slices.Equal(got, []string{hex.EncodeToString(sum[:])}) {
		t.Errorf("b's y segment hashes %v, want one 64 MiB segment, hashed", got)
	}
}

// A root does not copy a twin into a file a peer's pull is fetching part
// of, as the copy would write under the pull; once the pull is over, it
// does.
func TestDedupWaitsOutPull(t *testing.T) {
	w := pattern(300, 'w')
	tsA, _ := newCountingWANServer(t, map[string][]byte{"w.bin": w})
	tsB, _ := newCountingWANServer(t, map[string][]byte{"w.bin": w})
	mA, mB := wanModel("a", tsA.URL), wanModel("b", tsB.URL)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: "root-1", Roles: []string{"root"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{mA, mB}}, Selection: &catalog.Selection{Wanted: []string{"a"}},
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil), Dedup: true,
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	mf, err := d.Manifest(context.Background(), "b", "r1")
	if err != nil {
		t.Fatal(err)
	}
	fm := mf.Files[0]
	rs, _ := d.revState(mf)

	pull, _ := d.claimSegments(claimKey(mB, fm), fm, rs, []bool{true})
	if ok, err := d.reuseHeld(context.Background(), mB, fm, rs, d.twins()); ok || err != nil {
		t.Fatalf("reuseHeld during a pull = %v, %v; want no copy", ok, err)
	}
	d.releaseSegments(claimKey(mB, fm), pull)
	if ok, err := d.reuseHeld(context.Background(), mB, fm, rs, d.twins()); !ok || err != nil {
		t.Fatalf("reuseHeld after the pull = %v, %v; want a copy", ok, err)
	}
}

// A peer copies a file another revision in its store holds whole, and
// fetches it over the LAN as before when that one is no longer whole.
func TestDedupPeerCopiesOnlyWholeTwin(t *testing.T) {
	w, steer := pattern(300_000, 'w'), pattern(1000, 's')
	for _, held := range []bool{true, false} {
		rootSt, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		seedFiles(t, rootSt, "b", 0, map[string][]byte{"w.bin": w, "steering/v.safetensors": steer}, "w.bin", "steering/v.safetensors")
		ts, blobs := countingPeer(t, rootSt)

		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		mfA := seedFiles(t, st, "a", 0, map[string][]byte{"w.bin": w}, "w.bin")
		if !held {
			rs, _ := st.LoadState(mfA)
			rs.DropSegment(mfA.Files[0], 0)
			if err := st.SaveState(rs); err != nil {
				t.Fatal(err)
			}
		}
		mB := catalog.Model{Name: "b", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
		d := New(Config{
			NodeID: "spark", Roles: []string{"peer"}, Store: st,
			Catalog: &catalog.Catalog{Models: []catalog.Model{mB}}, Selection: allSelection(),
			Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}), Dedup: true,
		})
		if err := d.Reconcile(context.Background()); err != nil {
			t.Fatalf("held %v: Reconcile: %v", held, err)
		}

		wantBlobs, wantReused := int32(1), int64(len(w))
		if !held {
			wantBlobs, wantReused = 2, 0
		}
		ms := d.Status().Models[0]
		if ms.State != server.StateReady || ms.BytesReused != wantReused || blobs.Load() != wantBlobs {
			t.Errorf("twin held %v: %s, %d bytes reused, %d blob requests (%s); want ready, %d, %d", held, ms.State, ms.BytesReused, blobs.Load(), ms.Error, wantReused, wantBlobs)
		}
		if got, _ := os.ReadFile(st.FilePath("b", "r1", "w.bin")); !bytes.Equal(got, w) {
			t.Errorf("twin held %v: b's w.bin is not the release's", held)
		}
	}
}

// A peer that holds part of a file fetches the rest over the LAN rather
// than copy a twin over the segments it has checked already.
func TestDedupPeerFetchesRestOfPartFile(t *testing.T) {
	w := pattern(300_000, 'w')
	rootSt, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, rootSt, "b", 100_000, map[string][]byte{"w.bin": w}, "w.bin")
	ts, blobs := countingPeer(t, rootSt)

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedFiles(t, st, "a", 0, map[string][]byte{"w.bin": w}, "w.bin")
	mfB := seedFiles(t, st, "b", 100_000, map[string][]byte{"w.bin": w}, "w.bin")
	rs, _ := st.LoadState(mfB)
	rs.DropSegment(mfB.Files[0], 1)
	rs.DropSegment(mfB.Files[0], 2)
	if err := st.SaveState(rs); err != nil {
		t.Fatal(err)
	}
	mB := catalog.Model{Name: "b", Revision: "r1", Source: catalog.Source{Type: catalog.SourceHTTP, URL: "http://unused.invalid"}}
	d := New(Config{
		NodeID: "spark", Roles: []string{"peer"}, Store: st,
		Catalog: &catalog.Catalog{Models: []catalog.Model{mB}}, Selection: allSelection(),
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic([]string{ts.Listener.Addr().String()}), Dedup: true,
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ms := d.Status().Models[0]
	if ms.State != server.StateReady || ms.BytesReused != 0 || blobs.Load() == 0 {
		t.Errorf("%s, %d bytes reused, %d blob requests (%s); want ready, 0, some", ms.State, ms.BytesReused, blobs.Load(), ms.Error)
	}
}

// A twin that a re-verify finds bad while it is copied is not used, even
// once it is fetched again and complete by the time the copy ends: the
// copy read the bad bytes.
func TestDedupTwinRepairedDuringCopy(t *testing.T) {
	w := pattern(300_000, 'w')
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mfA := seedFiles(t, st, "a", 100_000, map[string][]byte{"w.bin": w}, "w.bin")
	d := New(Config{
		NodeID: "spark", Roles: []string{"peer"}, Store: st, Catalog: &catalog.Catalog{}, Selection: allSelection(),
		Sources: source.New(nil, ""), Discoverer: discovery.NewStatic(nil), Dedup: true,
	})
	rsA, err := d.revState(mfA)
	if err != nil {
		t.Fatal(err)
	}
	fa, fb := mfA.Files[0], mfA.Files[0]
	mB := catalog.Model{Name: "b", Revision: "r1"}
	for _, repair := range []bool{false, true} {
		progress := func(int64) {
			if repair {
				rsA.DropSegment(fa, 1)
				rsA.MarkSegment(fa, 1, "", store.TrustManifest)
				rsA.SetComplete(fa)
			}
		}
		if _, ok := d.reuse(context.Background(), mB, fb, d.twins(), progress); ok == repair {
			t.Errorf("twin repaired during the copy %v: reused %v", repair, ok)
		}
	}
}
