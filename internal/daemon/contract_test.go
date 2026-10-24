package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// fakeHub is a Hugging Face Hub with one commit per repo. Its tree and
// files answer only at the commit, so a fetch by branch or tag fails.
func fakeHub(t *testing.T, commit string, repos map[string]map[string][]byte) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for repo, files := range repos {
			if rev, ok := strings.CutPrefix(r.URL.Path, "/api/models/"+repo+"/revision/"); ok && rev != "" {
				fmt.Fprintf(w, `{"sha":%q}`, commit)
				return
			}
			if r.URL.Path == "/api/models/"+repo+"/tree/"+commit {
				var entries []map[string]any
				for _, p := range sortedKeys(files) {
					sum := sha256.Sum256(files[p])
					entries = append(entries, map[string]any{"type": "file", "path": p, "size": 1,
						"lfs": map[string]any{"oid": hex.EncodeToString(sum[:]), "size": len(files[p])}})
				}
				_ = json.NewEncoder(w).Encode(entries)
				return
			}
			p, ok := strings.CutPrefix(r.URL.Path, "/"+repo+"/resolve/"+commit+"/")
			if content, found := files[p]; ok && found {
				start, end := 0, len(content)-1
				if h := r.Header.Get("Range"); h != "" {
					a, b, _ := strings.Cut(strings.TrimPrefix(h, "bytes="), "-")
					start, _ = strconv.Atoi(a)
					if b != "" {
						end, _ = strconv.Atoi(b)
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
					w.WriteHeader(http.StatusPartialContent)
				}
				_, _ = w.Write(content[start : end+1])
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// toServer sends every request to ts, whatever its host.
type toServer struct{ ts *httptest.Server }

func (s toServer) RoundTrip(r *http.Request) (*http.Response, error) {
	u, _ := url.Parse(s.ts.URL)
	r = r.Clone(r.Context())
	r.URL.Scheme, r.URL.Host, r.Host = u.Scheme, u.Host, u.Host
	return http.DefaultTransport.RoundTrip(r)
}

// What a consumer reads, by the store contract in docs/design.md, with no
// model-loader code and no network: only the files under the store.
type consumerManifest struct {
	Commit string `json:"commit"`
	Files  []struct {
		Path        string `json:"path"`
		Size        int64  `json:"size"`
		SegmentSize int64  `json:"segment_size"`
	} `json:"files"`
}

type consumerFile struct {
	Path         string `json:"path"`
	Complete     bool   `json:"complete"`
	SegmentsDone []bool `json:"segments_done"`
	WholeFailed  bool   `json:"whole_failed"`
}

func consumerRead(t *testing.T, storeDir, model, revision string) (consumerManifest, map[string]consumerFile) {
	t.Helper()
	var mf consumerManifest
	var st struct {
		Files []consumerFile `json:"files"`
	}
	for p, v := range map[string]any{
		filepath.Join(storeDir, "manifests", model, revision+".json"): &mf,
		filepath.Join(storeDir, "state", model, revision+".json"):     &st,
	} {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, v); err != nil {
			t.Fatal(err)
		}
	}
	files := make(map[string]consumerFile)
	for _, f := range st.Files {
		files[f.Path] = f
	}
	return mf, files
}

// consumerComplete: every file of the manifest is complete.
func consumerComplete(mf consumerManifest, files map[string]consumerFile) bool {
	for _, f := range mf.Files {
		if fs := files[f.Path]; !fs.Complete || fs.WholeFailed {
			return false
		}
	}
	return len(mf.Files) > 0
}

// consumerVerified: [off, off+n) of path is verified on disk.
func consumerVerified(mf consumerManifest, files map[string]consumerFile, path string, off, n int64) bool {
	for _, f := range mf.Files {
		if f.Path != path {
			continue
		}
		fs := files[path]
		if fs.WholeFailed || off < 0 || n <= 0 || off+n > f.Size {
			return false
		}
		if fs.Complete {
			return true
		}
		all := true
		for i := range fs.SegmentsDone {
			all = all && fs.SegmentsDone[i]
		}
		if all && int64(len(fs.SegmentsDone))*f.SegmentSize >= f.Size {
			return false // every segment held: waiting on its whole-file check.
		}
		for i := off / f.SegmentSize; i <= (off+n-1)/f.SegmentSize; i++ {
			if i >= int64(len(fs.SegmentsDone)) || !fs.SegmentsDone[i] {
				return false
			}
		}
		return true
	}
	return false
}

// A root fetches a whole model by branch and part of another by tag from
// the Hub. With the Hub gone and only the store's files to read, a
// consumer finds the commit each revision resolved to, that the whole
// model is complete, and which ranges of the partial one are verified, and
// reads them at their offsets.
func TestStoreContractOffline(t *testing.T) {
	const commit = "0123456789abcdef0123456789abcdef01234567"
	weights, part := pattern(100, 'w'), pattern(100, 'p')
	hub := fakeHub(t, commit, map[string]map[string][]byte{
		"org/full": {"config.json": []byte(`{"a":1}`), "w.bin": weights},
		"org/part": {"experts.bin": part},
	})
	full := catalog.Model{Name: "full", Revision: "main", Source: catalog.Source{Type: catalog.SourceHuggingFace, Repo: "org/full"}}
	partial := catalog.Model{Name: "part", Revision: "v1", Source: catalog.Source{Type: catalog.SourceHuggingFace, Repo: "org/part"}}

	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := New(Config{
		NodeID: "nas", Roles: []string{"root"}, Store: st, SegmentSize: 10,
		Catalog: &catalog.Catalog{Models: []catalog.Model{full, partial}},
		Selection: &catalog.Selection{Wanted: []string{"full"}, Partial: []catalog.Pieces{{
			Model: "part", Ranges: []catalog.FileRanges{{Path: "experts.bin", Ranges: []catalog.ByteRange{{Offset: 25, Length: 10}}}},
		}}},
		Sources:    source.New(&http.Client{Transport: toServer{hub}}, ""),
		Discoverer: discovery.NewStatic(nil),
	})
	if err := d.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, ms := range d.Status().Models {
		if want := map[string]string{"full": server.StateReady, "part": server.StatePartial}[ms.Model]; ms.State != want {
			t.Fatalf("%s: state %q, want %q (%s)", ms.Model, ms.State, want, ms.Error)
		}
	}
	hub.Close()

	mf, files := consumerRead(t, dir, "full", "main")
	if mf.Commit != commit {
		t.Fatalf("full@main commit = %q, want %q", mf.Commit, commit)
	}
	if !consumerComplete(mf, files) {
		t.Fatalf("full@main not complete: %+v", files)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "models", "full", "main", "w.bin")); !bytes.Equal(got, weights) {
		t.Fatal("full@main w.bin does not match the origin")
	}

	mf, files = consumerRead(t, dir, "part", "v1")
	if mf.Commit != commit {
		t.Fatalf("part@v1 commit = %q, want %q", mf.Commit, commit)
	}
	if consumerComplete(mf, files) {
		t.Fatal("part@v1 complete, want only its range")
	}
	for _, c := range []struct {
		off, n int64
		want   bool
	}{{25, 10, true}, {20, 20, true}, {0, 10, false}, {35, 10, false}, {80, 5, false}} {
		if got := consumerVerified(mf, files, "experts.bin", c.off, c.n); got != c.want {
			t.Errorf("part@v1 experts.bin [%d, +%d) verified = %v, want %v", c.off, c.n, got, c.want)
		}
	}
	f, err := os.Open(filepath.Join(dir, "models", "part", "v1", "experts.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, 10)
	if _, err := f.ReadAt(got, 25); err != nil || !bytes.Equal(got, part[25:35]) {
		t.Fatalf("part@v1 experts.bin at 25 = %q, %v; want %q", got, err, part[25:35])
	}
	if info, _ := f.Stat(); info.Size() != int64(len(part)) {
		t.Fatalf("part@v1 experts.bin is %d bytes, want its full size %d", info.Size(), len(part))
	}
}
