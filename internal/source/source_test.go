package source

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/kindlingai/model-loader/internal/catalog"
)

func TestHTTPSourceResolveAndOpen(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"files":[{"path":"a.bin","size":5,"sha256":"deadbeef"}]}`)
	})
	mux.HandleFunc("/a.bin", func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng != "bytes=2-4" {
			t.Errorf("unexpected Range header: %q", rng)
		}
		w.WriteHeader(http.StatusPartialContent)
		io.WriteString(w, "llo")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	reg := New(srv.Client())
	model := catalog.Model{
		Name:     "t",
		Source:   catalog.Source{Type: catalog.SourceHTTP, URL: srv.URL},
		Revision: "v1",
	}
	src, err := reg.For(model)
	if err != nil {
		t.Fatalf("For: %v", err)
	}

	resolved, err := src.Resolve(context.Background(), model)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(resolved.Files) != 1 || resolved.Files[0].Path != "a.bin" || resolved.Files[0].Size != 5 {
		t.Fatalf("unexpected resolved files: %+v", resolved.Files)
	}

	rc, err := src.Open(context.Background(), model, "a.bin", 2, 3)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "llo" {
		t.Fatalf("got %q, want %q", data, "llo")
	}
}

func TestHTTPSourceResolveErrorsOnMissingManifest(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	reg := New(srv.Client())
	model := catalog.Model{
		Source:   catalog.Source{Type: catalog.SourceHTTP, URL: srv.URL},
		Revision: "v1",
	}
	src, _ := reg.For(model)
	if _, err := src.Resolve(context.Background(), model); err == nil {
		t.Fatalf("expected error for missing manifest, got nil")
	}
}

func TestHuggingfaceSourceResolve(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/models/org/repo/tree/rev", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("recursive") != "true" {
			t.Errorf("expected recursive=true, got %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[
			{"type":"directory","path":"subdir"},
			{"type":"file","path":"config.json","size":10},
			{"type":"file","path":"model.safetensors","size":999,"lfs":{"oid":"abc123sha256","size":123456}}
		]`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	hf := &huggingfaceSource{client: srv.Client()}
	// Point the hf source at our test server by overriding via a
	// client that rewrites the host; simplest is to just exercise the
	// path-construction logic against the real huggingface.co host name
	// is not possible offline, so instead verify decode logic directly
	// using the registry's HTTP client against our mux through a
	// transport that redirects huggingface.co requests to srv.
	rt := rewriteHostTransport{target: srv.URL, base: http.DefaultTransport}
	hf.client = &http.Client{Transport: rt}

	model := catalog.Model{
		Source:   catalog.Source{Type: catalog.SourceHuggingFace, Repo: "org/repo"},
		Revision: "rev",
	}
	resolved, err := hf.Resolve(context.Background(), model)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(resolved.Files) != 2 {
		t.Fatalf("got %d files, want 2 (directory entry should be skipped): %+v", len(resolved.Files), resolved.Files)
	}
	var sawLFS bool
	for _, f := range resolved.Files {
		if f.Path == "model.safetensors" {
			sawLFS = true
			if f.Size != 123456 || f.SHA256 != "abc123sha256" {
				t.Errorf("lfs file not resolved from lfs block: %+v", f)
			}
		}
	}
	if !sawLFS {
		t.Fatalf("model.safetensors not found in resolved files")
	}
}

// rewriteHostTransport redirects any request to target's host, so tests can
// exercise code that hardcodes "https://huggingface.co" against a local
// httptest server.
type rewriteHostTransport struct {
	target string
	base   http.RoundTripper
}

func (t rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	targetURL := *req.URL
	parsedTarget, err := url.Parse(t.target)
	if err != nil {
		return nil, err
	}
	targetURL.Scheme = parsedTarget.Scheme
	targetURL.Host = parsedTarget.Host
	req2 := req.Clone(req.Context())
	req2.URL = &targetURL
	req2.Host = parsedTarget.Host
	return t.base.RoundTrip(req2)
}
