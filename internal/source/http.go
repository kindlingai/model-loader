package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// httpSource fetches a model from a plain HTTP(S) file tree. It expects a
// manifest.json at the source URL describing the files, since plain HTTP has
// no directory listing convention to rely on.
type httpSource struct {
	client *http.Client
}

type httpManifest struct {
	Files []struct {
		Path   string `json:"path"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256,omitempty"`
	} `json:"files"`
}

func (s *httpSource) Resolve(ctx context.Context, m catalog.Model) (*Resolved, error) {
	u := joinURLPath(m.Source.URL, "manifest.json")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http source: GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http source: GET %s: %s", u, resp.Status)
	}

	var hm httpManifest
	if err := json.NewDecoder(resp.Body).Decode(&hm); err != nil {
		return nil, fmt.Errorf("http source: decode manifest from %s: %w", u, err)
	}

	resolved := &Resolved{}
	for _, f := range hm.Files {
		resolved.Files = append(resolved.Files, File{Path: f.Path, Size: f.Size, SHA256: f.SHA256})
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("http source: %s lists no files", u)
	}
	return resolved, nil
}

func (s *httpSource) Open(ctx context.Context, m catalog.Model, path string, offset, length int64) (io.ReadCloser, error) {
	u := joinURLPath(m.Source.URL, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 || length > 0 {
		req.Header.Set("Range", rangeHeader(offset, length))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http source: GET %s: %w", u, err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, nil
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("http source: GET %s: %s", u, resp.Status)
	}
}
