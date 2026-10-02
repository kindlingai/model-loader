package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// huggingfaceSource fetches a model from the Hugging Face Hub, using its
// plain HTTP API directly rather than depending on huggingface_hub: the tree
// API for the file list, and the ordinary resolve URL (which supports Range)
// for content.
type huggingfaceSource struct {
	client *http.Client
}

type hfTreeEntry struct {
	Type string `json:"type"`
	Path string `json:"path"`
	Size int64  `json:"size"`
	LFS  *struct {
		OID  string `json:"oid"`
		Size int64  `json:"size"`
	} `json:"lfs,omitempty"`
}

func (s *huggingfaceSource) Resolve(ctx context.Context, m catalog.Model) (*Resolved, error) {
	u := fmt.Sprintf("https://huggingface.co/api/models/%s/tree/%s?recursive=true",
		m.Source.Repo, url.PathEscape(m.Revision))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("huggingface source: GET %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("huggingface source: GET %s: %s", u, resp.Status)
	}

	var entries []hfTreeEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("huggingface source: decode tree for %s@%s: %w", m.Source.Repo, m.Revision, err)
	}

	resolved := &Resolved{}
	for _, e := range entries {
		if e.Type != "file" {
			continue
		}
		f := File{Path: e.Path, Size: e.Size}
		if e.LFS != nil {
			// LFS pointer files carry the real content's size and
			// sha256 separately from the pointer blob itself.
			f.Size = e.LFS.Size
			f.SHA256 = e.LFS.OID
		}
		resolved.Files = append(resolved.Files, f)
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("huggingface source: %s@%s has no files (check the repo is public and the revision exists)", m.Source.Repo, m.Revision)
	}
	return resolved, nil
}

func (s *huggingfaceSource) Open(ctx context.Context, m catalog.Model, path string, offset, length int64) (io.ReadCloser, error) {
	u := joinURLPath(fmt.Sprintf("https://huggingface.co/%s/resolve/%s", m.Source.Repo, m.Revision), path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 || length > 0 {
		req.Header.Set("Range", rangeHeader(offset, length))
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("huggingface source: GET %s: %w", u, err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, nil
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("huggingface source: GET %s: %s", u, resp.Status)
	}
}
