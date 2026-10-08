package source

import (
	"context"
	"encoding/hex"
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
	// token, if set, is sent as a bearer token so gated and private repos
	// can be fetched. net/http drops the Authorization header when the Hub
	// redirects a download to its CDN, so it only ever goes to huggingface.co.
	token string
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

func (s *huggingfaceSource) get(ctx context.Context, u string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	return req, nil
}

func (s *huggingfaceSource) Resolve(ctx context.Context, m catalog.Model) (*Resolved, error) {
	u := fmt.Sprintf("https://huggingface.co/api/models/%s/tree/%s?recursive=true",
		m.Source.Repo, url.PathEscape(m.Revision))
	req, err := s.get(ctx, u)
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
			// sha256 separately from the pointer blob itself. For a
			// gated repo the caller has no access to, the Hub masks the
			// sha256 with asterisks; downloads would then fail anyway.
			if !isSHA256(e.LFS.OID) {
				return nil, fmt.Errorf("huggingface source: %s@%s: no sha256 for %s (gated repo: request access on huggingface.co and set HF_TOKEN to a token that has it)", m.Source.Repo, m.Revision, e.Path)
			}
			f.Size = e.LFS.Size
			f.SHA256 = e.LFS.OID
		}
		resolved.Files = append(resolved.Files, f)
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("huggingface source: %s@%s has no files (check the revision exists and the repo is public or HF_TOKEN can read it)", m.Source.Repo, m.Revision)
	}
	return resolved, nil
}

func (s *huggingfaceSource) Open(ctx context.Context, m catalog.Model, path string, offset, length int64) (io.ReadCloser, error) {
	u := joinURLPath(fmt.Sprintf("https://huggingface.co/%s/resolve/%s", m.Source.Repo, m.Revision), path)
	req, err := s.get(ctx, u)
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

func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
