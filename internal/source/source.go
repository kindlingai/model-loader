// Package source resolves a catalog model into a concrete file list and
// fetches file content from its origin. Each catalog.SourceType has one
// implementation here; the transfer engine only ever talks to the Source
// interface, never to a specific origin's API directly.
package source

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// File is one file as described by an origin, before any bytes have been
// fetched.
type File struct {
	Path string
	Size int64
	// SHA256 is the origin's own content digest, when it provides one
	// (e.g. a Hugging Face LFS file's oid). Empty means the origin didn't
	// tell us — the transfer engine still computes and records a digest
	// itself as it downloads, so resumability and LAN re-serving work
	// either way; an origin-provided hash is simply cross-checked when
	// available rather than being required.
	SHA256 string
}

// Resolved is a model's file list as learned from its origin.
type Resolved struct {
	Files []File
}

// Source fetches model data from one kind of origin (HTTP, Hugging Face,
// ...). A node only ever constructs a Source when it is in root mode and
// needs to WAN-fetch something no LAN peer has yet.
type Source interface {
	// Resolve returns the file list for a model/revision.
	Resolve(ctx context.Context, m catalog.Model) (*Resolved, error)
	// Open returns a reader for byte range [offset, offset+length) of one
	// file. length <= 0 means "to EOF".
	Open(ctx context.Context, m catalog.Model, path string, offset, length int64) (io.ReadCloser, error)
}

// New builds the Source implementation for a catalog model's source type.
// hfToken, if non-empty, authenticates Hugging Face requests (gated or
// private repos).
func New(client *http.Client, hfToken string) *Registry {
	if client == nil {
		client = defaultClient()
	}
	return &Registry{
		http: &httpSource{client: client},
		hf:   &huggingfaceSource{client: client, token: hfToken},
	}
}

// Registry dispatches to the right Source for a given catalog.SourceType.
type Registry struct {
	http *httpSource
	hf   *huggingfaceSource
}

// For returns the Source for m's source type.
func (r *Registry) For(m catalog.Model) (Source, error) {
	switch m.Source.Type {
	case catalog.SourceHTTP:
		return r.http, nil
	case catalog.SourceHuggingFace:
		return r.hf, nil
	default:
		return nil, fmt.Errorf("source: unknown source type %q", m.Source.Type)
	}
}

// rangeHeader formats a Range header value. length <= 0 means to EOF.
func rangeHeader(offset, length int64) string {
	if length <= 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// joinURLPath appends a slash-separated relative path to a base URL,
// percent-escaping each segment so paths with spaces or other reserved
// characters survive the round trip.
func joinURLPath(base, relPath string) string {
	segments := strings.Split(relPath, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.TrimRight(base, "/") + "/" + strings.Join(segments, "/")
}

func defaultClient() *http.Client {
	return &http.Client{
		// No overall request timeout: file downloads can legitimately run
		// for a long time. Connection establishment still times out via
		// the transport below.
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
}
