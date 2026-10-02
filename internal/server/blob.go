package server

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/kindlingai/model-loader/internal/store"
)

// handleBlob serves a file, honoring Range requests and refusing to serve
// any byte range that hasn't been verified against the manifest yet: the
// destination file is pre-truncated to its final size as soon as a download
// starts (see store.EnsureFile), so an unverified region may contain zeros
// or stale data rather than real content. 416 means "ask someone else," not
// "this file doesn't exist here."
func (s *Server) handleBlob(w http.ResponseWriter, r *http.Request) {
	model, revision, relPath := r.PathValue("model"), r.PathValue("revision"), r.PathValue("path")

	manifest, err := s.store.LoadManifest(model, revision)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	fm, ok := manifest.Find(relPath)
	if !ok {
		http.NotFound(w, r)
		return
	}

	state, err := s.store.LoadState(manifest)
	if err != nil {
		http.Error(w, "state unavailable", http.StatusInternalServerError)
		return
	}
	fileState := state.FileState(fm)

	start, end, hasRange, err := parseRange(r.Header.Get("Range"), fm.Size)
	if err != nil {
		rangeNotSatisfiable(w, fm.Size)
		return
	}

	if !rangeIsReady(fileState.ReadyRanges(fm), start, end+1) {
		rangeNotSatisfiable(w, fm.Size)
		return
	}

	f, err := os.Open(s.store.FilePath(model, revision, relPath))
	if err != nil {
		http.Error(w, "open failed", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	length := end - start + 1
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	if hasRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fm.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if r.Method == http.MethodHead {
		return
	}
	// io.Copy from an *os.File to a ResponseWriter backed by a plain TCP
	// connection uses sendfile(2) under the hood, so this never buffers
	// file contents on the heap.
	_, _ = io.Copy(w, io.NewSectionReader(f, start, length))
}

func rangeNotSatisfiable(w http.ResponseWriter, size int64) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
}

func rangeIsReady(ranges []store.Range, start, end int64) bool {
	for _, r := range ranges {
		if r.Contains(start, end) {
			return true
		}
	}
	return false
}

// parseRange parses a single-range "Range: bytes=..." header as sent by our
// own transfer client. It deliberately doesn't support multi-range requests
// (not RFC 7233 complete) since this is an internal protocol between
// model-loader nodes, not a general-purpose file server.
func parseRange(header string, size int64) (start, end int64, hasRange bool, err error) {
	if header == "" {
		return 0, size - 1, false, nil
	}
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return 0, 0, false, fmt.Errorf("unsupported range unit in %q", header)
	}
	spec := strings.TrimPrefix(header, prefix)
	if strings.Contains(spec, ",") {
		return 0, 0, false, fmt.Errorf("multiple ranges not supported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("malformed range %q", header)
	}

	if parts[0] == "" {
		// Suffix range "bytes=-N": the last N bytes.
		n, perr := strconv.ParseInt(parts[1], 10, 64)
		if perr != nil || n <= 0 {
			return 0, 0, false, fmt.Errorf("malformed suffix range %q", header)
		}
		start = size - n
		if start < 0 {
			start = 0
		}
		return start, size - 1, true, nil
	}

	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, fmt.Errorf("invalid range start in %q", header)
	}
	if parts[1] == "" {
		return start, size - 1, true, nil
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, false, fmt.Errorf("invalid range end in %q", header)
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true, nil
}
