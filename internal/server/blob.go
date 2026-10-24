package server

import (
	"errors"
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

	if !s.store.StateSaved(model, revision) {
		// This node found held bytes bad and could not record it: serve
		// nothing from the revision until it can.
		http.Error(w, "state could not be saved", http.StatusServiceUnavailable)
		return
	}
	start, end, hasRange, err := parseRange(r.Header.Get("Range"), fm.Size)
	if err != nil {
		rangeNotSatisfiable(w, fm.Size)
		return
	}
	checks, err := store.ParseChecks(r.Header.Get(HeaderRangeXXH64))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Mark the range in use before reading the state, so eviction either
	// sees the use and keeps the segments, or dropped them from the state
	// first and this answers 416.
	if u, ok := s.status.(UseTracker); ok && fm.Size > 0 {
		defer u.BeginUse(model, revision, fm, start, end+1)()
	}

	state, err := s.revisionState(manifest)
	if err != nil {
		http.Error(w, "state unavailable", http.StatusInternalServerError)
		return
	}

	// A root pulls what it does not hold from the origin, and checks a
	// consumer's checksums before it serves a range it fetched from there.
	if s.puller != nil && r.Header.Get(HeaderNoPull) == "" && fm.Size > 0 && (len(checks) > 0 || !state.RangeReady(fm, start, end+1)) {
		pctx := r.Context()
		if p, err := strconv.Atoi(r.Header.Get(HeaderPriority)); err == nil {
			pctx = WithPriority(pctx, p)
		}
		if err := s.puller.Pull(pctx, model, revision, relPath, start, end+1, checks); err != nil {
			switch {
			case errors.Is(err, ErrNotPullable):
				rangeNotSatisfiable(w, fm.Size)
			case errors.Is(err, store.ErrCheckFailed):
				// The range's bytes are the origin's; the checksum is
				// what is wrong. Say so apart from a failing root.
				http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			default:
				http.Error(w, err.Error(), http.StatusBadGateway)
			}
			return
		}
		if state, err = s.revisionState(manifest); err != nil {
			http.Error(w, "state unavailable", http.StatusInternalServerError)
			return
		}
	}

	if !state.RangeReady(fm, start, end+1) {
		rangeNotSatisfiable(w, fm.Size)
		return
	}
	if fm.NumSegments() > 0 {
		// Say what this node holds for the segments, so a requester whose
		// manifest has no hash for them can verify them. This node's
		// manifest may have hashes (a root holding the full release) where
		// the requester's, from a pull-through root, does not.
		if sums, trusts := s.claims(manifest, fm, state, start, end+1); sums != nil {
			w.Header().Set(HeaderSegmentSHA256, strings.Join(sums, ","))
			w.Header().Set(HeaderSegmentTrust, strings.Join(trusts, ","))
		}
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

// claims returns the sha256 and trust this node holds for each segment of
// [start, end), when it is whole segments, at most MaxClaimSegments, and
// it has a sha256 for every one; nil otherwise.
func (s *Server) claims(m *store.Manifest, fm store.FileManifest, rs *store.RevisionState, start, end int64) ([]string, []string) {
	first, last := fm.SegmentIndex(start), fm.SegmentIndex(end-1)
	n := last - first + 1
	if s0, _ := fm.SegmentBounds(first); s0 != start || n > MaxClaimSegments {
		return nil, nil
	}
	if _, e0 := fm.SegmentBounds(last); e0 != end {
		return nil, nil
	}
	sums, trusts := make([]string, n), make([]string, n)
	complete := false
	rs.View(fm, func(fs *store.FileState) {
		complete = fs.Complete
		for k := range n {
			sums[k], trusts[k] = fs.SegmentHash(fm, first+k), fs.TrustOf(first+k)
		}
	})
	if fm.Small() && !fm.Hashed() {
		side := s.store.SegmentSums(m.Model, m.Revision, fm, first, n)
		for k := range n {
			if sums[k] == "" {
				sums[k] = side[k]
			}
		}
	}
	f := (*os.File)(nil)
	defer func() {
		if f != nil {
			f.Close()
		}
	}()
	for k := range n {
		if sums[k] != "" {
			continue
		}
		if !complete {
			return nil, nil
		}
		// A file verified whole that recorded no segment hash (a root's
		// whole download of a file of small segments): its bytes are the
		// origin's, so hash them now.
		if f == nil {
			var err error
			if f, err = os.Open(s.store.FilePath(m.Model, m.Revision, fm.Path)); err != nil {
				return nil, nil
			}
		}
		h, err := store.HashSegment(f, fm, first+k)
		if err != nil {
			return nil, nil
		}
		sums[k] = h
	}
	return sums, trusts
}

// StateSource, when the StatusProvider implements it, returns the state
// the node keeps in memory for a revision, so a blob request need not read
// the state file. The state in memory never holds a segment before its
// bytes and segment hash are on disk.
type StateSource interface {
	RevisionState(m *store.Manifest) (*store.RevisionState, error)
}

func (s *Server) revisionState(m *store.Manifest) (*store.RevisionState, error) {
	if src, ok := s.status.(StateSource); ok {
		return src.RevisionState(m)
	}
	return s.store.LoadState(m)
}

func rangeNotSatisfiable(w http.ResponseWriter, size int64) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
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
