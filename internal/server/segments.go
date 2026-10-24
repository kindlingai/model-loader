package server

import (
	"net/http"

	"github.com/kindlingai/model-loader/internal/store"
)

// SegmentsResponse is the body of GET /segments/{model}/{revision}: which
// segments of each file this node holds, verified. A file the node holds
// nothing of is left out, and a revision it has no manifest for has no
// files.
type SegmentsResponse struct {
	Model    string `json:"model"`
	Revision string `json:"revision"`
	// Roles are the node's roles, as in /status, so a node choosing
	// sources can prefer a peer to a root with one request.
	Roles []string                `json:"roles"`
	Files map[string]FileSegments `json:"files"`
}

// IsRoot reports whether the node has the root role.
func (s *SegmentsResponse) IsRoot() bool {
	for _, r := range s.Roles {
		if r == "root" {
			return true
		}
	}
	return false
}

// FileSegments is one file's held segments, as half-open ranges of segment
// indices: [[0, 4], [9, 10]] holds segments 0-3 and 9.
type FileSegments struct {
	Segments int      `json:"segments"`
	Held     [][2]int `json:"held"`
}

// Has reports whether segment i is held.
func (f FileSegments) Has(i int) bool {
	for _, r := range f.Held {
		if i >= r[0] && i < r[1] {
			return true
		}
	}
	return false
}

// Valid reports whether r is an answer about model@revision whose files
// are all in m, with held ranges inside each file's segments. A node
// choosing sources ignores an answer that is not.
func (r *SegmentsResponse) Valid(m *store.Manifest) bool {
	if r.Model != m.Model || r.Revision != m.Revision {
		return false
	}
	segs := make(map[string]int, len(m.Files))
	for _, fm := range m.Files {
		segs[fm.Path] = fm.NumSegments()
	}
	for path, fs := range r.Files {
		if n, ok := segs[path]; !ok || fs.Segments != n {
			return false
		}
		for _, h := range fs.Held {
			if h[0] < 0 || h[0] >= h[1] || h[1] > fs.Segments {
				return false
			}
		}
	}
	return true
}

// HeldRanges packs done into half-open index ranges.
func HeldRanges(done []bool) [][2]int {
	var out [][2]int
	for i := 0; i < len(done); i++ {
		if !done[i] {
			continue
		}
		j := i
		for j < len(done) && done[j] {
			j++
		}
		out = append(out, [2]int{i, j})
		i = j
	}
	return out
}

func (s *Server) handleSegments(w http.ResponseWriter, r *http.Request) {
	model, revision := r.PathValue("model"), r.PathValue("revision")
	resp := SegmentsResponse{Model: model, Revision: revision, Roles: s.status.Status().Roles, Files: map[string]FileSegments{}}
	m, err := s.store.LoadManifest(model, revision)
	if err != nil {
		// No manifest: this node holds nothing of it. That is an answer,
		// not a 404, so a 404 means only "too old to have /segments".
		writeJSON(w, http.StatusOK, resp)
		return
	}
	rs, err := s.store.LoadState(m)
	if err != nil {
		http.Error(w, "state unavailable", http.StatusInternalServerError)
		return
	}
	for _, fm := range m.Files {
		done := rs.FileState(fm).SegmentsDone
		if len(done) > fm.NumSegments() {
			done = done[:fm.NumSegments()] // stale state; see transfer.FetchFile.
		}
		held := HeldRanges(done)
		if len(held) == 0 {
			continue
		}
		resp.Files[fm.Path] = FileSegments{Segments: fm.NumSegments(), Held: held}
	}
	writeJSON(w, http.StatusOK, resp)
}
