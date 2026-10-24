package server

import (
	"net/http"
)

// SegmentsResponse is the body of GET /segments/{model}/{revision}: which
// segments of each file this node holds, verified. A file the node holds
// nothing of is left out.
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
	m, err := s.store.LoadManifest(model, revision)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	rs, err := s.store.LoadState(m)
	if err != nil {
		http.Error(w, "state unavailable", http.StatusInternalServerError)
		return
	}
	resp := SegmentsResponse{Model: model, Revision: revision, Roles: s.status.Status().Roles, Files: map[string]FileSegments{}}
	for _, fm := range m.Files {
		fs := rs.FileState(fm)
		held := HeldRanges(fs.SegmentsDone)
		if len(held) == 0 {
			continue
		}
		resp.Files[fm.Path] = FileSegments{Segments: fm.NumSegments(), Held: held}
	}
	writeJSON(w, http.StatusOK, resp)
}
