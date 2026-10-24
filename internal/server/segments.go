package server

import (
	"net/http"
	"sort"

	"github.com/kindlingai/model-loader/internal/store"
)

// SegmentsResponse is the body of GET /segments/{model}/{revision}: which
// segments of each file this node holds, verified. A file the node holds
// nothing of is left out, and a revision it has no manifest for has no
// files.
type SegmentsResponse struct {
	Model    string `json:"model"`
	Revision string `json:"revision"`
	// NodeID is the answering node's ID, as in /status. Eviction uses it
	// to decide which of two holders keeps a segment (see docs/design.md).
	NodeID string `json:"node_id,omitempty"`
	// Cataloged says whether the answering node's catalog names the
	// revision; nil from a node too old to say. A root's copy lets a peer
	// evict its own only while this is not false: a root whose catalog
	// dropped the revision cannot fetch it again.
	Cataloged *bool `json:"cataloged,omitempty"`
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

// Has reports whether segment i is held. Held is sorted (see Valid).
func (f FileSegments) Has(i int) bool {
	k := sort.Search(len(f.Held), func(k int) bool { return f.Held[k][1] > i })
	return k < len(f.Held) && f.Held[k][0] <= i
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
		for k, h := range fs.Held {
			if h[0] < 0 || h[0] >= h[1] || h[1] > fs.Segments || (k > 0 && h[0] < fs.Held[k-1][1]) {
				return false // not well formed, or not sorted (see Has).
			}
		}
	}
	return true
}

// HeldRanges packs done into half-open index ranges.
func HeldRanges(done []bool) [][2]int { return store.Runs(done) }

func (s *Server) handleSegments(w http.ResponseWriter, r *http.Request) {
	model, revision := r.PathValue("model"), r.PathValue("revision")
	st := s.status.Status()
	resp := SegmentsResponse{Model: model, Revision: revision, NodeID: st.NodeID, Roles: st.Roles, Files: map[string]FileSegments{}}
	if c, ok := s.status.(Cataloger); ok {
		named := c.Cataloged(model, revision)
		resp.Cataloged = &named
	}
	m, err := s.store.LoadManifest(model, revision)
	if err != nil {
		// No manifest: this node holds nothing of it. That is an answer,
		// not a 404, so a 404 means only "too old to have /segments".
		writeJSON(w, http.StatusOK, resp)
		return
	}
	rs, err := s.revisionState(m)
	if err != nil {
		http.Error(w, "state unavailable", http.StatusInternalServerError)
		return
	}
	for _, fm := range m.Files {
		var held [][2]int
		rs.View(fm, func(fs *store.FileState) {
			done := fs.SegmentsDone
			if len(done) > fm.NumSegments() {
				done = done[:fm.NumSegments()] // stale state; see transfer.FetchFile.
			}
			held = HeldRanges(done)
		})
		if len(held) == 0 {
			continue
		}
		resp.Files[fm.Path] = FileSegments{Segments: fm.NumSegments(), Held: held}
	}
	writeJSON(w, http.StatusOK, resp)
}
