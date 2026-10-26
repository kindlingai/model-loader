// Package server implements the LAN-facing HTTP API every node exposes,
// root or peer alike: /status, /manifest/{model}/{revision},
// /segments/{model}/{revision} and /blob/{model}/{revision}/{path}. See docs/protocol.md for the wire
// format.
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/kindlingai/model-loader/internal/store"
)

// ModelStatus is one catalog entry's state as reported by /status.
type ModelStatus struct {
	Model    string `json:"model"`
	Revision string `json:"revision"`
	State    string `json:"state"` // pending|downloading|partial|ready|error
	// Selection is "partial" when this node wants only part of the
	// revision. BytesTotal and BytesDone then count the wanted part.
	Selection  string `json:"selection,omitempty"`
	BytesTotal int64  `json:"bytes_total"`
	BytesDone  int64  `json:"bytes_done"`
	// BytesReused is how much of the revision this process copied from a
	// file of the same sha256 the store held already, instead of fetching
	// it (-dedup).
	BytesReused int64     `json:"bytes_reused,omitempty"`
	Source      string    `json:"source,omitempty"`
	Error       string    `json:"error,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

const (
	StatePending     = "pending"
	StateDownloading = "downloading"
	StateReady       = "ready"
	StateError       = "error"
	// StatePartial means every piece a partial selection names is held and
	// verified, but not the whole revision.
	StatePartial = "partial"
)

// SelectionPartial marks a ModelStatus for a partial selection.
const SelectionPartial = "partial"

// StatusResponse is the full body of GET /status.
type StatusResponse struct {
	NodeID string        `json:"node_id"`
	Roles  []string      `json:"roles"`
	Models []ModelStatus `json:"models"`
	// Disk is how the node uses its store's disk; nil on a node too old
	// to say.
	Disk *DiskStatus `json:"disk,omitempty"`
}

// DiskStatus is a node's disk budget and use.
type DiskStatus struct {
	// BudgetBytes is the most the store may hold; 0 means no budget, and
	// nothing is evicted.
	BudgetBytes int64 `json:"budget_bytes"`
	// UsedBytes is what the store holds: every held segment, and a root's
	// download from its origin in progress.
	UsedBytes int64 `json:"used_bytes"`
	// EvictableBytes is what eviction could remove now: held segments
	// nothing on this node wants, that another node keeps a copy of (see
	// docs/design.md). Counted only with a budget, at each reconcile tick.
	EvictableBytes int64 `json:"evictable_bytes"`
}

// Cataloger, when the StatusProvider implements it, says whether the
// node's catalog names a model revision.
type Cataloger interface {
	Cataloged(model, revision string) bool
}

// UseTracker, when the StatusProvider implements it, is told when a range
// starts being served. The function it returns is called when serving
// ends. Eviction keeps a segment that is being served, and removes the
// least recently used first.
type UseTracker interface {
	BeginUse(model, revision string, fm store.FileManifest, start, end int64) (done func())
}

// StatusProvider supplies the current node-wide status. It is implemented by
// the daemon package; server depends only on this narrow interface so it
// never imports daemon.
type StatusProvider interface {
	Status() StatusResponse
}

// Server is the LAN-facing HTTP API.
type Server struct {
	store  *store.Store
	status StatusProvider
	puller Puller // nil unless pull-through is on; see SetPuller
	mux    *http.ServeMux
}

// New builds a Server backed by st for file/manifest data and status for
// the node-wide status it reports.
func New(st *store.Store, status StatusProvider) *Server {
	s := &Server{store: st, status: status}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /manifest/{model}/{revision}", s.handleManifest)
	mux.HandleFunc("GET /segments/{model}/{revision}", s.handleSegments)
	mux.HandleFunc("GET /blob/{model}/{revision}/{path...}", s.handleBlob)
	mux.HandleFunc("HEAD /blob/{model}/{revision}/{path...}", s.handleBlob)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.status.Status())
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	model, revision := r.PathValue("model"), r.PathValue("revision")
	m, err := s.store.LoadManifest(model, revision)
	if err != nil && s.puller != nil {
		m, err = s.puller.Manifest(r.Context(), model, revision)
		if err != nil && !errors.Is(err, ErrNotPullable) {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	if err != nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
