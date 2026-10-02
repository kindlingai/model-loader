// Package server implements the LAN-facing HTTP API every node exposes,
// root or peer alike: /status, /manifest/{model}/{revision}, and
// /blob/{model}/{revision}/{path}. See docs/protocol.md for the wire
// format.
package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/kindlingai/model-loader/internal/store"
)

// ModelStatus is one catalog entry's state as reported by /status.
type ModelStatus struct {
	Model      string    `json:"model"`
	Revision   string    `json:"revision"`
	State      string    `json:"state"` // pending|downloading|ready|error
	BytesTotal int64     `json:"bytes_total"`
	BytesDone  int64     `json:"bytes_done"`
	Source     string    `json:"source,omitempty"`
	Error      string    `json:"error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

const (
	StatePending     = "pending"
	StateDownloading = "downloading"
	StateReady       = "ready"
	StateError       = "error"
)

// StatusResponse is the full body of GET /status.
type StatusResponse struct {
	NodeID string        `json:"node_id"`
	Roles  []string      `json:"roles"`
	Models []ModelStatus `json:"models"`
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
	mux    *http.ServeMux
}

// New builds a Server backed by st for file/manifest data and status for
// the node-wide status it reports.
func New(st *store.Store, status StatusProvider) *Server {
	s := &Server{store: st, status: status}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /manifest/{model}/{revision}", s.handleManifest)
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
