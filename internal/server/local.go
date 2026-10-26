package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/kindlingai/model-loader/internal/request"
)

// RequestStatus is the state of one named request: the request, and how
// much of what it names this node holds.
type RequestStatus struct {
	request.Request
	// State is pending (no manifest yet), downloading, ready (every
	// piece is held and verified), or invalid (the request names a file
	// or range the manifest does not have; it will not become ready).
	State      string `json:"state"`
	BytesTotal int64  `json:"bytes_total"`
	BytesDone  int64  `json:"bytes_done"`
	// Dir is where the revision's files are, by their manifest paths.
	// A piece is at its original offset in its file.
	Dir string `json:"dir"`
	// Error is the last failure fetching the model, or why the request is
	// invalid.
	Error string `json:"error,omitempty"`
}

// StateInvalid is a request's state when the manifest shows it names
// something that does not exist.
const StateInvalid = "invalid"

// Requests is what the local API needs from the daemon.
type Requests interface {
	// PutRequest creates or replaces a request. It returns an error
	// wrapping request.ErrInvalid for one that is not well formed.
	PutRequest(r request.Request) (RequestStatus, error)
	RequestStatus(name string) (RequestStatus, bool)
	// WaitRequest returns once the request is ready or invalid, or ctx
	// ends, with its status then. It reports false if there is no such
	// request.
	WaitRequest(ctx context.Context, name string) (RequestStatus, bool)
	DeleteRequest(name string) (bool, error)
	ListRequests() []RequestStatus
	// CatalogModels lists the catalog's models, so a consumer can find
	// the catalog name of a source it knows (a Hugging Face repo) instead
	// of being told it.
	CatalogModels() []CatalogModel
}

// CatalogModel is one catalog entry as GET /catalog lists it: its name,
// revision and source. Nothing else in the catalog (segment sizes,
// notes) is part of the local API.
type CatalogModel struct {
	Name     string        `json:"name"`
	Revision string        `json:"revision"`
	Source   CatalogSource `json:"source"`
}

// CatalogSource is a catalog entry's origin: type huggingface with repo,
// or type http with url.
type CatalogSource struct {
	Type string `json:"type"`
	Repo string `json:"repo,omitempty"`
	URL  string `json:"url,omitempty"`
}

// maxRequestBody bounds a request's JSON. A fetch plan for one rank of a
// large MoE model lists a few thousand ranges.
const maxRequestBody = 16 << 20

// maxWait bounds ?wait=.
const maxWait = 30 * time.Minute

// NewLocal returns the handler of the local request API. It is for
// consumers on this node, such as gr8, and is served on its own listener
// (a unix socket by default), apart from the LAN API.
func NewLocal(reqs Requests) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /catalog", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"models": reqs.CatalogModels()})
	})
	mux.HandleFunc("GET /requests", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, reqs.ListRequests())
	})
	mux.HandleFunc("PUT /requests/{name}", func(w http.ResponseWriter, r *http.Request) {
		var req request.Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		dec.DisallowUnknownFields()
		err := dec.Decode(&req)
		if err == nil {
			// Exactly one JSON object: anything after it, even past the
			// size limit, is an error.
			if _, extra := dec.Token(); extra != io.EOF {
				err = errors.New("more than one JSON value")
			}
		}
		if err != nil {
			http.Error(w, "bad request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		name := r.PathValue("name")
		if req.Name != "" && req.Name != name {
			http.Error(w, "body names request "+req.Name+", path "+name, http.StatusBadRequest)
			return
		}
		req.Name = name
		st, err := reqs.PutRequest(req)
		switch {
		case errors.Is(err, request.ErrInvalid):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			writeJSON(w, http.StatusOK, st)
		}
	})
	mux.HandleFunc("GET /requests/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		wait := r.URL.Query().Get("wait")
		if wait == "" {
			st, ok := reqs.RequestStatus(name)
			if !ok {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, http.StatusOK, st)
			return
		}
		d, err := time.ParseDuration(wait)
		if err != nil || d < 0 {
			http.Error(w, "wait: want a duration such as 30s", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), min(d, maxWait))
		defer cancel()
		st, ok := reqs.WaitRequest(ctx, name)
		switch {
		case !ok:
			http.NotFound(w, r)
		case st.State == StateReady:
			writeJSON(w, http.StatusOK, st)
		case st.State == StateInvalid:
			writeJSON(w, http.StatusUnprocessableEntity, st)
		default:
			writeJSON(w, http.StatusGatewayTimeout, st) // not ready in time
		}
	})
	mux.HandleFunc("DELETE /requests/{name}", func(w http.ResponseWriter, r *http.Request) {
		found, err := reqs.DeleteRequest(r.PathValue("name"))
		switch {
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		case !found:
			http.NotFound(w, r)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	return mux
}

// WithToken returns h, answering 401 to any request that does not carry
// "Authorization: Bearer token". An empty token returns h as it is.
func WithToken(h http.Handler, token string) http.Handler {
	if token == "" {
		return h
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}
