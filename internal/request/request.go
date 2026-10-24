// Package request keeps the named requests local consumers make: "hold
// these pieces of this model for me". gr8 works out which pieces a rank
// needs (g6load fetch-plan) and puts that as a request; model-loader
// fetches what is missing and keeps every piece a request names until the
// request is released.
//
// Requests are saved one file each under the store, so they outlive a
// restart of model-loader.
package request

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
)

// Request is one named request: the pieces of one catalog model revision a
// local consumer needs.
type Request struct {
	// Name identifies the request on this node. Putting a request with a
	// name that exists replaces it.
	Name string `json:"name"`
	catalog.Pieces
	// Priority orders requests that compete for transfers: a higher one
	// starts first. Zero, the default, is the selection file's priority.
	Priority int `json:"priority,omitempty"`

	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidName reports whether name may name a request: 1 to 128 letters,
// digits, '.', '_' and '-', starting with a letter or digit. A name is a
// file name in the store, so nothing else is allowed.
func ValidName(name string) bool { return nameRE.MatchString(name) }

// ErrInvalid wraps every error Put returns for a request that is not well
// formed or names nothing in the catalog.
var ErrInvalid = errors.New("invalid request")

// Resolve checks r against c and fills in its revision when it names
// none and the catalog has exactly one revision of its model. It does not
// check r against a manifest: a range past the end of its file is found
// when the manifest is known.
func Resolve(c *catalog.Catalog, r *Request) error {
	if !ValidName(r.Name) {
		return fmt.Errorf("%w: name %q: want 1-128 of [A-Za-z0-9._-], starting with a letter or digit", ErrInvalid, r.Name)
	}
	if err := r.Pieces.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	var revs []string
	for _, m := range c.Models {
		if m.Name == r.Model {
			revs = append(revs, m.Revision)
		}
	}
	switch {
	case len(revs) == 0:
		return fmt.Errorf("%w: no catalog model %s", ErrInvalid, r.Model)
	case r.Revision == "" && len(revs) == 1:
		r.Revision = revs[0]
	case r.Revision == "":
		return fmt.Errorf("%w: model %s has revisions %s in the catalog; name one", ErrInvalid, r.Model, strings.Join(revs, ", "))
	default:
		found := false
		for _, rev := range revs {
			found = found || rev == r.Revision
		}
		if !found {
			return fmt.Errorf("%w: no catalog model %s@%s", ErrInvalid, r.Model, r.Revision)
		}
	}
	return nil
}

// Registry holds a node's requests, in memory and on disk.
type Registry struct {
	dir string

	mu   sync.Mutex
	reqs map[string]Request
}

// Open loads the requests saved in dir, creating it if needed. A file that
// does not parse is skipped, and reported in the error alongside the
// registry of every other request.
func Open(dir string) (*Registry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("open requests: %w", err)
	}
	r := &Registry{dir: dir, reqs: make(map[string]Request)}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("open requests: %w", err)
	}
	var errs []error
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok || !ValidName(name) || e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		var req Request
		if err == nil {
			err = json.Unmarshal(data, &req)
		}
		if err == nil && req.Name != name {
			err = fmt.Errorf("names request %q", req.Name)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("request %s: %w", name, err))
			continue
		}
		r.reqs[name] = req
	}
	return r, errors.Join(errs...)
}

// Put saves req, replacing any request with its name, and returns the
// request as saved. The caller resolves it against the catalog first.
func (r *Registry) Put(req Request) (Request, error) {
	if !ValidName(req.Name) {
		return Request{}, fmt.Errorf("%w: name %q", ErrInvalid, req.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	req.Created, req.Updated = now, now
	if old, ok := r.reqs[req.Name]; ok {
		req.Created = old.Created
	}
	data, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return Request{}, err
	}
	if err := writeAtomic(filepath.Join(r.dir, req.Name+".json"), data); err != nil {
		return Request{}, fmt.Errorf("save request %s: %w", req.Name, err)
	}
	r.reqs[req.Name] = req
	return req, nil
}

// Get returns the request named name.
func (r *Registry) Get(name string) (Request, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	req, ok := r.reqs[name]
	return req, ok
}

// Delete releases the request named name. It reports whether there was
// one.
func (r *Registry) Delete(name string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.reqs[name]; !ok {
		return false, nil
	}
	if err := os.Remove(filepath.Join(r.dir, name+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return true, fmt.Errorf("release request %s: %w", name, err)
	}
	if err := syncDir(r.dir); err != nil {
		return true, fmt.Errorf("release request %s: %w", name, err)
	}
	delete(r.reqs, name)
	return true, nil
}

// List returns every request, highest priority first, then by name.
func (r *Registry) List() []Request {
	r.mu.Lock()
	out := make([]Request, 0, len(r.reqs))
	for _, req := range r.reqs {
		out = append(out, req)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// writeAtomic writes data to path through a temporary file and a rename,
// so a crash leaves the old request or the new one, never half of one.
func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".request-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir makes a rename or removal in dir durable, so a request PUT or
// DELETE acknowledged is still so after a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
