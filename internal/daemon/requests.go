package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
)

// A request is part of what the node wants, like an entry in its
// selection file: every reconcile adds the pieces of every request to the
// selection's. So a replaced request fetches only what the node does not
// hold yet, and the pieces both requests name are kept, not fetched again.
// While a request exists its pieces are wanted, and so pinned: eviction
// (kindlingai/model-loader#15) removes only pieces nothing wants.

// withRequests returns sel with the pieces of every request added, and
// the highest request priority of each model revision.
func (d *Daemon) withRequests(sel *catalog.Selection) (*catalog.Selection, map[string]int) {
	if d.cfg.Requests == nil {
		return sel, nil
	}
	reqs := d.cfg.Requests.List()
	if len(reqs) == 0 {
		return sel, nil
	}
	out := &catalog.Selection{Wanted: sel.Wanted, Partial: append([]catalog.Pieces(nil), sel.Partial...)}
	prio := make(map[string]int)
	for _, r := range reqs {
		out.Partial = append(out.Partial, r.Pieces)
		k := want.Key(r.Model, r.Revision)
		if p, ok := prio[k]; !ok || r.Priority > p {
			prio[k] = r.Priority
		}
	}
	return out, prio
}

// sortByPriority orders wants by their highest request priority, highest
// first, keeping catalog order among equals, so the most urgent model
// starts first and gets the transfer slots first.
func sortByPriority(wants []want.Want, prio map[string]int) {
	if len(prio) == 0 {
		return
	}
	sort.SliceStable(wants, func(i, j int) bool {
		return prio[want.Key(wants[i].Model.Name, wants[i].Model.Revision)] > prio[want.Key(wants[j].Model.Name, wants[j].Model.Revision)]
	})
}

// Kick starts a reconcile pass now rather than at the next tick. A pass
// already running for a model notices the kick when it finishes, and runs
// again, so a request made mid-pass is not left for a whole interval.
func (d *Daemon) Kick() {
	d.kicks.Add(1)
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

// changed returns a channel closed at the next status change.
func (d *Daemon) changed() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.change
}

// notifyLocked wakes everything waiting on changed. d.mu is held.
func (d *Daemon) notifyLocked() {
	close(d.change)
	d.change = make(chan struct{})
}

// PutRequest implements server.Requests.
func (d *Daemon) PutRequest(r request.Request) (server.RequestStatus, error) {
	if d.cfg.Requests == nil {
		return server.RequestStatus{}, errors.New("this node takes no requests")
	}
	if err := request.Resolve(d.cfg.Catalog, &r); err != nil {
		return server.RequestStatus{}, err
	}
	saved, err := d.cfg.Requests.Put(r)
	if err != nil {
		return server.RequestStatus{}, err
	}
	d.Kick()
	return d.requestStatus(saved), nil
}

// RequestStatus implements server.Requests.
func (d *Daemon) RequestStatus(name string) (server.RequestStatus, bool) {
	if d.cfg.Requests == nil {
		return server.RequestStatus{}, false
	}
	r, ok := d.cfg.Requests.Get(name)
	if !ok {
		return server.RequestStatus{}, false
	}
	return d.requestStatus(r), true
}

// WaitRequest implements server.Requests.
func (d *Daemon) WaitRequest(ctx context.Context, name string) (server.RequestStatus, bool) {
	for {
		ch := d.changed()
		st, ok := d.RequestStatus(name)
		if !ok || st.State == server.StateReady || st.State == server.StateInvalid {
			return st, ok
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return st, true
		}
	}
}

// DeleteRequest implements server.Requests.
func (d *Daemon) DeleteRequest(name string) (bool, error) {
	if d.cfg.Requests == nil {
		return false, nil
	}
	found, err := d.cfg.Requests.Delete(name)
	if found {
		d.Kick()
	}
	return found, err
}

// ListRequests implements server.Requests.
func (d *Daemon) ListRequests() []server.RequestStatus {
	out := []server.RequestStatus{}
	if d.cfg.Requests == nil {
		return out
	}
	for _, r := range d.cfg.Requests.List() {
		out = append(out, d.requestStatus(r))
	}
	return out
}

// requestStatus works out how much of r this node holds, from the
// revision's manifest and state.
func (d *Daemon) requestStatus(r request.Request) server.RequestStatus {
	st := server.RequestStatus{
		Request: r, State: server.StatePending,
		Dir: d.cfg.Store.ModelDir(r.Model, r.Revision),
	}
	d.mu.Lock()
	ms, seen := d.statuses[want.Key(r.Model, r.Revision)]
	d.mu.Unlock()
	if seen {
		st.Error = ms.Error
	}
	mf, err := d.cfg.Store.LoadManifest(r.Model, r.Revision)
	if err != nil {
		return st // no manifest yet
	}
	var m catalog.Model
	for _, cm := range d.cfg.Catalog.Models {
		if cm.Name == r.Model && cm.Revision == r.Revision {
			m = cm
		}
	}
	need, err := want.Resolve(mf, want.Want{Model: m, Pieces: []catalog.Pieces{r.Pieces}})
	if err != nil {
		st.State, st.Error = server.StateInvalid, err.Error()
		return st
	}
	rs, err := d.revState(mf)
	if err != nil {
		st.Error = err.Error()
		return st
	}
	st.BytesTotal, st.BytesDone = need.Bytes(mf), need.BytesDone(mf, rs)
	st.State = server.StateDownloading
	if need.Satisfied(mf, rs) && !checksPending(mf, need, rs) {
		st.State, st.Error = server.StateReady, ""
	}
	return st
}

// checksPending reports whether a consumer checksum in need has not yet
// matched the bytes held.
func checksPending(m *store.Manifest, need *want.Need, rs *store.RevisionState) bool {
	for _, fm := range m.Files {
		if fn := need.Files[fm.Path]; fn != nil && len(transfer.PendingChecks(fm, rs, fn.Checks)) > 0 {
			return true
		}
	}
	return false
}

// resolveEach resolves w against m piece by piece, leaving out any piece
// that does not resolve (a range past the end of its file, say), so one
// bad request does not stop the node fetching every other piece of the
// model. It returns why each piece left out failed.
func resolveEach(m *store.Manifest, w want.Want) (*want.Need, error) {
	need, err := want.Resolve(m, w)
	if err == nil || w.Whole {
		return need, err
	}
	var ok []catalog.Pieces
	var errs []error
	for _, p := range w.Pieces {
		if _, err := want.Resolve(m, want.Want{Model: w.Model, Pieces: []catalog.Pieces{p}}); err != nil {
			errs = append(errs, err)
			continue
		}
		ok = append(ok, p)
	}
	w.Pieces = ok
	need, err = want.Resolve(m, w)
	if err != nil {
		return nil, err
	}
	for _, e := range errs {
		log.Printf("model-loader: %s@%s: ignoring a piece: %v", m.Model, m.Revision, e)
	}
	return need, fmt.Errorf("%w: %w", errSomePieces, errors.Join(errs...))
}

// errSomePieces means some pieces of a want were left out; the rest are
// fetched.
var errSomePieces = errors.New("some pieces do not match the manifest")
