package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

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

// sortByPriority sets each want's priority to its highest request
// priority and orders wants by it, highest first, keeping catalog order
// among equals. The order is the order models start in; the priority is
// what their transfers ask the node's transfer limit for, so a waiting
// transfer of a higher one is admitted first.
func sortByPriority(wants []want.Want, prio map[string]int) {
	if len(prio) == 0 {
		return
	}
	for i := range wants {
		wants[i].Priority = prio[want.Key(wants[i].Model.Name, wants[i].Model.Revision)]
	}
	sort.SliceStable(wants, func(i, j int) bool { return wants[i].Priority > wants[j].Priority })
}

// Kick records that what the node wants changed, and starts a reconcile
// pass now rather than at the next tick. A pass already running for a
// model read its wants before the change; when it finishes it sees the
// change and wakes the loop again, so a request made mid-pass is not left
// for a whole interval.
func (d *Daemon) Kick() {
	d.wantsGen.Add(1)
	d.wake()
}

// wake starts a reconcile pass now, without recording a change, so passes
// waking the loop do not wake each other without end.
func (d *Daemon) wake() {
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

// waitPoll is how often WaitRequest looks at a request between status
// changes.
const waitPoll = 250 * time.Millisecond

// notify wakes everything waiting on changed.
func (d *Daemon) notify() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.notifyLocked()
}

// PutRequest implements server.Requests.
func (d *Daemon) PutRequest(r request.Request) (server.RequestStatus, error) {
	if d.cfg.Requests == nil {
		return server.RequestStatus{}, errors.New("this node takes no requests")
	}
	if err := request.Resolve(d.cfg.Catalog, &r); err != nil {
		return server.RequestStatus{}, err
	}
	// Under pinMu, so an eviction under way either sees this request's
	// pins or is over before the request is acknowledged.
	d.pinMu.Lock()
	saved, err := d.cfg.Requests.Put(r)
	d.pinMu.Unlock()
	if err != nil {
		return server.RequestStatus{}, err
	}
	d.Kick()
	d.notify()
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
	// Segments land without a status change, so this also looks again
	// every waitPoll.
	tick := time.NewTicker(waitPoll)
	defer tick.Stop()
	for {
		ch := d.changed()
		st, ok := d.RequestStatus(name)
		if !ok || st.State == server.StateReady || st.State == server.StateInvalid {
			return st, ok
		}
		select {
		case <-ch:
		case <-tick.C:
		case <-ctx.Done():
			return d.RequestStatus(name) // as it is now, not as it was
		}
	}
}

// DeleteRequest implements server.Requests.
func (d *Daemon) DeleteRequest(name string) (bool, error) {
	if d.cfg.Requests == nil {
		return false, nil
	}
	d.pinMu.Lock()
	found, err := d.cfg.Requests.Delete(name)
	d.pinMu.Unlock()
	if found {
		d.Kick()
		d.notify()
	}
	return found, err
}

// ListRequests implements server.Requests.
// CatalogModels implements server.Requests: every catalog entry, in
// catalog order.
func (d *Daemon) CatalogModels() []server.CatalogModel {
	out := []server.CatalogModel{}
	if d.cfg.Catalog == nil {
		return out
	}
	for _, m := range d.cfg.Catalog.Models {
		out = append(out, server.CatalogModel{Name: m.Name, Revision: m.Revision, Source: server.CatalogSource{
			Type: string(m.Source.Type), Repo: m.Source.Repo, URL: m.Source.URL}})
	}
	return out
}

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
	need, err := d.resolveRequest(r, m, mf)
	if err != nil {
		st.State, st.Error = server.StateInvalid, err.Error()
		return st
	}
	rs, err := d.revState(mf)
	if err != nil {
		st.Error = err.Error()
		return st
	}
	total, done, ready := d.countRequest(r.Name, mf, need, rs)
	st.BytesTotal, st.BytesDone = total, done
	st.State = server.StateDownloading
	if ready {
		st.State, st.Error = server.StateReady, ""
	}
	return st
}

// resolvedNeed is a request's need, worked out once for the request and
// manifest it was worked out from: a consumer waiting on a request asks
// for its status four times a second.
type resolvedNeed struct {
	name    string
	updated time.Time
	mf      *store.Manifest
	need    *want.Need
	err     error

	// What the need came to against rs at generation gen.
	rs                *store.RevisionState
	gen               uint64
	total, done       int64
	ready, hasCounted bool
}

// resolveRequest resolves r against mf, reusing the last answer for the
// same request version and manifest.
func (d *Daemon) resolveRequest(r request.Request, m catalog.Model, mf *store.Manifest) (*want.Need, error) {
	d.needMu.Lock()
	c := d.needs[r.Name]
	d.needMu.Unlock()
	if c != nil && c.updated.Equal(r.Updated) && sameManifest(c.mf, mf) {
		return c.need, c.err
	}
	need, err := want.Resolve(mf, want.Want{Model: m, Pieces: []catalog.Pieces{r.Pieces}})
	d.needMu.Lock()
	if d.needs == nil {
		d.needs = make(map[string]*resolvedNeed)
	}
	d.needs[r.Name] = &resolvedNeed{name: r.Name, updated: r.Updated, mf: mf, need: need, err: err}
	d.needMu.Unlock()
	return need, err
}

// countRequest returns how much of need rs holds and whether all of it is
// ready, reusing the last count while rs has not changed: over a
// release's segments a count takes tens of ms, and a waiting consumer
// asks four times a second.
func (d *Daemon) countRequest(name string, mf *store.Manifest, need *want.Need, rs *store.RevisionState) (total, done int64, ready bool) {
	gen := rs.Gen()
	d.needMu.Lock()
	c := d.needs[name]
	if c != nil && c.need == need && c.hasCounted && c.rs == rs && c.gen == gen {
		total, done, ready = c.total, c.done, c.ready
		d.needMu.Unlock()
		return
	}
	d.needMu.Unlock()
	total, done = need.Bytes(mf), need.BytesDone(mf, rs)
	ready = need.Satisfied(mf, rs) && !checksPending(mf, need, rs)
	d.needMu.Lock()
	if c != nil && c.need == need {
		c.rs, c.gen, c.total, c.done, c.ready, c.hasCounted = rs, gen, total, done, ready, true
	}
	d.needMu.Unlock()
	return
}

// sameManifest reports whether a and b describe the same files.
func sameManifest(a, b *store.Manifest) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || a.Model != b.Model || a.Revision != b.Revision || a.Commit != b.Commit || len(a.Files) != len(b.Files) {
		return false
	}
	for i := range a.Files {
		x, y := a.Files[i], b.Files[i]
		if x.Path != y.Path || x.Size != y.Size || x.SHA256 != y.SHA256 || x.SegmentSize != y.SegmentSize || len(x.SegmentSHA256) != len(y.SegmentSHA256) {
			return false
		}
	}
	return true
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
// model, or the whole model when w is Whole. It returns why each piece
// left out failed.
func resolveEach(m *store.Manifest, w want.Want) (*want.Need, error) {
	need, err := want.Resolve(m, w)
	if err == nil {
		return need, nil
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
