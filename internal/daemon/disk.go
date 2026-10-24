package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/want"
)

// Disk budget and eviction (kindlingai/model-loader#15).
//
// With a budget, a fetch first makes room for what it still has to fetch:
// if the store's held bytes plus what every fetch in flight still needs
// would go over the budget, segments are evicted, least recently used
// first, until it fits. A segment may be evicted only when all of these
// hold:
//
//   - Nothing on this node wants it: no request, and no entry of the
//     selection file, names it. Those pieces are pinned.
//   - It is not being served to a peer just now.
//   - The fleet keeps a copy. A root (one that can fetch the model from its
//     origin) may always evict, as the origin has it. A peer may evict only
//     a segment a root holds, or a peer with a lower node ID holds. Of the
//     peers holding a segment no root holds, the one with the lowest ID
//     never evicts it, so two peers evicting at once cannot both drop the
//     last copy. A peer that does not answer is taken to hold nothing.
//
// Revisions the catalog no longer names are removed whole. Without a
// budget nothing is evicted or removed.

// reservation is a fetch in flight: remaining returns how many bytes it
// still has to fetch.
type reservation struct{ remaining func() int64 }

// victim is a held segment eviction may remove.
type victim struct {
	mf    *store.Manifest
	rs    *store.RevisionState
	fm    store.FileManifest
	i     int
	bytes int64
	used  int64
}

// makeRoom evicts what it must so that a fetch that still has remaining()
// bytes to fetch fits in the budget, along with every other fetch in
// flight. It returns the release to call once the fetch is over, or an
// error if the fetch cannot fit even after evicting everything it may.
func (d *Daemon) makeRoom(ctx context.Context, remaining func() int64) (func(), error) {
	if d.cfg.DiskBudget <= 0 {
		return func() {}, nil
	}
	d.diskMu.Lock()
	defer d.diskMu.Unlock()
	r := &reservation{remaining}
	d.reserved[r] = struct{}{}
	used, err := d.usedBytes()
	if err != nil {
		delete(d.reserved, r)
		return nil, fmt.Errorf("disk budget: %w", err)
	}
	need := d.outstandingLocked()
	if used+need > d.cfg.DiskBudget {
		used = d.evictLocked(ctx, used, need)
	}
	d.setDiskUsed(used)
	if used+need > d.cfg.DiskBudget {
		delete(d.reserved, r)
		return nil, fmt.Errorf("disk budget: %d bytes held and %d to fetch, over the budget of %d bytes; nothing more may be evicted", used, need, d.cfg.DiskBudget)
	}
	return func() {
		d.diskMu.Lock()
		delete(d.reserved, r)
		d.diskMu.Unlock()
	}, nil
}

// outstandingLocked is what every fetch in flight still has to fetch.
// d.diskMu is held.
func (d *Daemon) outstandingLocked() int64 {
	var n int64
	for r := range d.reserved {
		n += max(0, r.remaining())
	}
	return n
}

// evictLocked evicts victims, oldest use first, until used+need fits the
// budget or none is left, and returns the bytes then held. d.diskMu is
// held.
func (d *Daemon) evictLocked(ctx context.Context, used, need int64) int64 {
	vs, err := d.victims(ctx)
	if err != nil {
		log.Printf("model-loader: not evicting: %v", err)
		return used
	}
	for _, v := range vs {
		if used+need <= d.cfg.DiskBudget {
			break
		}
		ok, err := d.evict(v)
		if err != nil {
			log.Printf("model-loader: evict %s@%s %s segment %d: %v", v.mf.Model, v.mf.Revision, v.fm.Path, v.i, err)
		}
		if ok {
			used -= v.bytes
		}
	}
	return used
}

// diskPass runs at every reconcile tick: with a budget, it removes the
// revisions the catalog no longer names, evicts down to the budget (it
// may have been lowered, or a root's pull-through gone over it), and
// counts what could be evicted for /status.
func (d *Daemon) diskPass(ctx context.Context) {
	if d.cfg.DiskBudget > 0 {
		d.removeOld()
	}
	d.diskMu.Lock()
	defer d.diskMu.Unlock()
	used, err := d.usedBytes()
	if err != nil {
		log.Printf("model-loader: disk use: %v", err)
		return
	}
	if d.cfg.DiskBudget <= 0 {
		d.setDisk(server.DiskStatus{UsedBytes: used})
		return
	}
	need := d.outstandingLocked()
	if used+need > d.cfg.DiskBudget {
		used = d.evictLocked(ctx, used, need)
	}
	var evictable int64
	if vs, err := d.victims(ctx); err == nil {
		for _, v := range vs {
			evictable += v.bytes
		}
	}
	d.setDisk(server.DiskStatus{BudgetBytes: d.cfg.DiskBudget, UsedBytes: used, EvictableBytes: evictable})
}

func (d *Daemon) setDisk(ds server.DiskStatus) {
	d.mu.Lock()
	d.disk = &ds
	d.mu.Unlock()
}

func (d *Daemon) setDiskUsed(used int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.disk == nil {
		d.disk = &server.DiskStatus{BudgetBytes: d.cfg.DiskBudget}
	}
	ds := *d.disk
	ds.UsedBytes = used
	d.disk = &ds
}

// usedBytes is what the store holds: every held segment of every
// revision, and the progress of a root's download from an origin of a
// revision it has no manifest for yet.
func (d *Daemon) usedBytes() (int64, error) {
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, ref := range refs {
		mf, err := d.cfg.Store.LoadManifest(ref.Model, ref.Revision)
		if err != nil {
			n, err := d.cfg.Store.OriginBytes(ref.Model, ref.Revision)
			if err != nil {
				return 0, err
			}
			total += n
			continue
		}
		rs, err := d.revState(mf)
		if err != nil {
			return 0, err
		}
		total += rs.BytesDone(mf)
	}
	return total, nil
}

// pins returns, for each revision this node wants, what of it is wanted:
// nil means all of it. It fails if the wants cannot all be worked out;
// then nothing may be evicted, as a pinned piece could look unwanted.
func (d *Daemon) pins() (map[string]*want.Need, error) {
	d.mu.Lock()
	sel := d.cfg.Selection
	d.mu.Unlock()
	sel, _ = d.withRequests(sel)
	wants, err := want.FromSelection(d.cfg.Catalog, sel)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*want.Need, len(wants))
	for _, w := range wants {
		key := want.Key(w.Model.Name, w.Model.Revision)
		mf, err := d.cfg.Store.LoadManifest(w.Model.Name, w.Model.Revision)
		if err != nil {
			out[key] = nil
			continue
		}
		need, err := resolveEach(mf, w)
		if err != nil && !errors.Is(err, errSomePieces) {
			out[key] = nil // keep all of it rather than guess.
			continue
		}
		out[key] = need
	}
	return out, nil
}

// victims lists the held segments this node may evict now, least
// recently used first.
func (d *Daemon) victims(ctx context.Context) ([]victim, error) {
	pins, err := d.pins()
	if err != nil {
		return nil, err
	}
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		return nil, err
	}
	var out []victim
	for _, ref := range refs {
		m, ok := d.catalogModel(ref.Model, ref.Revision)
		if !ok {
			continue // removed whole by removeOld.
		}
		need, pinned := pins[want.Key(ref.Model, ref.Revision)]
		if pinned && need == nil {
			continue
		}
		mf, err := d.cfg.Store.LoadManifest(ref.Model, ref.Revision)
		if err != nil {
			continue
		}
		rs, err := d.revState(mf)
		if err != nil {
			return nil, err
		}
		var mine []victim
		for _, fm := range mf.Files {
			fs := rs.FileState(fm)
			var fn *want.FileNeed
			if pinned {
				fn = need.Files[fm.Path]
			}
			for i, held := range fs.SegmentsDone {
				if !held || i >= fm.NumSegments() || (fn != nil && fn.Segments[i]) {
					continue
				}
				start, end := fm.SegmentBounds(i)
				mine = append(mine, victim{mf: mf, rs: rs, fm: fm, i: i, bytes: end - start, used: fs.LastUsed(i)})
			}
		}
		if len(mine) == 0 {
			continue
		}
		if !d.canRefetch(m) {
			keeps := d.fleetKeeps(ctx, m, mf)
			kept := mine[:0]
			for _, v := range mine {
				if keeps(v.fm.Path, v.i) {
					kept = append(kept, v)
				}
			}
			mine = kept
		}
		out = append(out, mine...)
	}
	sort.SliceStable(out, func(a, b int) bool {
		va, vb := out[a], out[b]
		if va.used != vb.used {
			return va.used < vb.used
		}
		if va.mf.Model != vb.mf.Model || va.mf.Revision != vb.mf.Revision {
			return want.Key(va.mf.Model, va.mf.Revision) < want.Key(vb.mf.Model, vb.mf.Revision)
		}
		if va.fm.Path != vb.fm.Path {
			return va.fm.Path < vb.fm.Path
		}
		return va.i < vb.i
	})
	return out, nil
}

// canRefetch reports whether this node can fetch m from its origin.
func (d *Daemon) canRefetch(m catalog.Model) bool {
	if !d.IsWANEligible() {
		return false
	}
	_, err := d.cfg.Sources.For(m)
	return err == nil
}

// fleetKeeps asks every peer which segments of m it holds, and returns
// whether another node keeps a copy of segment i of a file, so this node
// may drop its own: a root holds it, or a peer with a lower node ID does.
func (d *Daemon) fleetKeeps(ctx context.Context, m catalog.Model, mf *store.Manifest) func(path string, i int) bool {
	var answers []*server.SegmentsResponse
	if peers, err := d.cfg.Discoverer.Peers(ctx); err == nil {
		answers = make([]*server.SegmentsResponse, len(peers))
		var wg sync.WaitGroup
		for k, p := range peers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case d.availSlots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-d.availSlots }()
				qctx, cancel := context.WithTimeout(ctx, availabilityTimeout)
				defer cancel()
				resp, err := d.cfg.Peers.Segments(qctx, p.Addr, m.Name, m.Revision)
				if err == nil && resp.Valid(mf) && resp.NodeID != d.cfg.NodeID {
					answers[k] = resp
				}
			}()
		}
		wg.Wait()
	}
	return func(path string, i int) bool {
		for _, a := range answers {
			if a == nil {
				continue
			}
			fs, ok := a.Files[path]
			if !ok || !fs.Has(i) {
				continue
			}
			if a.IsRoot() || (a.NodeID != "" && a.NodeID < d.cfg.NodeID) {
				return true
			}
		}
		return false
	}
}

// evict removes segment v from the store, unless it is being served or
// is no longer held. It reports whether it did.
func (d *Daemon) evict(v victim) (bool, error) {
	d.useMu.Lock()
	defer d.useMu.Unlock()
	if d.serving[useKey(v.mf.Model, v.mf.Revision, v.fm.Path, v.i)] > 0 {
		return false, nil
	}
	unlock := v.rs.LockFile(v.fm.Path)
	defer unlock()
	if v.rs.SegmentTrust(v.fm, v.i) == "" {
		return false, nil
	}
	v.rs.EvictSegment(v.fm, v.i)
	if err := d.cfg.Store.SaveState(v.rs); err != nil {
		return false, err
	}
	last := true
	for _, held := range v.rs.FileState(v.fm).SegmentsDone {
		last = last && !held
	}
	if err := d.cfg.Store.FreeSegment(v.mf.Model, v.mf.Revision, v.fm, v.i, last); err != nil {
		return true, err // dropped from the state; the space may not be freed.
	}
	return true, nil
}

// removeOld removes every revision in the store the catalog does not
// name: no selection or request can name it, so nothing can want it.
func (d *Daemon) removeOld() {
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		log.Printf("model-loader: list revisions: %v", err)
		return
	}
	for _, ref := range refs {
		if _, ok := d.catalogModel(ref.Model, ref.Revision); ok {
			continue
		}
		prefix := want.Key(ref.Model, ref.Revision) + "\x00"
		d.useMu.Lock()
		busy := false
		for k, n := range d.serving {
			busy = busy || (n > 0 && len(k) > len(prefix) && k[:len(prefix)] == prefix)
		}
		if !busy {
			if err := d.cfg.Store.RemoveRevision(ref.Model, ref.Revision); err != nil {
				log.Printf("model-loader: remove old revision %s@%s: %v", ref.Model, ref.Revision, err)
			} else {
				log.Printf("model-loader: removed %s@%s: the catalog no longer names it", ref.Model, ref.Revision)
				d.stateMu.Lock()
				delete(d.states, want.Key(ref.Model, ref.Revision))
				d.stateMu.Unlock()
			}
		}
		d.useMu.Unlock()
	}
}

func useKey(model, revision, path string, i int) string {
	return fmt.Sprintf("%s\x00%s\x00%d", want.Key(model, revision), path, i)
}

// BeginUse implements server.UseTracker: segments being served are not
// evicted, and serving them counts as a use.
func (d *Daemon) BeginUse(model, revision string, fm store.FileManifest, start, end int64) func() {
	first, last := fm.SegmentIndex(start), fm.SegmentIndex(end-1)
	d.useMu.Lock()
	for i := first; i <= last; i++ {
		d.serving[useKey(model, revision, fm.Path, i)]++
	}
	d.useMu.Unlock()
	d.stateMu.Lock()
	rs := d.states[want.Key(model, revision)]
	d.stateMu.Unlock()
	if rs != nil {
		rs.Touch(fm, first, last)
	}
	return func() {
		d.useMu.Lock()
		defer d.useMu.Unlock()
		for i := first; i <= last; i++ {
			k := useKey(model, revision, fm.Path, i)
			if d.serving[k]--; d.serving[k] <= 0 {
				delete(d.serving, k)
			}
		}
	}
}
