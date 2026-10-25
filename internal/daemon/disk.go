package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/want"
)

// Disk budget and eviction (kindlingai/model-loader#15).
//
// The store's use is the disk space its model files take: the blocks the
// filesystem has allocated to them (store.Allocated). That counts what is
// really on disk, including bytes a failed transfer wrote and space a hole
// punch could not free, and it holds across restarts.
//
// With a budget, a fetch first reserves room for every segment it still
// has to fetch. A reservation is per segment, shared by every fetch of
// that segment. If the store's use plus what every reservation still has
// to fetch would go over the budget, the node frees space until it fits:
//
//  1. Bytes in files nothing is writing that no held segment covers (left
//     by a failed transfer) are freed.
//  2. Held segments are evicted, least recently used first.
//
// A segment may be evicted only when all of these hold:
//
//   - Nothing on this node wants it: no request, and no entry of the
//     selection file, names it. Those pieces are pinned. The pins are read
//     again just before each eviction, under pinMu, which a request change
//     also holds, so a request acknowledged before eviction is never
//     overtaken by it.
//   - Nothing is serving it, nor writing its file, just now.
//   - The fleet keeps a copy. A root (one that can fetch the model from its
//     origin) may always evict, as the origin has it. A peer may evict only
//     a segment a root holds, or a peer with a lower node ID holds. Of the
//     peers holding a segment no root holds, the one with the lowest ID
//     never evicts it, so two peers evicting at once cannot both drop the
//     last copy. A peer that does not answer is taken to hold nothing.
//
// A revision the catalog no longer names, and that no saved request or
// selection entry here names either, is retired: at every tick its
// segments are evicted, whatever the budget, by the same fleet rule
// except that no node can fetch it again, roots included. So a node drops
// a retired segment only if a node with a lower ID holds it, and the
// lowest-ID holder keeps one copy until it is removed by hand: another
// node's catalog may still name it, and with discovery that may miss
// nodes, no node can tell that every catalog in the fleet dropped it.
// Once nothing of it is held here, its files, state and manifest go.
//
// For the same reason a root's copy counts for a peer only while the
// root's catalog names the revision (cataloged in /segments): a root that
// dropped it from its catalog can no longer fetch it again.
//
// Without a budget nothing is evicted or removed.

// reservation is room held for some segments of one chunk (or a root's
// whole download of a revision) that fetches still have to fetch.
type reservation struct {
	refs      int
	remaining func() int64
	chunk     *chunkNeed // nil for a whole download
}

// spaceNeed is one thing a fetch needs room for, keyed so that fetches
// of the same chunk share one reservation.
type spaceNeed struct {
	key       string
	remaining func() int64
	chunk     *chunkNeed
}

// chunkNeed is the segments of one chunk of a file that fetches need,
// held or not. Fetches of the same chunk share one, the union of what
// each needs. d.diskMu guards segs.
type chunkNeed struct {
	rs    *store.RevisionState
	fm    store.FileManifest
	bs    int64
	first int    // the chunk's first segment
	segs  []bool // segs[k]: segment first+k is needed

	// The last count, at rs generation gen; see remaining. d.diskMu.
	counted   bool
	gen, left uint64
}

// remaining is the room the segments not held yet need, in the
// filesystem blocks each touches, bs bytes a block, as the store's use is
// counted.
func (c *chunkNeed) remaining() int64 {
	gen := c.rs.Gen()
	if c.counted && c.gen == gen {
		return int64(c.left)
	}
	var n int64
	defer func() { c.counted, c.gen, c.left = true, gen, uint64(n) }()
	c.rs.View(c.fm, func(fs *store.FileState) {
		for k, want := range c.segs {
			i := c.first + k
			if !want || (i < len(fs.SegmentsDone) && fs.SegmentsDone[i]) {
				continue
			}
			start, end := c.fm.SegmentBounds(i)
			n += roundUp(end, c.bs) - start/c.bs*c.bs
		}
	})
	return n
}

// segmentNeeds is the room the segments segs marks of fm need until they
// are held, one reservation per chunk.
func segmentNeeds(mf *store.Manifest, rs *store.RevisionState, fm store.FileManifest, segs []bool, bs int64) []spaceNeed {
	var out []spaceNeed
	var cur *chunkNeed
	curChunk := -1
	rs.View(fm, func(fs *store.FileState) {
		for i, want := range segs {
			if !want || (i < len(fs.SegmentsDone) && fs.SegmentsDone[i]) {
				continue
			}
			if c := fm.ChunkOf(i); c != curChunk {
				curChunk = c
				first, end := fm.ChunkBounds(c)
				cur = &chunkNeed{rs: rs, fm: fm, bs: bs, first: first, segs: make([]bool, end-first)}
				out = append(out, spaceNeed{key: useKey(mf.Model, mf.Revision, fm.Path, c), remaining: cur.remaining, chunk: cur})
			}
			cur.segs[i-cur.first] = true
		}
	})
	return out
}

// roundUp rounds n up to a multiple of bs.
func roundUp(n, bs int64) int64 {
	return (n + bs - 1) / bs * bs
}

// blockSize returns the store's filesystem block size, looked up once.
func (d *Daemon) blockSize() int64 {
	d.bsOnce.Do(func() { d.bs = d.cfg.Store.BlockSize() })
	return d.bs
}

// victim is what eviction may remove of one chunk of a file (see
// store.ChunkSize; one segment, for a file of 64 MiB segments): its held
// segments segs, in order.
type victim struct {
	mf    *store.Manifest
	rs    *store.RevisionState
	fm    store.FileManifest
	chunk int
	segs  []int
	bytes int64
	used  int64
}

// filter keeps the segments of v keep reports true for, and their bytes.
func (v victim) filter(keep func(i int) bool) victim {
	out := v
	out.segs, out.bytes = nil, 0
	for _, i := range v.segs {
		if keep(i) {
			out.segs = append(out.segs, i)
			start, end := v.fm.SegmentBounds(i)
			out.bytes += end - start
		}
	}
	return out
}

// makeRoom reserves room for needs, freeing space as it must so that the
// store's use plus every reservation fits the budget. It returns the
// release to call once the fetch is over, or an error if the needs cannot
// fit even after freeing everything it may.
func (d *Daemon) makeRoom(ctx context.Context, needs []spaceNeed) (func(), error) {
	if d.cfg.DiskBudget <= 0 || len(needs) == 0 {
		return func() {}, nil
	}
	d.diskMu.Lock()
	defer d.diskMu.Unlock()
	joined := true
	for _, n := range needs {
		r := d.reserved[n.key]
		if r == nil {
			r = &reservation{remaining: n.remaining, chunk: n.chunk}
			d.reserved[n.key] = r
			joined = false
		} else if r.chunk != nil && n.chunk != nil {
			// The reservation covers what every fetch of the chunk needs.
			for k, want := range n.chunk.segs {
				if want && k < len(r.chunk.segs) && !r.chunk.segs[k] {
					r.chunk.segs[k] = true
					r.chunk.counted = false
					joined = false
				}
			}
		}
		r.refs++
	}
	release := func() {
		for _, n := range needs {
			if r := d.reserved[n.key]; r != nil {
				if r.refs--; r.refs <= 0 {
					delete(d.reserved, n.key)
				}
			}
		}
	}
	done := func() {
		d.diskMu.Lock()
		defer d.diskMu.Unlock()
		release()
	}
	if joined {
		// Every segment is being fetched already, and was let in: the
		// bytes it wrote so far are in the store's use and its whole
		// length is still reserved, so checking again would count them
		// twice.
		return done, nil
	}
	used, need, err := d.measureLocked()
	if err == nil && used+need > d.cfg.DiskBudget {
		d.freeLocked(ctx)
		used, need, err = d.measureLocked()
	}
	if err != nil {
		release()
		return nil, fmt.Errorf("disk budget: %w", err)
	}
	d.setDiskUsed(used)
	if used+need > d.cfg.DiskBudget {
		release()
		return nil, fmt.Errorf("disk budget: %d bytes on disk and %d to fetch, over the budget of %d bytes; nothing more may be evicted", used, need, d.cfg.DiskBudget)
	}
	return done, nil
}

// measureLocked returns the store's use and what every reservation still
// has to fetch. What is still to fetch is read first: a segment that lands
// in between is then counted twice, never not at all. d.diskMu is held.
func (d *Daemon) measureLocked() (used, need int64, err error) {
	for _, r := range d.reserved {
		need += max(0, r.remaining())
	}
	used, err = d.cfg.Store.Allocated()
	return used, need, err
}

// fitsLocked reports whether the store's use plus every reservation fits
// the budget. d.diskMu is held.
func (d *Daemon) fitsLocked() bool {
	used, need, err := d.measureLocked()
	return err == nil && used+need <= d.cfg.DiskBudget
}

// freeLocked frees space until the store's use plus every reservation
// fits the budget, or nothing more may be freed: first bytes no held
// segment covers, then victims, oldest use first. d.diskMu is held.
func (d *Daemon) freeLocked(ctx context.Context) {
	d.reclaimLocked()
	if d.fitsLocked() {
		return
	}
	vs, err := d.victims(ctx)
	if err != nil {
		log.Printf("model-loader: not evicting: %v", err)
		return
	}
	// Read the pins again, under pinMu, now that the fleet has answered:
	// a request made meanwhile pins what it names before it is answered.
	d.pinMu.Lock()
	defer d.pinMu.Unlock()
	pins, err := d.pins()
	if err != nil {
		log.Printf("model-loader: not evicting: %v", err)
		return
	}
	used, need, err := d.measureLocked()
	if err != nil {
		return
	}
	over := used + need - d.cfg.DiskBudget
	// Pick victims, oldest first, until their bytes cover what is over,
	// then evict them together: one state save per revision, not one per
	// chunk. Freed space is measured again after, and another batch goes
	// if holes did not free what was counted (a partial block, say).
	for len(vs) > 0 && over > 0 {
		var batch []victim
		var bytes int64
		for len(vs) > 0 && bytes < over {
			v := vs[0]
			vs = vs[1:]
			if v = v.filter(func(i int) bool { return !pinned(pins, v.mf, v.fm, i) }); len(v.segs) > 0 {
				batch = append(batch, v)
				bytes += v.bytes
			}
		}
		for _, err := range d.evictBatch(batch) {
			log.Printf("model-loader: evict: %v", err)
		}
		if used, need, err = d.measureLocked(); err != nil {
			return
		}
		over = used + need - d.cfg.DiskBudget
	}
}

// reclaimLocked frees, in every file nothing is writing, the space under
// segments not held: what a transfer wrote before it failed, or a hole
// punch that failed before. A segment counts as not held only if neither
// the state in memory nor the one saved says it is: one whose drop was
// not saved would be held again after a restart. A segment being served
// is left alone, as the reader may have its bytes still to send.
// d.diskMu is held.
func (d *Daemon) reclaimLocked() {
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		return
	}
	for _, ref := range refs {
		mf, err := d.cfg.Store.LoadManifest(ref.Model, ref.Revision)
		if err != nil {
			continue
		}
		rs, err := d.revState(mf)
		if err != nil {
			continue
		}
		saved, err := d.cfg.Store.LoadState(mf)
		if err != nil {
			continue
		}
		for _, fm := range mf.Files {
			if d.cfg.Store.FileAllocated(mf.Model, mf.Revision, fm.Path) == 0 {
				continue
			}
			// Only a file with more on disk than its held segments take
			// has anything to free. (Allocated includes the blocks a held
			// run's ends share with unheld bytes: none of those can be
			// freed anyway.)
			var heldBytes int64
			rs.View(fm, func(fs *store.FileState) { heldBytes = fs.BytesDone(fm) })
			if d.cfg.Store.FileAllocated(mf.Model, mf.Revision, fm.Path) <= heldBytes {
				continue
			}
			savedHeld := saved.FileState(fm).SegmentsDone
			d.useMu.Lock()
			if d.serving[useKey(mf.Model, mf.Revision, fm.Path, fileUse)] == 0 {
				busy := d.chunksInUseLocked(mf, fm)
				free := make([]bool, fm.NumSegments())
				rs.View(fm, func(fs *store.FileState) {
					for i := range free {
						free[i] = !(i < len(fs.SegmentsDone) && fs.SegmentsDone[i]) && !(i < len(savedHeld) && savedHeld[i]) && !busy[fm.ChunkOf(i)]
					}
				})
				_ = d.cfg.Store.FreeSegments(mf.Model, mf.Revision, fm, store.Runs(free), false)
			}
			d.useMu.Unlock()
		}
	}
}

// diskPass runs at every reconcile tick: with a budget, it evicts what
// it may of retired revisions, frees space down to the budget (it
// may have been lowered), and counts what could be evicted, for /status.
func (d *Daemon) diskPass(ctx context.Context) {
	d.diskMu.Lock()
	defer d.diskMu.Unlock()
	if d.cfg.DiskBudget > 0 {
		d.retireLocked(ctx)
	}
	if d.cfg.DiskBudget > 0 && !d.fitsLocked() {
		d.freeLocked(ctx)
	}
	used, _, err := d.measureLocked()
	if err != nil {
		log.Printf("model-loader: disk use: %v", err)
		return
	}
	if d.cfg.DiskBudget <= 0 {
		d.setDisk(server.DiskStatus{UsedBytes: used})
		return
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

// pinned reports whether pins names segment i of fm.
func pinned(pins map[string]*want.Need, mf *store.Manifest, fm store.FileManifest, i int) bool {
	need, ok := pins[want.Key(mf.Model, mf.Revision)]
	if !ok {
		return false
	}
	if need == nil {
		return true
	}
	fn := need.Files[fm.Path]
	return fn != nil && i < len(fn.Segments) && fn.Segments[i]
}

// victims lists the held segments this node may evict now, least
// recently used first: not pinned, not in use, and kept by the fleet.
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
			continue // see removeOld.
		}
		if need, ok := pins[want.Key(ref.Model, ref.Revision)]; ok && need == nil {
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
			for _, v := range heldChunks(mf, rs, fm) {
				if v = v.filter(func(i int) bool { return !pinned(pins, mf, fm, i) }); len(v.segs) > 0 && !d.inUse(v) {
					mine = append(mine, v)
				}
			}
		}
		if len(mine) == 0 {
			continue
		}
		if !d.canRefetch(m) {
			keeps := d.fleetKeeps(d.askPeers(ctx, m.Name, m.Revision, mf))
			kept := mine[:0]
			for _, v := range mine {
				if v = v.filter(func(i int) bool { return keeps(v.fm.Path, i) }); len(v.segs) > 0 {
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
		if ka, kb := want.Key(va.mf.Model, va.mf.Revision), want.Key(vb.mf.Model, vb.mf.Revision); ka != kb {
			return ka < kb
		}
		if va.fm.Path != vb.fm.Path {
			return va.fm.Path < vb.fm.Path
		}
		return va.chunk < vb.chunk
	})
	return out, nil
}

// heldChunks returns, for each chunk of fm with a segment held, a victim
// of its held segments.
func heldChunks(mf *store.Manifest, rs *store.RevisionState, fm store.FileManifest) []victim {
	var out []victim
	rs.View(fm, func(fs *store.FileState) {
		for c := range fm.NumChunks() {
			first, end := fm.ChunkBounds(c)
			v := victim{mf: mf, rs: rs, fm: fm, chunk: c, used: fs.LastUsed(c)}
			for i := first; i < end && i < len(fs.SegmentsDone); i++ {
				if fs.SegmentsDone[i] {
					v.segs = append(v.segs, i)
					s, e := fm.SegmentBounds(i)
					v.bytes += e - s
				}
			}
			if len(v.segs) > 0 {
				out = append(out, v)
			}
		}
	})
	return out
}

// canRefetch reports whether this node can fetch m from its origin.
func (d *Daemon) canRefetch(m catalog.Model) bool {
	if !d.IsWANEligible() {
		return false
	}
	_, err := d.cfg.Sources.For(m)
	return err == nil
}

// askPeers asks every peer, in parallel, what it holds of model@revision.
// An entry is nil for a peer that did not answer, answered for another
// revision, or (with mf) answered with files or ranges mf does not have;
// this node's own answer is left out.
func (d *Daemon) askPeers(ctx context.Context, model, revision string, mf *store.Manifest) []*server.SegmentsResponse {
	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err != nil {
		return []*server.SegmentsResponse{nil} // unknown: one peer that did not answer.
	}
	answers := make([]*server.SegmentsResponse, len(peers))
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
			resp, err := d.cfg.Peers.Segments(qctx, p.Addr, model, revision)
			if err != nil || resp.Model != model || resp.Revision != revision || (mf != nil && !resp.Valid(mf)) {
				return
			}
			answers[k] = resp
		}()
	}
	wg.Wait()
	out := answers[:0]
	for _, a := range answers {
		if a == nil || a.NodeID != d.cfg.NodeID {
			out = append(out, a)
		}
	}
	return out
}

// fleetKeeps returns whether another node keeps a copy of segment i of a
// file, so this node may drop its own: a root whose catalog names the
// revision (so it can fetch it again) holds it, or a node with a lower ID
// does.
func (d *Daemon) fleetKeeps(answers []*server.SegmentsResponse) func(path string, i int) bool {
	return func(path string, i int) bool {
		for _, a := range answers {
			if a == nil {
				continue
			}
			fs, ok := a.Files[path]
			if !ok || !fs.Has(i) {
				continue
			}
			if (a.IsRoot() && (a.Cataloged == nil || *a.Cataloged)) || (a.NodeID != "" && a.NodeID < d.cfg.NodeID) {
				return true
			}
		}
		return false
	}
}

// lowerHolds returns whether a node with a lower ID than this one holds
// segment i of a file: the rule for retired revisions, which no node can
// fetch again.
func (d *Daemon) lowerHolds(answers []*server.SegmentsResponse) func(path string, i int) bool {
	return func(path string, i int) bool {
		for _, a := range answers {
			if a == nil || a.NodeID == "" || a.NodeID >= d.cfg.NodeID {
				continue
			}
			if fs, ok := a.Files[path]; ok && fs.Has(i) {
				return true
			}
		}
		return false
	}
}

// evict removes the segments of v from the store (see evictBatch). It
// reports whether it dropped any from the state.
func (d *Daemon) evict(v victim) (bool, error) {
	dropped, errs := d.evictBatchN([]victim{v})
	return dropped > 0, errors.Join(errs...)
}

// evictBatch removes the segments of every victim of vs whose chunk is
// not in use, and returns what went wrong.
func (d *Daemon) evictBatch(vs []victim) []error {
	_, errs := d.evictBatchN(vs)
	return errs
}

// evictBatchN removes the held segments of every victim of vs whose chunk
// is not in use; those no longer held are left alone. They are dropped
// from the state in memory, with the chunk marked in use so nothing
// serves or fetches into it meanwhile; each revision's state is saved
// once; then their blocks are freed. A revision whose save fails keeps
// its bytes (the state file was removed, see Store.SaveState). It
// returns how many victims it dropped segments of; the space it frees is
// measured, not assumed.
func (d *Daemon) evictBatchN(vs []victim) (int, []error) {
	type done struct {
		v    victim
		last bool
	}
	var taken []done
	var keys []string
	d.useMu.Lock()
	for _, v := range vs {
		if d.inUseLocked(v) {
			continue
		}
		unlock := v.rs.LockFile(v.fm.Path)
		v = v.filter(func(i int) bool { return v.rs.SegmentTrust(v.fm, i) != "" })
		if len(v.segs) > 0 {
			v.rs.EvictSegments(v.fm, v.segs)
			last := true
			v.rs.View(v.fm, func(fs *store.FileState) {
				for _, held := range fs.SegmentsDone {
					last = last && !held
				}
			})
			taken = append(taken, done{v, last})
			k := useKey(v.mf.Model, v.mf.Revision, v.fm.Path, v.chunk)
			d.serving[k]++
			keys = append(keys, k)
		}
		unlock()
	}
	d.useMu.Unlock()
	defer func() {
		d.useMu.Lock()
		for _, k := range keys {
			if d.serving[k]--; d.serving[k] <= 0 {
				delete(d.serving, k)
			}
		}
		d.useMu.Unlock()
	}()

	var errs []error
	saved := map[*store.RevisionState]error{}
	for _, t := range taken {
		if _, ok := saved[t.v.rs]; !ok {
			saved[t.v.rs] = d.cfg.Store.SaveState(t.v.rs)
			if err := saved[t.v.rs]; err != nil {
				errs = append(errs, fmt.Errorf("%s@%s: save state: %w", t.v.mf.Model, t.v.mf.Revision, err))
			}
		}
	}
	for _, t := range taken {
		if saved[t.v.rs] != nil {
			continue
		}
		v := t.v
		gone := make([]bool, v.fm.NumSegments())
		for _, i := range v.segs {
			gone[i] = true
		}
		// The chunk is marked in use, so nothing writes into it now. A
		// file with nothing held is removed only if nothing writes any
		// of it (useMu, fileUse); else just the hole is punched. If this
		// fails the space stays charged, and reclaimLocked tries again.
		last := t.last
		if last {
			d.useMu.Lock()
			last = d.serving[useKey(v.mf.Model, v.mf.Revision, v.fm.Path, fileUse)] == 0
			if last {
				err := d.cfg.Store.FreeSegments(v.mf.Model, v.mf.Revision, v.fm, store.Runs(gone), true)
				d.useMu.Unlock()
				if err != nil {
					errs = append(errs, fmt.Errorf("%s@%s %s chunk %d: %w", v.mf.Model, v.mf.Revision, v.fm.Path, v.chunk, err))
				}
				continue
			}
			d.useMu.Unlock()
		}
		if err := d.cfg.Store.FreeSegments(v.mf.Model, v.mf.Revision, v.fm, store.Runs(gone), false); err != nil {
			errs = append(errs, fmt.Errorf("%s@%s %s chunk %d: %w", v.mf.Model, v.mf.Revision, v.fm.Path, v.chunk, err))
		}
	}
	return len(taken), errs
}

// retireLocked evicts what it may of every retired revision (see the
// top of this file), and removes one once nothing of it is held here.
// d.diskMu is held.
func (d *Daemon) retireLocked(ctx context.Context) {
	refs, err := d.cfg.Store.Revisions()
	if err != nil {
		log.Printf("model-loader: list revisions: %v", err)
		return
	}
	for _, ref := range refs {
		if d.Cataloged(ref.Model, ref.Revision) || d.namedLocally(ref) {
			continue
		}
		mf, err := d.cfg.Store.LoadManifest(ref.Model, ref.Revision)
		if err != nil {
			continue // a download that never finished; see below.
		}
		rs, err := d.revState(mf)
		if err != nil {
			continue
		}
		lower := d.lowerHolds(d.askPeers(ctx, ref.Model, ref.Revision, mf))
		d.pinMu.Lock() // see freeLocked
		if d.namedLocally(ref) {
			d.pinMu.Unlock()
			continue
		}
		held := false
		var batch []victim
		for _, fm := range mf.Files {
			for _, v := range heldChunks(mf, rs, fm) {
				all := len(v.segs)
				if v = v.filter(func(i int) bool { return lower(fm.Path, i) }); len(v.segs) < all {
					held = true // a segment only this node holds stays.
				}
				if len(v.segs) > 0 {
					batch = append(batch, v)
				}
			}
		}
		dropped, errs := d.evictBatchN(batch)
		for _, err := range errs {
			log.Printf("model-loader: evict retired %s@%s: %v", ref.Model, ref.Revision, err)
		}
		held = held || dropped < len(batch) || len(errs) > 0
		if !held {
			d.useMu.Lock()
			if !d.revisionInUseLocked(ref) {
				var paths []string
				for _, fm := range mf.Files {
					paths = append(paths, fm.Path)
				}
				rs.Close() // no save put off for later may write it back.
				err := d.cfg.Store.RemoveRevision(ref.Model, ref.Revision, paths)
				// Closed, rs saves nothing more: load it again if needed.
				d.stateMu.Lock()
				delete(d.states, want.Key(ref.Model, ref.Revision))
				d.stateMu.Unlock()
				if err != nil {
					log.Printf("model-loader: remove retired %s@%s: %v", ref.Model, ref.Revision, err)
				} else {
					log.Printf("model-loader: removed retired %s@%s: nothing of it is held here", ref.Model, ref.Revision)
				}
			}
			d.useMu.Unlock()
		}
		d.pinMu.Unlock()
	}
}

// namedLocally reports whether a saved request or an entry of the
// selection file names ref, whatever the catalog says: such a revision
// is kept until it is released.
func (d *Daemon) namedLocally(ref store.RevisionRef) bool {
	if d.cfg.Requests != nil {
		for _, r := range d.cfg.Requests.List() {
			if r.Model == ref.Model && r.Revision == ref.Revision {
				return true
			}
		}
	}
	d.mu.Lock()
	sel := d.cfg.Selection
	d.mu.Unlock()
	if sel == nil {
		return false
	}
	if sel.Matches(ref.Model) {
		return true
	}
	for _, p := range sel.Partial {
		if p.Model == ref.Model && (p.Revision == "" || p.Revision == ref.Revision) {
			return true
		}
	}
	return false
}

// fleetRetired reports whether every peer answered and said its catalog
// no longer names the revision.
func (d *Daemon) fleetRetired(answers []*server.SegmentsResponse) bool {
	for _, a := range answers {
		if a == nil || a.Cataloged == nil || *a.Cataloged {
			return false
		}
	}
	return true
}

// Cataloged implements server.Cataloger.
func (d *Daemon) Cataloged(model, revision string) bool {
	_, ok := d.catalogModel(model, revision)
	return ok
}

// chunksInUseLocked returns the chunks of fm being served or fetched.
// d.useMu is held.
func (d *Daemon) chunksInUseLocked(mf *store.Manifest, fm store.FileManifest) map[int]bool {
	prefix := fmt.Sprintf("%s\x00%s\x00", want.Key(mf.Model, mf.Revision), fm.Path)
	busy := map[int]bool{}
	for k, n := range d.serving {
		if rest, ok := strings.CutPrefix(k, prefix); ok && n > 0 {
			if c, err := strconv.Atoi(rest); err == nil {
				busy[c] = true
			}
		}
	}
	return busy
}

// fileUse is the chunk index useKey takes for a whole file: something
// is writing it. Use is counted per chunk (see store.ChunkSize; a segment,
// for a file of 64 MiB segments).
const fileUse = -1

func useKey(model, revision, path string, chunk int) string {
	return fmt.Sprintf("%s\x00%s\x00%d", want.Key(model, revision), path, chunk)
}

// inUse reports whether v is being served or its file written.
func (d *Daemon) inUse(v victim) bool {
	d.useMu.Lock()
	defer d.useMu.Unlock()
	return d.inUseLocked(v)
}

func (d *Daemon) inUseLocked(v victim) bool {
	return d.serving[useKey(v.mf.Model, v.mf.Revision, v.fm.Path, v.chunk)] > 0 ||
		d.serving[useKey(v.mf.Model, v.mf.Revision, v.fm.Path, fileUse)] > 0
}

// revisionInUseLocked reports whether anything of ref is being served or
// written. d.useMu is held.
func (d *Daemon) revisionInUseLocked(ref store.RevisionRef) bool {
	prefix := want.Key(ref.Model, ref.Revision) + "\x00"
	for k, n := range d.serving {
		if n > 0 && strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

// use marks keys in use until the returned function is called.
func (d *Daemon) use(keys []string) func() {
	d.useMu.Lock()
	for _, k := range keys {
		d.serving[k]++
	}
	d.useMu.Unlock()
	return func() {
		d.useMu.Lock()
		defer d.useMu.Unlock()
		for _, k := range keys {
			if d.serving[k]--; d.serving[k] <= 0 {
				delete(d.serving, k)
			}
		}
	}
}

// writing marks fm's file as being written, so eviction leaves the whole
// file alone, until the returned function is called.
func (d *Daemon) writing(model, revision string, fm store.FileManifest) func() {
	return d.use([]string{useKey(model, revision, fm.Path, fileUse)})
}

// useSegments marks the chunks of the segments segs marks of fm in use.
func (d *Daemon) useSegments(model, revision string, fm store.FileManifest, segs []bool) func() {
	var keys []string
	last := -1
	for i, s := range segs {
		if c := fm.ChunkOf(i); s && c != last {
			keys = append(keys, useKey(model, revision, fm.Path, c))
			last = c
		}
	}
	return d.use(keys)
}

// BeginUse implements server.UseTracker: segments being served are not
// evicted, and serving them counts as a use.
func (d *Daemon) BeginUse(model, revision string, fm store.FileManifest, start, end int64) func() {
	first, last := fm.SegmentIndex(start), fm.SegmentIndex(end-1)
	var keys []string
	for c := fm.ChunkOf(first); c <= fm.ChunkOf(last); c++ {
		keys = append(keys, useKey(model, revision, fm.Path, c))
	}
	done := d.use(keys)
	d.stateMu.Lock()
	rs := d.states[want.Key(model, revision)]
	d.stateMu.Unlock()
	if rs != nil {
		rs.Touch(fm, first, last)
	}
	return done
}
