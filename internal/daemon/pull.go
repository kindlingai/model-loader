package daemon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
)

// defaultPullWorkers caps how many segments a root fetches from origins at
// once for pull-through. Every pull reuses the source's kept-alive
// connections; one connection per range flooded cluster DNS and conntrack
// in gr8 (kindlingai/gr8#147).
const defaultPullWorkers = 4

// pullTimeout bounds one segment's fetch from the origin. It runs on after
// the request that started it goes away, so other requests waiting on the
// same segment still get it.
const pullTimeout = 10 * time.Minute

// pullCall is one in-flight fetch that later callers wait on.
type pullCall struct {
	done chan struct{}
	val  any
	err  error
}

// once runs fn for key unless a call for key is in flight, in which case
// it waits for that call and returns its result. Two peers asking a root
// for the same range at once cause one fetch from the origin. A call that
// failed only because its own caller went away is not taken as the answer
// for a caller still waiting: that caller runs fn itself.
func (d *Daemon) once(ctx context.Context, key string, fn func() (any, error)) (any, error) {
	for {
		v, err, retry := d.onceTry(ctx, key, fn)
		if !retry {
			return v, err
		}
	}
}

func (d *Daemon) onceTry(ctx context.Context, key string, fn func() (any, error)) (any, error, bool) {
	d.pullMu.Lock()
	if c, ok := d.pulls[key]; ok {
		d.pullMu.Unlock()
		select {
		case <-c.done:
			if c.err != nil && (errors.Is(c.err, context.Canceled) || errors.Is(c.err, context.DeadlineExceeded)) && ctx.Err() == nil {
				return nil, nil, true
			}
			return c.val, c.err, false
		case <-ctx.Done():
			return nil, ctx.Err(), false
		}
	}
	c := &pullCall{done: make(chan struct{})}
	d.pulls[key] = c
	d.pullMu.Unlock()

	c.val, c.err = fn()
	d.pullMu.Lock()
	delete(d.pulls, key)
	d.pullMu.Unlock()
	close(c.done)
	return c.val, c.err, false
}

// revState returns the one RevisionState this node keeps for m's revision,
// loading it from the store the first time. Everything that fetches into
// a revision shares it, so concurrent fetches never save over each other.
func (d *Daemon) revState(m *store.Manifest) (*store.RevisionState, error) {
	key := want.Key(m.Model, m.Revision)
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if rs := d.states[key]; rs != nil {
		return rs, nil
	}
	rs, err := d.cfg.Store.LoadState(m)
	if err != nil {
		return nil, err
	}
	d.states[key] = rs
	return rs, nil
}

// RevisionState implements server.StateSource: the server reads what is
// held from the state this node keeps in memory, not the state file.
func (d *Daemon) RevisionState(m *store.Manifest) (*store.RevisionState, error) {
	return d.revState(m)
}

// catalogModel finds name@revision in the catalog.
func (d *Daemon) catalogModel(name, revision string) (catalog.Model, bool) {
	for _, m := range d.cfg.Catalog.Models {
		if m.Name == name && m.Revision == revision {
			return m, true
		}
	}
	return catalog.Model{}, false
}

// Manifest implements server.Puller. A root that has no manifest for a
// catalog model publishes one from the origin's file list, with no segment
// hashes (see store.FileManifest.Hashed), so that peers can fetch parts of
// it through this root. A model it wants whole is published the same way,
// before it is downloaded: peers fetch each segment as soon as the root
// holds it, or have the root pull it through first.
func (d *Daemon) Manifest(ctx context.Context, name, revision string) (*store.Manifest, error) {
	m, ok := d.catalogModel(name, revision)
	if !ok || !d.IsWANEligible() {
		return nil, server.ErrNotPullable
	}
	if mf, err := d.cfg.Store.LoadManifest(name, revision); err == nil {
		return mf, nil
	}
	v, err := d.once(ctx, "manifest "+want.Key(name, revision), func() (any, error) {
		if mf, err := d.cfg.Store.LoadManifest(name, revision); err == nil {
			return mf, nil
		}
		// The asking peer gives up at its manifestTimeout, and a slow
		// origin may take longer. Bound to the asker, the build would start
		// over at each asking and never finish, so it runs on its own clock
		// and is saved for the next.
		bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), lazyManifestTimeout)
		defer cancel()
		return d.publishLazyManifest(bctx, m)
	})
	if err != nil {
		return nil, err
	}
	return v.(*store.Manifest), nil
}

// lazyManifestTimeout bounds building a manifest from the origin's file
// list, which no asking peer's going away ends.
const lazyManifestTimeout = 5 * time.Minute

func (d *Daemon) publishLazyManifest(ctx context.Context, m catalog.Model) (*store.Manifest, error) {
	src, err := d.cfg.Sources.For(m)
	if err != nil {
		return nil, err
	}
	resolved, err := src.Resolve(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", m.Name, err)
	}
	if len(resolved.Files) == 0 {
		return nil, fmt.Errorf("source for %s resolved no files", m.Name)
	}
	mf := &store.Manifest{Model: m.Name, Revision: m.Revision, Commit: resolved.Commit}
	for _, rf := range resolved.Files {
		fm := store.FileManifest{Path: rf.Path, Size: rf.Size, SHA256: rf.SHA256, SegmentSize: d.segmentSizeFor(m, rf.Path)}
		if fm.Size == 0 && fm.SHA256 == "" {
			fm.SHA256 = emptySHA256 // nothing to fetch; its digest is known.
		}
		mf.Files = append(mf.Files, fm)
	}
	if err := mf.Validate(); err != nil {
		return nil, err
	}
	if err := d.cfg.Store.SaveManifest(mf); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}
	return mf, nil
}

// emptySHA256 is the sha256 of no bytes.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// maxCheckSpan bounds one consumer checksum's range, since a root fetches
// every segment a check spans before it serves any of them. An expert half
// in gr8 is a few MiB.
const maxCheckSpan = 1 << 30

func (d *Daemon) segmentSize() int64 {
	if d.cfg.SegmentSize > 0 {
		return d.cfg.SegmentSize
	}
	return store.DefaultSegmentSize
}

// segmentSizeFor is the segment size this root gives the file at path of
// m in a manifest it makes: the catalog's rule for it, else the default.
func (d *Daemon) segmentSizeFor(m catalog.Model, path string) int64 {
	if size := m.SegmentSizeFor(path); size > 0 {
		return size
	}
	return d.segmentSize()
}

// Pull implements server.Puller: it fetches the segments of path that
// [start, end) touches from the origin, unless they are held already, and
// verifies checks against the held bytes. A check that fails on a segment
// fetched from the origin drops it and fetches it once more, in case it
// was damaged on the way; failing again, it is an error.
func (d *Daemon) Pull(ctx context.Context, name, revision, path string, start, end int64, checks []store.RangeCheck) error {
	m, ok := d.catalogModel(name, revision)
	if !ok || !d.IsWANEligible() {
		return server.ErrNotPullable
	}
	mf, err := d.Manifest(ctx, name, revision)
	if err != nil {
		return err
	}
	fm, ok := mf.Find(path)
	if !ok {
		return server.ErrNotPullable
	}
	if start < 0 || end > fm.Size || start >= end {
		return fmt.Errorf("pull %s: range %d-%d outside the file's %d bytes", path, start, end, fm.Size)
	}
	segs := make([]bool, fm.NumSegments())
	mark := func(start, end int64) {
		for i := fm.SegmentIndex(start); i <= fm.SegmentIndex(end-1); i++ {
			segs[i] = true
		}
	}
	mark(start, end)
	// A check that spans past the range still covers it: fetch every
	// segment it spans, so it can be checked here before anything is
	// served.
	var used []store.RangeCheck
	for _, c := range checks {
		if !c.Overlaps(start, end) {
			continue
		}
		if c.Length > maxCheckSpan || c.Offset+c.Length > fm.Size {
			return fmt.Errorf("pull %s: check offset %d length %d is outside the file or longer than %d bytes", path, c.Offset, c.Length, maxCheckSpan)
		}
		mark(c.Offset, c.Offset+c.Length)
		used = append(used, c)
	}
	rs, err := d.revState(mf)
	if err != nil {
		return err
	}
	// Every segment the pull needs, the range and what its checks span, is
	// in use until it is over, so making room for it cannot evict one of
	// them.
	defer d.useSegments(m.Name, m.Revision, fm, segs)()
	release, err := d.makeRoom(ctx, segmentNeeds(mf, rs, fm, segs, d.blockSize()))
	if err != nil {
		return err
	}
	defer release()
	return d.pullFile(ctx, m, mf, fm, segs, used)
}

// pullFile fetches the segments of fm that segs marks and this node does
// not hold, from a LAN node that holds them if there is one, else from the
// origin. Then it applies checks and completes the file if it is whole. A
// check or whole-file check that fails drops what it covers; that is
// fetched once more before pullFile gives up.
func (d *Daemon) pullFile(ctx context.Context, m catalog.Model, mf *store.Manifest, fm store.FileManifest, segs []bool, checks []store.RangeCheck) error {
	rs, err := d.revState(mf)
	if err != nil {
		return err
	}
	srcs := d.pullSources(ctx, m, mf)
	for attempt := 0; ; attempt++ {
		if err := d.pullSegments(ctx, m, fm, rs, segs, srcs); err != nil {
			return err
		}
		f, err := d.cfg.Store.EnsureFile(m.Name, m.Revision, fm)
		if err != nil {
			return err
		}
		err = transfer.ApplyChecks(f, fm, rs, checks)
		save := d.cfg.Store.SaveState
		if fm.Small() && err == nil {
			save = d.cfg.Store.SaveStateSoon // see transfer.Options.SaveSoon
		}
		if serr := save(rs); err == nil {
			err = serr
		}
		if err == nil {
			err = transfer.FinishFile(d.cfg.Store, f, fm, rs)
		}
		f.Close()
		if err == nil || attempt == 1 {
			return err
		}
		// What failed may have come from a LAN node with a bad copy: get
		// it from the origin this time.
		srcs = d.originSources(atCommit(m, mf))
	}
}

// pullSourcesTTL is how long a root reuses what LAN nodes reported holding
// for its pulls, so a burst of peer requests does not ask every node again
// for each segment.
const pullSourcesTTL = 10 * time.Second

type cachedSources struct {
	srcs *sources
	at   time.Time
}

// pullSources returns m's sources for pull-through: LAN nodes, asked with
// server.HeaderNoPull so they serve only what they hold, then the origin.
func (d *Daemon) pullSources(ctx context.Context, m catalog.Model, mf *store.Manifest) *sources {
	key := want.Key(m.Name, m.Revision)
	d.pullMu.Lock()
	c, ok := d.pullSrcs[key]
	d.pullMu.Unlock()
	if ok && time.Since(c.at) < pullSourcesTTL {
		return c.srcs
	}
	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err != nil {
		peers = nil
	}
	srcs := d.buildSources(ctx, m, mf, peers)
	srcs.holdersOnly = true
	d.pullMu.Lock()
	d.pullSrcs[key] = cachedSources{srcs, time.Now()}
	d.pullMu.Unlock()
	return srcs
}

// atCommit returns m with its revision replaced by the commit mf resolved
// it to, if any, so what is fetched from the origin is of that commit even
// after a branch moves.
func atCommit(m catalog.Model, mf *store.Manifest) catalog.Model {
	if mf != nil && mf.Commit != "" {
		m.Revision = mf.Commit
	}
	return m
}

// originSources returns m's origin as its only source.
func (d *Daemon) originSources(m catalog.Model) *sources {
	s := &sources{holdersOnly: true}
	if src, err := d.cfg.Sources.For(m); err == nil {
		s.fetchers = append(s.fetchers, transfer.WANFetcher{Source: src, Model: m})
	}
	return s
}

// pullSegments fetches the segments segs marks that rs does not hold:
// one fetch per segment however many callers want it, at most
// Config.PullWorkers of this call's chunks in flight, and at most that
// many across the node (pullLimit). Each segment a call fetches is
// claimed first (see claimSegments); segments another call is fetching
// are waited for, not fetched again. A call's claimed segments of one
// chunk go as one fetch, in as few ranges as the sources allow (see
// transfer.FetchFile), so callers wanting disjoint segments of one chunk
// fetch them in parallel.
func (d *Daemon) pullSegments(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, segs []bool, srcs *sources) error {
	file := claimKey(m, fm)
	// Each round fetches what this call claimed and waits for the rest.
	// What another call failed to fetch is claimed by this one next
	// round; a fetch of this call's own that fails ends it.
	for {
		mine, theirs := d.claimSegments(file, fm, rs, segs)
		if mine == nil && theirs == nil {
			return nil
		}
		if mine != nil {
			err := d.fetchClaimed(ctx, m, fm, rs, mine, srcs)
			d.releaseSegments(file, mine)
			if err != nil {
				return err
			}
		}
		for _, ch := range theirs {
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// claimKey keys the claims on the segments of m's file fm.
func claimKey(m catalog.Model, fm store.FileManifest) string {
	return want.Key(m.Name, m.Revision) + "\x00" + fm.Path
}

// pullClaim is one segment a call is fetching: done is closed when it
// is over, held or not.
type pullClaim struct{ done chan struct{} }

// claimSegments claims for the caller the segments segs marks that rs
// does not hold and no other call is fetching (mine, one bool per
// segment, nil if none), and returns the done channels of those another
// call is fetching (theirs).
func (d *Daemon) claimSegments(file string, fm store.FileManifest, rs *store.RevisionState, segs []bool) (mine []bool, theirs []chan struct{}) {
	d.pullMu.Lock()
	defer d.pullMu.Unlock()
	if d.claims == nil {
		d.claims = make(map[string]map[int]*pullClaim)
	}
	held := rs.FileState(fm).SegmentsDone
	claims := d.claims[file]
	for i, w := range segs {
		if !w || (i < len(held) && held[i]) {
			continue
		}
		if c := claims[i]; c != nil {
			theirs = append(theirs, c.done)
			continue
		}
		if claims == nil {
			claims = make(map[int]*pullClaim)
			d.claims[file] = claims
		}
		claims[i] = &pullClaim{done: make(chan struct{})}
		if mine == nil {
			mine = make([]bool, len(segs))
		}
		mine[i] = true
	}
	return mine, theirs
}

// releaseSegments ends the caller's claims on mine.
func (d *Daemon) releaseSegments(file string, mine []bool) {
	d.pullMu.Lock()
	defer d.pullMu.Unlock()
	claims := d.claims[file]
	for i, m := range mine {
		if c := claims[i]; m && c != nil {
			close(c.done)
			delete(claims, i)
		}
	}
	if len(claims) == 0 {
		delete(d.claims, file)
	}
}

// fetchClaimed fetches the segments mine marks, a chunk at a time, at
// most Config.PullWorkers chunks at once.
func (d *Daemon) fetchClaimed(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, mine []bool, srcs *sources) error {
	queue := make(chan []bool)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for range d.cfg.PullWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for need := range queue {
				if err := d.pullFetch(ctx, m, fm, rs, need, srcs); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
loop:
	for c := range fm.NumChunks() {
		first, end := fm.ChunkBounds(c)
		var need []bool
		for i := first; i < end; i++ {
			if mine[i] {
				if need == nil {
					need = make([]bool, fm.NumSegments())
				}
				need[i] = true
			}
		}
		if need == nil {
			continue
		}
		select {
		case queue <- need:
		case <-ctx.Done():
			break loop
		}
	}
	close(queue)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Join(errs...)
}

// pullFetch fetches the segments need marks, at the caller's priority.
func (d *Daemon) pullFetch(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, need []bool, srcs *sources) error {
	// Wait for a slot, at the caller's priority (the first caller's, for
	// a chunk many want), as long as the caller does; once the fetch has
	// started it finishes even if the caller goes away, as others may be
	// waiting on it (see pullTimeout).
	if err := d.pullLimit.Acquire(ctx, server.Priority(ctx)); err != nil {
		return err
	}
	defer d.pullLimit.Release()
	pctx, cancel := context.WithTimeout(transfer.WithNoPull(context.WithoutCancel(ctx)), pullTimeout)
	defer cancel()
	defer d.writing(m.Name, m.Revision, fm)()
	return transfer.FetchFile(pctx, d.cfg.Store, m.Name, m.Revision, fm, rs,
		srcs.fetchers, transfer.Options{Need: need, Order: srcs.order(fm), SaveSoon: true})
}

// lazy reports whether mf has a file with no segment hashes: a manifest a
// root published for pull-through.
func lazy(mf *store.Manifest) bool {
	for _, fm := range mf.Files {
		if !fm.Hashed() {
			return true
		}
	}
	return false
}
