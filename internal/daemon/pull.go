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
// for the same range at once cause one fetch from the origin.
func (d *Daemon) once(ctx context.Context, key string, fn func() (any, error)) (any, error) {
	d.pullMu.Lock()
	if c, ok := d.pulls[key]; ok {
		d.pullMu.Unlock()
		select {
		case <-c.done:
			return c.val, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
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
	return c.val, c.err
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

// catalogModel finds name@revision in the catalog.
func (d *Daemon) catalogModel(name, revision string) (catalog.Model, bool) {
	for _, m := range d.cfg.Catalog.Models {
		if m.Name == name && m.Revision == revision {
			return m, true
		}
	}
	return catalog.Model{}, false
}

// wantsWhole reports whether the selection names m whole.
func (d *Daemon) wantsWhole(m catalog.Model) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cfg.Selection != nil && d.cfg.Selection.Matches(m.Name)
}

// Manifest implements server.Puller. A root that has no manifest for a
// catalog model it does not want whole publishes one from the origin's
// file list, with no segment hashes (see store.FileManifest.Hashed), so
// that peers can fetch parts of it through this root. A model it wants
// whole is downloaded whole first, as before, and published only then.
func (d *Daemon) Manifest(ctx context.Context, name, revision string) (*store.Manifest, error) {
	m, ok := d.catalogModel(name, revision)
	if !ok || !d.IsWANEligible() {
		return nil, server.ErrNotPullable
	}
	if mf, err := d.cfg.Store.LoadManifest(name, revision); err == nil {
		return mf, nil
	}
	if d.wantsWhole(m) {
		return nil, server.ErrNotPullable
	}
	v, err := d.once(ctx, "manifest "+want.Key(name, revision), func() (any, error) {
		if mf, err := d.cfg.Store.LoadManifest(name, revision); err == nil {
			return mf, nil
		}
		return d.publishLazyManifest(ctx, m)
	})
	if err != nil {
		return nil, err
	}
	return v.(*store.Manifest), nil
}

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
	mf := &store.Manifest{Model: m.Name, Revision: m.Revision}
	for _, rf := range resolved.Files {
		mf.Files = append(mf.Files, store.FileManifest{Path: rf.Path, Size: rf.Size, SHA256: rf.SHA256, SegmentSize: d.segmentSize()})
	}
	if err := mf.Validate(); err != nil {
		return nil, err
	}
	if err := d.cfg.Store.SaveManifest(mf); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}
	return mf, nil
}

func (d *Daemon) segmentSize() int64 {
	if d.cfg.SegmentSize > 0 {
		return d.cfg.SegmentSize
	}
	return store.DefaultSegmentSize
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
	for i := fm.SegmentIndex(start); i <= fm.SegmentIndex(end-1); i++ {
		segs[i] = true
	}
	var inside []store.RangeCheck
	for _, c := range checks {
		if c.Inside(start, end) {
			inside = append(inside, c)
		}
	}
	return d.pullFile(ctx, m, mf, fm, segs, inside)
}

// pullFile fetches the segments of fm that segs marks from the origin,
// unless they are held, then applies checks and completes the file if it
// is whole.
func (d *Daemon) pullFile(ctx context.Context, m catalog.Model, mf *store.Manifest, fm store.FileManifest, segs []bool, checks []store.RangeCheck) error {
	rs, err := d.revState(mf)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		if err := d.pullSegments(ctx, m, fm, rs, segs); err != nil {
			return err
		}
		f, err := d.cfg.Store.EnsureFile(m.Name, m.Revision, fm)
		if err != nil {
			return err
		}
		err = transfer.ApplyChecks(f, fm, rs, checks)
		if err == nil {
			err = d.cfg.Store.SaveState(rs)
		}
		if err == nil {
			err = transfer.FinishFile(d.cfg.Store, f, fm, rs)
		} else {
			_ = d.cfg.Store.SaveState(rs)
		}
		f.Close()
		if err == nil || attempt == 1 {
			return err
		}
	}
}

// pullSegments fetches the segments segs marks that rs does not hold, at
// most Config.PullWorkers at once across the node, one fetch per segment
// however many callers want it.
func (d *Daemon) pullSegments(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, segs []bool) error {
	var wg sync.WaitGroup
	errs := make([]error, len(segs))
	for i, want := range segs {
		if !want || rs.FileState(fm).TrustOf(i) != "" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = d.pullSegment(ctx, m, fm, rs, i)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (d *Daemon) pullSegment(ctx context.Context, m catalog.Model, fm store.FileManifest, rs *store.RevisionState, i int) error {
	key := fmt.Sprintf("segment %s %s %d", want.Key(m.Name, m.Revision), fm.Path, i)
	_, err := d.once(ctx, key, func() (any, error) {
		if rs.FileState(fm).TrustOf(i) != "" {
			return nil, nil // another call fetched it just now.
		}
		src, err := d.cfg.Sources.For(m)
		if err != nil {
			return nil, err
		}
		// The fetch outlives the request that started it; see pullTimeout.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pullTimeout)
		defer cancel()
		select {
		case d.pullSlots <- struct{}{}:
		case <-pctx.Done():
			return nil, pctx.Err()
		}
		defer func() { <-d.pullSlots }()
		need := make([]bool, fm.NumSegments())
		need[i] = true
		return nil, transfer.FetchFile(pctx, d.cfg.Store, m.Name, m.Revision, fm, rs,
			[]transfer.Fetcher{transfer.WANFetcher{Source: src, Model: m}}, transfer.Options{Need: need})
	})
	return err
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
