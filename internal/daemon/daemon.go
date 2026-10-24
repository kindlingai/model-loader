// Package daemon implements the reconcile loop that ties the catalog, store,
// source, discovery, and transfer packages together: for every model a node
// wants, make sure its manifest and files exist locally, pulling from LAN
// peers first and falling back to WAN only when this node is root-eligible
// and nobody on the LAN has it yet.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
)

// defaultMaxConcurrentTransfers caps simultaneous file downloads per node.
// home-ops hit real OOMs at 4 concurrent Hugging Face downloads; 2 is a safe
// default for a box that may also be serving an LLM workload.
const defaultMaxConcurrentTransfers = 2

// defaultStallTimeout aborts a download from a model's origin that has
// received no bytes for this long. A CDN connection can stall without
// closing, which would otherwise hang that model's reconcile forever.
const defaultStallTimeout = 2 * time.Minute

// Config configures a Daemon. Store, Catalog, Selection, Sources, and
// Discoverer are required; the rest have sane defaults.
type Config struct {
	NodeID string
	// Roles is the node's run-time mode: some combination of "root" and
	// "peer" (or just one of them). A node is WAN-eligible when Roles
	// contains "root" -- see IsWANEligible.
	Roles []string

	Store      *store.Store
	Catalog    *catalog.Catalog
	Selection  *catalog.Selection
	Sources    *source.Registry
	Discoverer discovery.Discoverer

	// SelectionPath, if set, is re-read at the start of every reconcile
	// pass, so a selection file created or edited after startup takes
	// effect without a restart. A missing file is an empty selection; one
	// that fails to parse leaves Selection unchanged.
	SelectionPath string

	// Peers talks to other nodes' HTTP APIs. Defaults to a PeerClient
	// wrapping http.DefaultClient.
	Peers *transfer.PeerClient

	// MaxConcurrentTransfers bounds simultaneous file downloads across all
	// models. Defaults to defaultMaxConcurrentTransfers.
	MaxConcurrentTransfers int

	// StallTimeout bounds how long a download from a model's origin may go
	// without receiving any bytes. Defaults to defaultStallTimeout.
	StallTimeout time.Duration

	// VerifyOnStart re-hashes every held segment of a model the first time
	// this process reconciles it, and fetches again any that no longer
	// match the manifest.
	VerifyOnStart bool
}

// Daemon runs the reconcile loop and reports status via the StatusProvider
// interface server.Server depends on.
type Daemon struct {
	cfg Config
	sem chan struct{}

	mu       sync.Mutex
	statuses map[string]server.ModelStatus
	visited  map[string]bool
}

// New builds a Daemon from cfg, applying defaults for anything left zero.
func New(cfg Config) *Daemon {
	if cfg.Peers == nil {
		cfg.Peers = transfer.NewPeerClient(nil)
	}
	if cfg.MaxConcurrentTransfers <= 0 {
		cfg.MaxConcurrentTransfers = defaultMaxConcurrentTransfers
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = defaultStallTimeout
	}
	return &Daemon{
		cfg:      cfg,
		sem:      make(chan struct{}, cfg.MaxConcurrentTransfers),
		statuses: make(map[string]server.ModelStatus),
		visited:  make(map[string]bool),
	}
}

// IsWANEligible reports whether this node may fetch catalog entries from
// WAN when no LAN peer has them yet.
func (d *Daemon) IsWANEligible() bool {
	for _, r := range d.cfg.Roles {
		if r == "root" {
			return true
		}
	}
	return false
}

// Status implements server.StatusProvider.
func (d *Daemon) Status() server.StatusResponse {
	d.mu.Lock()
	defer d.mu.Unlock()
	resp := server.StatusResponse{NodeID: d.cfg.NodeID, Roles: d.cfg.Roles, Models: []server.ModelStatus{}}
	for _, ms := range d.statuses {
		resp.Models = append(resp.Models, ms)
	}
	return resp
}

func (d *Daemon) setStatus(ms server.ModelStatus) {
	ms.UpdatedAt = time.Now().UTC()
	d.mu.Lock()
	d.statuses[want.Key(ms.Model, ms.Revision)] = ms
	d.mu.Unlock()
}

// Run reconciles every wanted model immediately, then again every interval,
// until ctx is canceled. Each tick re-reads the selection file and starts
// every wanted model that isn't still busy from an earlier tick, so a model
// whose fetch failed (say, its source peer restarted) retries with freshly
// discovered peers on the next tick instead of waiting for every other
// model's transfer to finish. A failure is passed to onErr, which may be
// called concurrently, and does not stop the loop -- a transient WAN or peer
// failure for one model shouldn't take the whole node out of service.
func (d *Daemon) Run(ctx context.Context, interval time.Duration, onErr func(error)) {
	if onErr == nil {
		onErr = func(error) {}
	}
	var busy sync.Map
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		wants, err := d.refreshWants()
		if err != nil {
			onErr(err)
		}
		for _, w := range wants {
			key := want.Key(w.Model.Name, w.Model.Revision)
			if _, running := busy.LoadOrStore(key, true); running {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer busy.Delete(key)
				if err := d.reconcileModel(ctx, w); err != nil {
					onErr(fmt.Errorf("model %s: %w", key, err))
				}
			}()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Reconcile runs one pass over every catalog entry this node's Selection
// wants, bringing each one's manifest and files up to date. Models are
// reconciled concurrently (resolving a manifest and checking peer status is
// cheap); actual file transfers are bounded by MaxConcurrentTransfers
// regardless of how many models are in flight.
func (d *Daemon) Reconcile(ctx context.Context) error {
	wants, selErr := d.refreshWants()

	var wg sync.WaitGroup
	errs := make([]error, len(wants))
	for i, w := range wants {
		wg.Add(1)
		go func(i int, w want.Want) {
			defer wg.Done()
			if err := d.reconcileModel(ctx, w); err != nil {
				errs[i] = fmt.Errorf("model %s: %w", want.Key(w.Model.Name, w.Model.Revision), err)
			}
		}(i, w)
	}
	wg.Wait()

	return errors.Join(append(errs, selErr)...)
}

// refreshWants re-reads the selection file and returns what this node
// wants now, dropping models it no longer wants from /status. A selection
// file that fails to parse leaves the last good selection in place; its
// error is returned with the wants.
func (d *Daemon) refreshWants() ([]want.Want, error) {
	selErr := d.reloadSelection()
	d.mu.Lock()
	sel := d.cfg.Selection
	d.mu.Unlock()
	wants, err := want.FromSelection(d.cfg.Catalog, sel)
	d.pruneStatuses(wants)
	return wants, errors.Join(selErr, err)
}

func (d *Daemon) reloadSelection() error {
	if d.cfg.SelectionPath == "" {
		return nil
	}
	sel, err := catalog.LoadSelection(d.cfg.SelectionPath)
	if errors.Is(err, fs.ErrNotExist) {
		sel, err = &catalog.Selection{}, nil
	}
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.cfg.Selection = sel
	d.mu.Unlock()
	return nil
}

// pruneStatuses drops models the selection no longer wants, so /status
// doesn't keep advertising them after the selection file changes.
func (d *Daemon) pruneStatuses(wants []want.Want) {
	keep := make(map[string]bool, len(wants))
	for _, w := range wants {
		keep[want.Key(w.Model.Name, w.Model.Revision)] = true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.statuses {
		if !keep[k] {
			delete(d.statuses, k)
		}
	}
}

// progress builds the status of w from its need and rs: ready once the
// whole revision is held, partial once every needed piece of a partial
// want is held, and downloading (or the given state) until then.
func progress(w want.Want, m *store.Manifest, need *want.Need, rs *store.RevisionState, state string) server.ModelStatus {
	ms := server.ModelStatus{
		Model: w.Model.Name, Revision: w.Model.Revision, State: state,
		BytesTotal: need.Bytes(m), BytesDone: need.BytesDone(m, rs),
	}
	if !w.Whole {
		ms.Selection = server.SelectionPartial
	}
	switch {
	case rs.Complete():
		ms.State = server.StateReady
	case state != server.StateError && need.Satisfied(m, rs):
		ms.State = server.StatePartial
	}
	return ms
}

func (d *Daemon) reconcileModel(ctx context.Context, w want.Want) error {
	m := w.Model
	manifest, err := d.cfg.Store.LoadManifest(m.Name, m.Revision)
	if err != nil {
		manifest, err = d.acquireManifest(ctx, m)
		if err != nil {
			ms := server.ModelStatus{
				Model: m.Name, Revision: m.Revision, State: server.StatePending,
				Error: err.Error(),
			}
			if !w.Whole {
				ms.Selection = server.SelectionPartial
			}
			d.setStatus(ms)
			return nil // no source has it yet; retry on the next tick, not fatal.
		}
	}

	need, err := want.Resolve(manifest, w)
	if err != nil {
		d.setStatus(server.ModelStatus{
			Model: m.Name, Revision: m.Revision, State: server.StateError,
			Selection: server.SelectionPartial, Error: err.Error(),
		})
		return err
	}

	rs, err := d.cfg.Store.LoadState(manifest)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	if d.cfg.VerifyOnStart && d.firstVisit(want.Key(m.Name, m.Revision)) {
		dropped, err := d.cfg.Store.Reverify(manifest, rs)
		if err != nil {
			return fmt.Errorf("re-verify: %w", err)
		}
		if dropped > 0 {
			log.Printf("model-loader: %s@%s: re-verify dropped %d segments that no longer match the manifest", m.Name, m.Revision, dropped)
		}
	}
	if rs.Complete() || need.Satisfied(manifest, rs) {
		d.setStatus(progress(w, manifest, need, rs, server.StateReady))
		return nil
	}

	d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))

	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err != nil {
		peers = nil // a discovery hiccup shouldn't block falling back to WAN.
	}
	fetchers := d.buildFetchers(m, peers)

	for _, fm := range manifest.Files {
		fn := need.Files[fm.Path]
		if fn == nil || rs.FileState(fm).Complete {
			continue
		}
		d.sem <- struct{}{}
		err := transfer.FetchFile(ctx, d.cfg.Store, m.Name, m.Revision, fm, rs, fetchers, fn.Segments)
		<-d.sem
		if err != nil {
			ms := progress(w, manifest, need, rs, server.StateError)
			ms.Error = err.Error()
			d.setStatus(ms)
			return fmt.Errorf("fetch %s: %w", fm.Path, err)
		}
		d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))
	}

	d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))
	return nil
}

// firstVisit reports whether key has not been seen before by this process.
func (d *Daemon) firstVisit(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.visited[key] {
		return false
	}
	d.visited[key] = true
	return true
}
