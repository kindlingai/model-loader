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
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
)

// defaultMaxConcurrentTransfers caps simultaneous file downloads per node.
// home-ops hit real OOMs at 4 concurrent Hugging Face downloads; 2 is a safe
// default for a box that may also be serving an LLM workload.
const defaultMaxConcurrentTransfers = 2

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

	// Peers talks to other nodes' HTTP APIs. Defaults to a PeerClient
	// wrapping http.DefaultClient.
	Peers *transfer.PeerClient

	// MaxConcurrentTransfers bounds simultaneous file downloads across all
	// models. Defaults to defaultMaxConcurrentTransfers.
	MaxConcurrentTransfers int
}

// Daemon runs the reconcile loop and reports status via the StatusProvider
// interface server.Server depends on.
type Daemon struct {
	cfg Config
	sem chan struct{}

	mu       sync.Mutex
	statuses map[string]server.ModelStatus
}

// New builds a Daemon from cfg, applying defaults for anything left zero.
func New(cfg Config) *Daemon {
	if cfg.Peers == nil {
		cfg.Peers = transfer.NewPeerClient(nil)
	}
	if cfg.MaxConcurrentTransfers <= 0 {
		cfg.MaxConcurrentTransfers = defaultMaxConcurrentTransfers
	}
	return &Daemon{
		cfg:      cfg,
		sem:      make(chan struct{}, cfg.MaxConcurrentTransfers),
		statuses: make(map[string]server.ModelStatus),
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
	resp := server.StatusResponse{NodeID: d.cfg.NodeID, Roles: d.cfg.Roles}
	for _, ms := range d.statuses {
		resp.Models = append(resp.Models, ms)
	}
	return resp
}

func statusKey(model, revision string) string { return model + "@" + revision }

func (d *Daemon) setStatus(ms server.ModelStatus) {
	ms.UpdatedAt = time.Now().UTC()
	d.mu.Lock()
	d.statuses[statusKey(ms.Model, ms.Revision)] = ms
	d.mu.Unlock()
}

// Run reconciles immediately, then again every interval, until ctx is
// canceled. A failed reconcile pass is logged by returning it from the
// per-tick call but does not stop the loop -- a transient WAN or peer
// failure for one model shouldn't take the whole node out of service.
func (d *Daemon) Run(ctx context.Context, interval time.Duration, onErr func(error)) {
	if onErr == nil {
		onErr = func(error) {}
	}
	if err := d.Reconcile(ctx); err != nil {
		onErr(err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.Reconcile(ctx); err != nil {
				onErr(err)
			}
		}
	}
}

// Reconcile runs one pass over every catalog entry this node's Selection
// wants, bringing each one's manifest and files up to date. Models are
// reconciled concurrently (resolving a manifest and checking peer status is
// cheap); actual file transfers are bounded by MaxConcurrentTransfers
// regardless of how many models are in flight.
func (d *Daemon) Reconcile(ctx context.Context) error {
	wanted := d.cfg.Catalog.Wanted(d.cfg.Selection)

	var wg sync.WaitGroup
	errs := make([]error, len(wanted))
	for i, m := range wanted {
		wg.Add(1)
		go func(i int, m catalog.Model) {
			defer wg.Done()
			if err := d.reconcileModel(ctx, m); err != nil {
				errs[i] = fmt.Errorf("model %s@%s: %w", m.Name, m.Revision, err)
			}
		}(i, m)
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (d *Daemon) reconcileModel(ctx context.Context, m catalog.Model) error {
	manifest, err := d.cfg.Store.LoadManifest(m.Name, m.Revision)
	if err != nil {
		manifest, err = d.acquireManifest(ctx, m)
		if err != nil {
			d.setStatus(server.ModelStatus{
				Model: m.Name, Revision: m.Revision, State: server.StatePending,
				Error: err.Error(),
			})
			return nil // no source has it yet; retry on the next tick, not fatal.
		}
	}

	rs, err := d.cfg.Store.LoadState(manifest)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	if rs.Complete() {
		d.setStatus(server.ModelStatus{
			Model: m.Name, Revision: m.Revision, State: server.StateReady,
			BytesTotal: manifest.TotalSize(), BytesDone: manifest.TotalSize(),
		})
		return nil
	}

	d.setStatus(server.ModelStatus{
		Model: m.Name, Revision: m.Revision, State: server.StateDownloading,
		BytesTotal: manifest.TotalSize(), BytesDone: rs.BytesDone(manifest),
	})

	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err != nil {
		peers = nil // a discovery hiccup shouldn't block falling back to WAN.
	}
	fetchers := d.buildFetchers(m, peers)

	for _, fm := range manifest.Files {
		if rs.FileState(fm).Complete {
			continue
		}
		d.sem <- struct{}{}
		err := transfer.FetchFile(ctx, d.cfg.Store, m.Name, m.Revision, fm, rs, fetchers)
		<-d.sem
		if err != nil {
			d.setStatus(server.ModelStatus{
				Model: m.Name, Revision: m.Revision, State: server.StateError,
				BytesTotal: manifest.TotalSize(), BytesDone: rs.BytesDone(manifest),
				Error: err.Error(),
			})
			return fmt.Errorf("fetch %s: %w", fm.Path, err)
		}
		d.setStatus(server.ModelStatus{
			Model: m.Name, Revision: m.Revision, State: server.StateDownloading,
			BytesTotal: manifest.TotalSize(), BytesDone: rs.BytesDone(manifest),
		})
	}

	d.setStatus(server.ModelStatus{
		Model: m.Name, Revision: m.Revision, State: server.StateReady,
		BytesTotal: manifest.TotalSize(), BytesDone: manifest.TotalSize(),
	})
	return nil
}
