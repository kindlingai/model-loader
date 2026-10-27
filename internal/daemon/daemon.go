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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
)

// defaultMaxConcurrentTransfers caps simultaneous segment transfers per
// node.
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

	// SelectionPath, if set, is re-read at the start of every reconcile
	// pass, so a selection file created or edited after startup takes
	// effect without a restart. A missing file is an empty selection; one
	// that fails to parse leaves Selection unchanged.
	SelectionPath string

	// Peers talks to other nodes' HTTP APIs. Defaults to a PeerClient
	// wrapping http.DefaultClient.
	Peers *transfer.PeerClient

	// MaxConcurrentTransfers bounds simultaneous segment transfers across
	// all files and models. Defaults to defaultMaxConcurrentTransfers.
	MaxConcurrentTransfers int

	// VerifyOnStart re-hashes every held segment of a model the first time
	// this process reconciles it, and fetches again any that no longer
	// match the manifest.
	VerifyOnStart bool

	// PullWorkers bounds how many segments a root fetches from origins at
	// once, for pull-through and for what it wants itself, and how many
	// files of one model it fetches at once. Defaults to
	// defaultPullWorkers.
	PullWorkers int

	// SegmentSize is the segment size of manifests a root publishes for
	// pull-through. Defaults to store.DefaultSegmentSize; tests set it
	// small.
	SegmentSize int64

	// Requests, if set, holds the named requests of local consumers. Their
	// pieces are wanted along with the selection's.
	Requests *request.Registry

	// DiskBudget is the most the store may hold, in bytes. Fetches evict
	// what they may to stay under it (see disk.go). 0 means no budget.
	DiskBudget int64

	// Dedup copies a file another revision in the store holds whole with
	// the same sha256 instead of fetching it (see dedup.go).
	Dedup bool
}

// Daemon runs the reconcile loop and reports status via the StatusProvider
// interface server.Server depends on.
type Daemon struct {
	cfg        Config
	limit      *transfer.Limiter // segment transfers, by priority
	availSlots chan struct{}     // see availabilityQueries
	kick       chan struct{}     // see Kick
	wantsGen   atomic.Uint64     // see Kick

	diskMu   sync.Mutex              // serializes making room and evicting
	reserved map[string]*reservation // room held for fetches; diskMu guards
	pinMu    sync.Mutex              // orders request changes and eviction
	bsOnce   sync.Once               // see blockSize
	bs       int64
	useMu    sync.Mutex     // guards serving
	serving  map[string]int // segments being served; see useKey
	onPass   func()         // tests: called as each Run pass starts

	mu       sync.Mutex
	statuses map[string]server.ModelStatus
	disk     *server.DiskStatus // see diskPass
	change   chan struct{}      // closed and replaced on every status change
	verified map[string]bool
	reused   map[string]int64 // bytes copied by dedup, by want.Key

	stateMu sync.Mutex
	states  map[string]*store.RevisionState // see revState

	pullMu sync.Mutex
	pulls  map[string]*pullCall          // see once
	claims map[string]map[int]*pullClaim // see claimSegments

	needMu    sync.Mutex
	needs     map[string]*resolvedNeed // see resolveRequest
	pullSrcs  map[string]cachedSources
	pullLimit *transfer.Limiter // pull-through segment fetches, by priority
}

// New builds a Daemon from cfg, applying defaults for anything left zero.
func New(cfg Config) *Daemon {
	if cfg.Peers == nil {
		cfg.Peers = transfer.NewPeerClient(nil)
	}
	if cfg.MaxConcurrentTransfers <= 0 {
		cfg.MaxConcurrentTransfers = defaultMaxConcurrentTransfers
	}
	if cfg.PullWorkers <= 0 {
		cfg.PullWorkers = defaultPullWorkers
	}
	return &Daemon{
		cfg:        cfg,
		limit:      transfer.NewLimiter(cfg.MaxConcurrentTransfers),
		availSlots: make(chan struct{}, availabilityQueries),
		kick:       make(chan struct{}, 1),
		reserved:   make(map[string]*reservation),
		serving:    make(map[string]int),
		change:     make(chan struct{}),
		statuses:   make(map[string]server.ModelStatus),
		verified:   make(map[string]bool),
		reused:     make(map[string]int64),
		states:     make(map[string]*store.RevisionState),
		pulls:      make(map[string]*pullCall),
		pullSrcs:   make(map[string]cachedSources),
		pullLimit:  transfer.NewLimiter(cfg.PullWorkers),
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
	if d.disk != nil {
		ds := *d.disk
		resp.Disk = &ds
	}
	return resp
}

func (d *Daemon) setStatus(ms server.ModelStatus) {
	ms.UpdatedAt = time.Now().UTC()
	key := want.Key(ms.Model, ms.Revision)
	d.mu.Lock()
	ms.BytesReused = d.reused[key]
	d.statuses[key] = ms
	d.notifyLocked()
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
		gen := d.wantsGen.Load() // before the wants it goes with
		if d.onPass != nil {
			d.onPass()
		}
		wants, err := d.refreshWants()
		if err != nil {
			onErr(err)
		}
		if _, running := busy.LoadOrStore(diskPassKey, true); !running {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer busy.Delete(diskPassKey)
				d.diskPass(ctx)
			}()
		}
		for _, w := range wants {
			key := want.Key(w.Model.Name, w.Model.Revision)
			if _, running := busy.LoadOrStore(key, true); running {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := d.reconcileModel(ctx, w)
				busy.Delete(key)
				if err != nil {
					onErr(fmt.Errorf("model %s: %w", key, err))
				}
				if d.wantsGen.Load() != gen {
					d.wake() // wants changed since they were read: look again.
				}
			}()
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.kick:
		}
	}
}

// diskPassKey keys the disk pass in Run's busy map; no model key has a
// NUL in it.
const diskPassKey = "\x00disk"

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
	sel, prio := d.withRequests(sel)
	wants, err := want.FromSelection(d.cfg.Catalog, sel)
	sortByPriority(wants, prio)
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
	d.pinMu.Lock() // see PutRequest
	d.mu.Lock()
	d.cfg.Selection = sel
	d.mu.Unlock()
	d.pinMu.Unlock()
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
		manifest, err = d.acquireManifest(ctx, w)
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

	need, err := resolveEach(manifest, w)
	if errors.Is(err, errSomePieces) {
		err = nil // the rest are fetched; requestStatus reports the bad one.
	}
	if err != nil {
		d.setStatus(server.ModelStatus{
			Model: m.Name, Revision: m.Revision, State: server.StateError,
			Selection: server.SelectionPartial, Error: err.Error(),
		})
		return err
	}

	rs, err := d.revState(manifest)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	if key := want.Key(m.Name, m.Revision); d.cfg.VerifyOnStart && !d.visited(key) {
		dropped, err := d.cfg.Store.Reverify(manifest, rs)
		if err != nil {
			return fmt.Errorf("re-verify: %w", err)
		}
		d.markVisited(key)
		if dropped > 0 {
			log.Printf("model-loader: %s@%s: re-verify dropped %d segments that no longer match the manifest", m.Name, m.Revision, dropped)
		}
	}
	// A consumer checksum given after its range was fetched is checked
	// against the held bytes now; one that fails drops what it covers, so
	// the need is no longer satisfied and that is fetched again.
	if err := d.applyPendingChecks(manifest, need, rs); err != nil {
		log.Printf("model-loader: %s@%s: %v", m.Name, m.Revision, err)
	}
	if rs.Complete() || need.Satisfied(manifest, rs) {
		d.setStatus(progress(w, manifest, need, rs, server.StateReady))
		return nil
	}

	d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))

	var needs []spaceNeed
	for _, fm := range manifest.Files {
		if fn := need.Files[fm.Path]; fn != nil {
			needs = append(needs, segmentNeeds(manifest, rs, fm, fn.Segments, d.blockSize())...)
		}
	}
	release, err := d.makeRoom(ctx, needs)
	if err != nil {
		ms := progress(w, manifest, need, rs, server.StateError)
		ms.Error = err.Error()
		d.setStatus(ms)
		return err
	}
	defer release()

	// A root holding a manifest it published for pull-through fetches what
	// it needs itself the same way it does for a peer: from the origin, one
	// fetch per segment however many ask.
	pull := d.IsWANEligible() && lazy(manifest)
	var srcs *sources
	if !pull {
		peers, err := d.cfg.Discoverer.Peers(ctx)
		if err != nil {
			peers = nil // a discovery hiccup shouldn't block falling back to WAN.
		}
		srcs = d.buildSources(ctx, m, manifest, peers)
	}

	// Transfers this pass makes ask for slots at the want's priority, here
	// and, through the priority header, at a root pulling through.
	ctx = server.WithPriority(ctx, w.Priority)
	// A root fetches several files from the origin at once, so a model of
	// many files is not fetched one file at a time; their segment fetches
	// share its pull slots all the same (see pullFetch).
	files := 1
	if pull {
		files = d.cfg.PullWorkers
	}
	slots := make(chan struct{}, files)
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex // guards errs and lastProgress
		errs         []error
		lastProgress = time.Now()
	)
	twins := sync.OnceValue(d.twins) // see dedup.go; read when first needed
	for _, fm := range manifest.Files {
		fn := need.Files[fm.Path]
		if fn == nil || rs.FileState(fm).Complete {
			continue
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			var err error
			reused := false
			done := d.writing(m.Name, m.Revision, fm)
			if d.cfg.Dedup && fn.Whole {
				reused, err = d.reuseHeld(ctx, m, fm, rs, twins())
			}
			switch {
			case reused || err != nil:
			case pull:
				err = d.pullFile(ctx, m, manifest, fm, fn.Segments, fn.Checks)
			default:
				err = transfer.FetchFile(ctx, d.cfg.Store, m.Name, m.Revision, fm, rs, srcs.fetchers, transfer.Options{
					Need: fn.Segments, Order: srcs.order(fm), Limit: d.limit, Priority: w.Priority, Checks: fn.Checks,
				})
			}
			done()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// Go on with the other files: one bad piece (a consumer
				// checksum that does not match, say) must not hold back
				// every other request for the model.
				errs = append(errs, fmt.Errorf("fetch %s: %w", fm.Path, err))
				return
			}
			// Progress is worked out over every needed segment: at most
			// once a second, not after each of hundreds of files.
			if time.Since(lastProgress) >= time.Second {
				d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))
				lastProgress = time.Now()
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		ms := progress(w, manifest, need, rs, server.StateError)
		ms.Error = err.Error()
		d.setStatus(ms)
		return err
	}

	d.setStatus(progress(w, manifest, need, rs, server.StateDownloading))
	return nil
}

// visited reports whether this process has re-verified key already.
func (d *Daemon) visited(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.verified[key]
}

func (d *Daemon) markVisited(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.verified[key] = true
}

// applyPendingChecks checks every consumer checksum in need that covers
// only held segments and has not matched before, and saves rs.
func (d *Daemon) applyPendingChecks(m *store.Manifest, need *want.Need, rs *store.RevisionState) error {
	var errs []error
	changed := false
	for _, fm := range m.Files {
		fn := need.Files[fm.Path]
		if fn == nil || len(transfer.PendingChecks(fm, rs, fn.Checks)) == 0 {
			continue
		}
		changed = true
		f, err := os.Open(d.cfg.Store.FilePath(m.Model, m.Revision, fm.Path))
		if err != nil {
			// The file is gone: nothing the checks cover is held.
			for _, c := range transfer.PendingChecks(fm, rs, fn.Checks) {
				first, last := c.Segments(fm)
				for i := first; i <= last; i++ {
					rs.DropSegment(fm, i)
				}
			}
			errs = append(errs, err)
			continue
		}
		if err := transfer.ApplyChecks(f, fm, rs, fn.Checks); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", fm.Path, err))
		}
		f.Close()
	}
	if changed {
		if err := d.cfg.Store.SaveState(rs); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
