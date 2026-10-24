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
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
	"github.com/kindlingai/model-loader/internal/want"
)

// acquireManifest gets m's manifest onto this node for the first time,
// preferring a LAN peer that already has it and falling back to a WAN fetch
// only when this node is root-eligible. This is the "soft check-LAN before
// pulling" the design doc calls for, so two root-eligible nodes booting at
// once don't both hit WAN for the same new catalog entry if one of them (or
// a plain peer that already mirrored it) answers first.
//
// A root without the manifest downloads a model it wants whole from the
// origin and publishes the manifest when done, as before; for one it wants
// only part of it publishes a manifest for pull-through at once (see
// Manifest).
func (d *Daemon) acquireManifest(ctx context.Context, w want.Want) (*store.Manifest, error) {
	m := w.Model
	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err == nil {
		for _, p := range peers {
			mf, err := d.cfg.Peers.Manifest(ctx, p.Addr, m.Name, m.Revision)
			if err != nil {
				continue
			}
			if mf.Model != m.Name || mf.Revision != m.Revision {
				continue
			}
			if err := mf.Validate(); err != nil {
				continue // a broken peer; ask the next one.
			}
			if err := d.cfg.Store.SaveManifest(mf); err != nil {
				return nil, fmt.Errorf("save manifest from peer %s: %w", p.Addr, err)
			}
			return mf, nil
		}
	}

	if !d.IsWANEligible() {
		return nil, fmt.Errorf("no peer has %s@%s yet, and this node is not WAN-eligible", m.Name, m.Revision)
	}
	if !w.Whole {
		return d.Manifest(ctx, m.Name, m.Revision)
	}

	if err := d.limit.Acquire(ctx, w.Priority); err != nil {
		return nil, err
	}
	defer d.limit.Release()
	return d.downloadFromWAN(ctx, m)
}

// availabilityTimeout bounds how long one peer's /segments may take.
const availabilityTimeout = 10 * time.Second

// sources are the places a node can fetch m's segments from: every known
// LAN peer, then this node's own WAN source if it may use one, with what
// each peer reported holding.
type sources struct {
	fetchers []transfer.Fetcher
	// avail[k] is what peer k holds, or nil when it did not say (an older
	// version, or the query failed). Unused past the peers.
	avail []*server.SegmentsResponse
	// whole[k] means peer k is a plain peer too old for /segments whose
	// /status reports the revision ready: it holds every segment.
	whole []bool
	peers int
	// holdersOnly leaves out of each segment's order the LAN nodes that
	// do not hold it: a root filling a pull asks them with
	// server.HeaderNoPull, so they could only answer 416.
	holdersOnly bool
}

// availabilityQueries bounds how many peers the node asks at once, across
// every model it reconciles.
const availabilityQueries = 8

// buildSources lists m's sources and asks every peer, in parallel, which
// segments it holds.
func (d *Daemon) buildSources(ctx context.Context, m catalog.Model, manifest *store.Manifest, peers []discovery.Peer) *sources {
	s := &sources{avail: make([]*server.SegmentsResponse, len(peers)), whole: make([]bool, len(peers)), peers: len(peers)}
	var wg sync.WaitGroup
	for k, p := range peers {
		s.fetchers = append(s.fetchers, transfer.PeerFetcher{
			Client: d.cfg.Peers, Addr: p.Addr, Model: m.Name, Revision: m.Revision,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One bound across the node, however many models reconcile.
			select {
			case d.availSlots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-d.availSlots }()
			qctx, cancel := context.WithTimeout(ctx, availabilityTimeout)
			defer cancel()
			// On any error (a timeout, a bad answer) avail[k] stays nil:
			// unknown, which ranks it behind every holder.
			resp, err := d.cfg.Peers.Segments(qctx, p.Addr, m.Name, m.Revision)
			if err == nil {
				if resp.Valid(manifest) {
					s.avail[k] = resp
				}
				return
			}
			if errors.Is(err, transfer.ErrNoSegments) {
				s.whole[k] = d.olderPeerHasAll(qctx, p.Addr, m)
			}
		}()
	}
	wg.Wait()
	if d.IsWANEligible() {
		if src, err := d.cfg.Sources.For(m); err == nil {
			s.fetchers = append(s.fetchers, transfer.WANFetcher{Source: src, Model: atCommit(m, manifest)})
		}
	}
	return s
}

// olderPeerHasAll reports whether a peer too old for /segments is a plain
// peer whose /status reports m ready, as nodes chose sources before.
func (d *Daemon) olderPeerHasAll(ctx context.Context, addr string, m catalog.Model) bool {
	st, err := d.cfg.Peers.Status(ctx, addr)
	if err != nil {
		return false
	}
	for _, r := range st.Roles {
		if r == "root" {
			return false // a root ranks after peers anyway.
		}
	}
	for _, ms := range st.Models {
		if ms.Model == m.Name && ms.Revision == m.Revision && ms.State == server.StateReady {
			return true
		}
	}
	return false
}

// order returns, for each segment of the file at path, the sources to try:
// the plain peers that hold it, then the roots that hold it, each group
// starting at a different node for each segment so that consecutive
// segments come from different nodes at once; then peers that did not say
// what they hold; then roots that do not hold it (a root can pull it from
// the origin); then the other peers, in case they gained it since they
// were asked; and last this node's own WAN source.
func (s *sources) order(path string) func(i int) []int {
	return func(i int) []int {
		var peerHolders, rootHolders, unknown, roots, others []int
		for k := 0; k < s.peers; k++ {
			a := s.avail[k]
			has := s.whole[k] || (a != nil && a.Files[path].Has(i))
			switch {
			case s.whole[k]:
				peerHolders = append(peerHolders, k)
			case a == nil:
				unknown = append(unknown, k)
			case has && a.IsRoot():
				rootHolders = append(rootHolders, k)
			case has:
				peerHolders = append(peerHolders, k)
			case a.IsRoot():
				roots = append(roots, k)
			default:
				others = append(others, k)
			}
		}
		out := make([]int, 0, len(s.fetchers))
		out = appendRotated(out, peerHolders, i)
		out = appendRotated(out, rootHolders, i)
		out = append(out, unknown...)
		if !s.holdersOnly {
			out = append(out, roots...)
			out = append(out, others...)
		}
		for k := s.peers; k < len(s.fetchers); k++ {
			out = append(out, k)
		}
		return out
	}
}

// appendRotated appends ks to out starting at ks[i % len(ks)].
func appendRotated(out, ks []int, i int) []int {
	for j := range ks {
		out = append(out, ks[(i+j)%len(ks)])
	}
	return out
}
