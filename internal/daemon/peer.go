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
// A root without the manifest publishes one at once from the origin's file
// list (see Manifest), whether it wants the model whole or only part of it,
// and then fetches what it wants through it, as it does for a peer.
func (d *Daemon) acquireManifest(ctx context.Context, w want.Want) (*store.Manifest, error) {
	m := w.Model
	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err == nil {
		for _, p := range peers {
			mctx, cancel := context.WithTimeout(ctx, manifestTimeout)
			mf, err := d.cfg.Peers.Manifest(mctx, p.Addr, m.Name, m.Revision)
			cancel()
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
	return d.Manifest(ctx, m.Name, m.Revision)
}

// manifestTimeout bounds how long one peer's /manifest may take, so a
// peer that takes the connection and never answers is passed over. A root
// asked for one it does not hold fetches it from its origin first.
const manifestTimeout = 30 * time.Second

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
	// link[k] is how this node reaches peer k (discovery.Peer.Link).
	link []string
	// relays[k] is what the nodes peer k relays for hold (see
	// server.SegmentsResponse.Relayed), nil if it relays none.
	relays [][]server.RelayedHolder
	peers  int
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
	s := &sources{
		avail: make([]*server.SegmentsResponse, len(peers)), whole: make([]bool, len(peers)),
		link: make([]string, len(peers)), relays: make([][]server.RelayedHolder, len(peers)), peers: len(peers),
	}
	var wg sync.WaitGroup
	for k, p := range peers {
		s.link[k] = p.Link
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
					s.relays[k] = resp.ValidRelayed(manifest, d.cfg.NodeID)
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
//
// A relay that relays a node holding the segment (see relay.go) goes
// after the holders this node reaches at a preferred address
// (discovery.LinkPreferred) and before every other holder: a holder
// reached at no preferred address may be a box on the slow LAN that the
// relay reaches on a fast link.
//
// The rotation is by chunk (see store.ChunkSize), the same for every
// segment of one: a segment of a file of 64 MiB segments, so each one
// starts at a different node, as before; the segments of one chunk of a
// file of small ones go to one node, as one range (see
// transfer.FetchFile).
func (s *sources) order(fm store.FileManifest) func(i int) []int {
	path := fm.Path
	return func(i int) []int {
		rot := fm.ChunkOf(i)
		var peerHolders, rootHolders, farPeers, farRoots, relays, unknown, roots, others []int
		for k := 0; k < s.peers; k++ {
			a := s.avail[k]
			has := s.whole[k] || (a != nil && a.Files[path].Has(i))
			far := has && s.far(k, path, i)
			switch {
			case has && far && (s.whole[k] || !a.IsRoot()):
				farPeers = append(farPeers, k)
			case has && far:
				farRoots = append(farRoots, k)
			case s.whole[k]:
				peerHolders = append(peerHolders, k)
			case a == nil:
				unknown = append(unknown, k)
			case has && a.IsRoot():
				rootHolders = append(rootHolders, k)
			case has:
				peerHolders = append(peerHolders, k)
			case s.relays[k] != nil && relayHolds(s.relays[k], path, i):
				relays = append(relays, k)
			case a.IsRoot():
				roots = append(roots, k)
			default:
				others = append(others, k)
			}
		}
		out := make([]int, 0, len(s.fetchers))
		out = appendRotated(out, peerHolders, rot)
		out = appendRotated(out, rootHolders, rot)
		out = appendRotated(out, relays, rot)
		out = appendRotated(out, farPeers, rot)
		out = appendRotated(out, farRoots, rot)
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

// far reports whether holder k of segment i of the file at path ranks
// after a relay of it: some other peer relays a node holding it, and this
// node reaches k at an address with no preferred tag
// (discovery.LinkOther), or does not know how it reaches k and k is a node
// that relay relays for (a box named in -peers, say).
func (s *sources) far(k int, path string, i int) bool {
	if s.link[k] == discovery.LinkPreferred {
		return false
	}
	id := ""
	if a := s.avail[k]; a != nil {
		id = a.NodeID
	}
	for j := 0; j < s.peers; j++ {
		if j == k || s.relays[j] == nil {
			continue
		}
		for _, h := range s.relays[j] {
			if fs, ok := h.Files[path]; !ok || !fs.Has(i) {
				continue
			}
			if s.link[k] == discovery.LinkOther || (id != "" && h.NodeID == id) {
				return true
			}
		}
	}
	return false
}

// relayHolds reports whether one of hs holds segment i of the file at
// path.
func relayHolds(hs []server.RelayedHolder, path string, i int) bool {
	for _, h := range hs {
		if fs, ok := h.Files[path]; ok && fs.Has(i) {
			return true
		}
	}
	return false
}

// appendRotated appends ks to out starting at ks[i % len(ks)].
func appendRotated(out, ks []int, i int) []int {
	for j := range ks {
		out = append(out, ks[(i+j)%len(ks)])
	}
	return out
}
