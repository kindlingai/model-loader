package daemon

import (
	"context"
	"fmt"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
)

// acquireManifest gets m's manifest onto this node for the first time,
// preferring a LAN peer that already has it and falling back to a WAN fetch
// only when this node is root-eligible. This is the "soft check-LAN before
// pulling" the design doc calls for, so two root-eligible nodes booting at
// once don't both hit WAN for the same new catalog entry if one of them (or
// a plain peer that already mirrored it) answers first.
func (d *Daemon) acquireManifest(ctx context.Context, m catalog.Model) (*store.Manifest, error) {
	peers, err := d.cfg.Discoverer.Peers(ctx)
	if err == nil {
		for _, p := range peers {
			mf, err := d.cfg.Peers.Manifest(ctx, p.Addr, m.Name, m.Revision)
			if err != nil {
				continue
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

	d.sem <- struct{}{}
	defer func() { <-d.sem }()
	return d.downloadFromWAN(ctx, m)
}

// buildFetchers orders candidate sources for m's remaining files: every
// known LAN peer first, then this node's own WAN source if it's eligible to
// use one. transfer.FetchFile tries each in order per segment, so a peer
// that's only partially synced still serves what it has before anything
// falls through to WAN.
func (d *Daemon) buildFetchers(m catalog.Model, peers []discovery.Peer) []transfer.Fetcher {
	fetchers := make([]transfer.Fetcher, 0, len(peers)+1)
	for _, p := range peers {
		fetchers = append(fetchers, transfer.PeerFetcher{
			Client: d.cfg.Peers, Addr: p.Addr, Model: m.Name, Revision: m.Revision,
		})
	}
	if d.IsWANEligible() {
		if src, err := d.cfg.Sources.For(m); err == nil {
			fetchers = append(fetchers, transfer.WANFetcher{Source: src, Model: m})
		}
	}
	return fetchers
}
