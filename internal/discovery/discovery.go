// Package discovery finds other model-loader nodes on the LAN. Each
// deployment target has its own natural answer (a fixed list for compose, a
// headless Service's DNS for Kubernetes, mentat's peer table for
// kindling-spark-os), so discovery is a small interface with one
// implementation per backend rather than a single mechanism every
// deployment has to bend to fit.
package discovery

import "context"

// Peer is one other node's address, as "host:port".
type Peer struct {
	Addr string
}

// Discoverer lists the currently known peers. Implementations may cache or
// re-resolve on every call; callers should treat the result as a snapshot
// that can change between calls, not a stable membership list.
type Discoverer interface {
	Peers(ctx context.Context) ([]Peer, error)
}
