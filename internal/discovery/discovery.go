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
	// Link is how this node reaches the peer at Addr, as far as the
	// backend knows: LinkPreferred, LinkOther, or "" for unknown.
	Link string
}

// Link classes of a Peer. A node ranks a relay (see docs/design.md,
// "Relay") after the holders it reaches at a preferred address or does
// not know how it reaches, and before those it reaches at LinkOther.
const (
	// LinkPreferred is an address with a preferred tag (such as
	// connectx), probed direct.
	LinkPreferred = "preferred"
	// LinkOther is an address with no preferred tag, at a box whose
	// addresses are tagged: the LAN, when the box has a fabric address
	// this node cannot reach.
	LinkOther = "other"
)

// Discoverer lists the currently known peers. Implementations may cache or
// re-resolve on every call; callers should treat the result as a snapshot
// that can change between calls, not a stable membership list.
type Discoverer interface {
	Peers(ctx context.Context) ([]Peer, error)
}
