package discovery

import "context"

// Static is a fixed, operator-supplied peer list. It works on every
// deployment target with zero dependencies, which makes it the default for
// docker compose and a useful fallback seed list alongside a dynamic
// backend elsewhere.
type Static struct {
	peers []Peer
}

// NewStatic builds a Static discoverer from a list of "host:port" addresses.
func NewStatic(addrs []string) *Static {
	peers := make([]Peer, len(addrs))
	for i, a := range addrs {
		peers[i] = Peer{Addr: a}
	}
	return &Static{peers: peers}
}

func (s *Static) Peers(ctx context.Context) ([]Peer, error) {
	out := make([]Peer, len(s.peers))
	copy(out, s.peers)
	return out, nil
}
