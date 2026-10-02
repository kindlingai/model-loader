package discovery

import "context"

// Multi queries several Discoverers and unions their results, deduplicated
// by address. This lets a deployment combine a static seed list (e.g. a
// known root's address) with a dynamic backend, rather than having to pick
// exactly one source of truth for membership.
type Multi struct {
	backends []Discoverer
}

// NewMulti combines several discovery backends into one.
func NewMulti(backends ...Discoverer) *Multi {
	return &Multi{backends: backends}
}

func (m *Multi) Peers(ctx context.Context) ([]Peer, error) {
	seen := make(map[string]bool)
	var out []Peer
	var firstErr error
	for _, b := range m.backends {
		peers, err := b.Peers(ctx)
		if err != nil {
			// A single backend being unreachable (e.g. k8s DNS not
			// resolvable from a dev box) shouldn't blind the node to
			// peers a different backend already knows about.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, p := range peers {
			if !seen[p.Addr] {
				seen[p.Addr] = true
				out = append(out, p)
			}
		}
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}
