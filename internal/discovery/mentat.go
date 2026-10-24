package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultMentatURL is where mentatd serves its HTTP side port on every
// kindling-spark-os box (mentat daemon --http-port, default 6380).
const DefaultMentatURL = "http://127.0.0.1:6380"

// mentatStatusLimit bounds the /status answer read. A peer row is a few
// KiB (its addresses and probe table), so this is room for hundreds of
// boxes; a larger answer is refused rather than read into memory.
const mentatStatusLimit = 16 << 20

// mentatTimeout bounds one /status call. mentatd is on this box and
// answers from memory, so a slow answer means it is wedged, and the
// caller is better off with an error than waiting.
const mentatTimeout = 5 * time.Second

// Mentat discovers peers from the local mentatd's peer table, on
// kindling-spark-os, where mentatd already finds the other boxes on the
// LAN. It reads mentatd's GET /status on every call: mentatd answers from
// memory, so there is nothing to gain from caching, and a box that dies is
// dropped as soon as mentatd notices.
//
// mentatd knows each box's address, not the port model-loader listens on
// there, so every box is taken to listen on the same port. (Reading the
// port from a record each box publishes waits on kindlingai/mentat#4.)
type Mentat struct {
	statusURL string
	port      int
	client    *http.Client
}

// NewMentat builds a discoverer that asks the mentatd at baseURL (such as
// DefaultMentatURL) for its peers, and reaches model-loader on each at
// port.
func NewMentat(baseURL string, port int) *Mentat {
	return &Mentat{
		statusURL: strings.TrimSuffix(baseURL, "/") + "/status",
		port:      port,
		client:    &http.Client{Timeout: mentatTimeout},
	}
}

// mentatStatus is the part of mentatd's /status this reads.
type mentatStatus struct {
	NodeID string                `json:"node_id"`
	Peers  map[string]mentatPeer `json:"peers"`
}

type mentatPeer struct {
	NodeIP string `json:"node_ip"`
	Alive  bool   `json:"alive"`
}

// Peers returns every box mentatd holds alive, in node ID order. A box
// mentatd holds dead is left out: it would only time out. This box is
// not in mentatd's peer table.
func (m *Mentat) Peers(ctx context.Context) ([]Peer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.statusURL, nil)
	if err != nil {
		return nil, fmt.Errorf("mentat discovery: %w", err)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mentat discovery: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mentat discovery: %s: %s", m.statusURL, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, mentatStatusLimit+1))
	if err != nil {
		return nil, fmt.Errorf("mentat discovery: read %s: %w", m.statusURL, err)
	}
	if len(body) > mentatStatusLimit {
		return nil, fmt.Errorf("mentat discovery: %s: answer over %d bytes", m.statusURL, mentatStatusLimit)
	}
	var st mentatStatus
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("mentat discovery: parse %s: %w", m.statusURL, err)
	}
	if st.NodeID == "" {
		return nil, fmt.Errorf("mentat discovery: %s is not a mentatd status (no node_id)", m.statusURL)
	}

	ids := make([]string, 0, len(st.Peers))
	for id := range st.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	port := strconv.Itoa(m.port)
	seen := make(map[string]bool)
	var out []Peer
	for _, id := range ids {
		p := st.Peers[id]
		if !p.Alive || id == st.NodeID || net.ParseIP(p.NodeIP) == nil {
			continue
		}
		addr := net.JoinHostPort(p.NodeIP, port)
		if !seen[addr] {
			seen[addr] = true
			out = append(out, Peer{Addr: addr})
		}
	}
	return out, nil
}
