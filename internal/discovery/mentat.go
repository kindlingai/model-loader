package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultMentatURL is where mentatd serves its HTTP side port on every
// kindling-spark-os box (mentat daemon --http-port, default 6380).
const DefaultMentatURL = "http://127.0.0.1:6380"

// mentatStatusLimit bounds the /status answer read. A peer row is a few
// KiB (its addresses and probe table), so this is room for hundreds of
// boxes; a larger answer is refused rather than read into memory.
const mentatStatusLimit = 16 << 20

// mentatTTL is how long one /status answer serves every caller. The
// daemon asks for peers once per model and per pull, so dozens of calls
// can come at once; mentatd answers each on its own thread under its one
// state lock, so they share one fetch and its answer for this long.
const mentatTTL = time.Second

// mentatTimeout bounds one /status call. mentatd is on this box and
// answers from memory, so a slow answer means it is wedged, and the
// caller is better off with an error than waiting.
const mentatTimeout = 5 * time.Second

// Mentat discovers peers from the local mentatd's peer table, on
// kindling-spark-os, where mentatd already finds the other boxes on the
// LAN. It reads mentatd's GET /status: callers that overlap share one
// fetch, and its answer serves for mentatTTL after it arrives. So a box
// mentatd finds dead drops out once a fetch begun after that has ended:
// within mentatTTL plus a fetch's time, at most mentatTimeout.
//
// mentatd knows each box's addresses, not the port model-loader listens
// on there, so every box is taken to listen on the same port; a box with
// several addresses is reached at the first that takes a connection on
// it (see choose). (Reading the
// port from a record each box publishes waits on kindlingai/mentat#4.)
type Mentat struct {
	statusURL string
	port      int
	client    *http.Client

	mu      sync.Mutex
	at      time.Time         // when peers and err were fetched
	peers   []Peer            // the last answer
	err     error             // or why it failed
	pending chan struct{}     // closed when the fetch under way ends; nil if none
	probes  map[string]probed // by mentat node ID; see choose

	dial func(network, addr string, timeout time.Duration) (net.Conn, error)
}

// NewMentat builds a discoverer that asks the mentatd at baseURL (such as
// DefaultMentatURL) for its peers, and reaches model-loader on each at
// port.
func NewMentat(baseURL string, port int) (*Mentat, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("mentat discovery: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("mentat discovery: %q is not an http(s)://host[:port] URL", baseURL)
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n <= 0 || n > 65535 {
			return nil, fmt.Errorf("mentat discovery: %q has no valid port", baseURL)
		}
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/status"
	u.RawPath = ""
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("mentat discovery: %d is not a port", port)
	}
	return &Mentat{
		statusURL: u.String(),
		port:      port,
		client:    &http.Client{Timeout: mentatTimeout},
		dial:      net.DialTimeout,
	}, nil
}

// mentatStatus is the part of mentatd's /status this reads.
type mentatStatus struct {
	NodeID string                 `json:"node_id"`
	Peers  *map[string]mentatPeer `json:"peers"`
}

// mentatPeer is one row of mentatd's peer table (status.rs, peer_row).
type mentatPeer struct {
	// NodeIP is what the box calls itself; on a box with several networks
	// it may be one this box cannot route to.
	NodeIP string `json:"node_ip"`
	// LinkIP is the address mentatd's working link to the box uses, so
	// this box can reach it.
	LinkIP string `json:"link_ip"`
	// Addrs is every address the box listens on.
	Addrs []string `json:"addrs"`
	Alive bool     `json:"alive"`
}

// candidates returns the hosts the box may be reached at, best first:
// link_ip, which a working connection from this box uses (an IP, or the
// host name of a seed mentatd dialled), then node_ip, then addrs. Each is
// normalized (brackets off an IPv6 address, an IPv4-mapped one unmapped)
// and listed once; what is not a usable host is left out.
func (p mentatPeer) candidates() []string {
	var out []string
	seen := make(map[string]bool)
	for _, h := range append([]string{p.LinkIP, p.NodeIP}, p.Addrs...) {
		h = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(h), "["), "]")
		if a, err := netip.ParseAddr(h); err == nil {
			if a.IsUnspecified() {
				continue
			}
			h = a.Unmap().String()
		} else if !validHostname(h) {
			continue
		}
		if !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	return out
}

// validHostname reports whether h can be a DNS host name.
func validHostname(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(h, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
				return false
			}
		}
	}
	return true
}

// Peers returns every box mentatd holds alive, in node ID order. A box
// mentatd holds dead is left out: it would only time out. This box is
// not in mentatd's peer table.
//
// A call that finds a fetch under way waits for it rather than start
// another; ctx ends only its own wait, not the fetch, which other callers
// may be waiting on too.
func (m *Mentat) Peers(ctx context.Context) ([]Peer, error) {
	for {
		m.mu.Lock()
		if !m.at.IsZero() && time.Since(m.at) < mentatTTL {
			peers, err := m.peers, m.err
			m.mu.Unlock()
			return append([]Peer(nil), peers...), err
		}
		if m.pending == nil {
			done := make(chan struct{})
			m.pending = done
			go func() {
				peers, err := m.fetch()
				m.mu.Lock()
				m.peers, m.err, m.at, m.pending = peers, err, time.Now(), nil
				m.mu.Unlock()
				close(done)
			}()
		}
		wait := m.pending
		m.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, fmt.Errorf("mentat discovery: %w", ctx.Err())
		}
	}
}

// fetch reads mentatd's /status once, bounded by mentatTimeout.
func (m *Mentat) fetch() ([]Peer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), mentatTimeout)
	defer cancel()
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
	if st.NodeID == "" || st.Peers == nil {
		return nil, fmt.Errorf("mentat discovery: %s is not a mentatd status (no node_id or peers)", m.statusURL)
	}

	table := *st.Peers
	ids := make([]string, 0, len(table))
	for id := range table {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type row struct {
		id    string
		hosts []string
	}
	var rows []row
	for _, id := range ids {
		p := table[id]
		if hosts := p.candidates(); p.Alive && id != st.NodeID && len(hosts) > 0 {
			rows = append(rows, row{id, hosts})
		}
	}
	chosen := make([]string, len(rows))
	var wg sync.WaitGroup
	for k, r := range rows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chosen[k] = m.choose(r.id, r.hosts)
		}()
	}
	wg.Wait()
	seen := make(map[string]bool)
	var out []Peer
	for _, host := range chosen {
		addr := net.JoinHostPort(host, strconv.Itoa(m.port))
		if !seen[addr] {
			seen[addr] = true
			out = append(out, Peer{Addr: addr})
		}
	}
	return out, nil
}

// probeTimeout bounds one connection attempt to a candidate address, and
// probeTTL how long the address found serves before it is tried again.
const (
	probeTimeout = 500 * time.Millisecond
	probeTTL     = 30 * time.Second
)

// probed is the address chosen for a box, and from which candidates.
type probed struct {
	hosts string // the candidates, joined
	host  string
	at    time.Time
}

// choose returns the host to reach box id at. With one candidate that is
// it. With more, each is tried in order, a TCP connection to model-loader's
// port, and the first that connects is taken; if none does (model-loader
// is not up there yet, say) the first is. The answer is kept for probeTTL,
// or until mentatd lists other candidates.
func (m *Mentat) choose(id string, hosts []string) string {
	if len(hosts) == 1 {
		return hosts[0]
	}
	key := strings.Join(hosts, ",")
	m.mu.Lock()
	c, ok := m.probes[id]
	m.mu.Unlock()
	if ok && c.hosts == key && time.Since(c.at) < probeTTL {
		return c.host
	}
	host := hosts[0]
	for _, h := range hosts {
		conn, err := m.dial("tcp", net.JoinHostPort(h, strconv.Itoa(m.port)), probeTimeout)
		if err == nil {
			conn.Close()
			host = h
			break
		}
	}
	m.mu.Lock()
	if m.probes == nil {
		m.probes = make(map[string]probed)
	}
	m.probes[id] = probed{hosts: key, host: host, at: time.Now()}
	m.mu.Unlock()
	return host
}
