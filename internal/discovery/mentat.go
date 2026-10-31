package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
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
// Each box's model-loader publishes its port in a record through its own
// mentatd (see Publish), and a box with no record, such as one whose
// mentatd predates records, is taken to listen on the default port. A box
// with several addresses is reached at the first that takes a connection
// on it (see choose). Addresses with a preferred tag go first (see
// NewMentat).
type Mentat struct {
	statusURL string
	recordURL string
	port      int
	prefer    []string // address tags to try first (see candidates)
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
// port. A box's addresses that mentatd tags with one of preferTags (such
// as "connectx") and that this box's mentatd probes ok are tried before
// its others.
func NewMentat(baseURL string, port int, preferTags []string) (*Mentat, error) {
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
	base := strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	u.Path = base + "/status"
	statusURL := u.String()
	u.Path = base + "/records/" + MentatRecordName
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("mentat discovery: %d is not a port", port)
	}
	return &Mentat{
		statusURL: statusURL,
		recordURL: u.String(),
		port:      port,
		prefer:    preferTags,
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
	// Records is what the box's processes publish through its mentatd,
	// by name. Each is parsed on its own, so one that is malformed costs
	// only that box its port.
	Records map[string]json.RawMessage `json:"records"`
	// AddrTags is the tags the box gives each of its addresses, such as
	// "connectx" and "rdma" or "lan". Older mentatd sends none.
	AddrTags json.RawMessage `json:"addr_tags"`
	// Probes is this box's mentatd's probes to the box: by this box's
	// address, then the box's address. Older mentatd sends none. Like
	// AddrTags it is parsed on its own (see preferred), so one that is
	// malformed costs only that box its preferred addresses.
	Probes json.RawMessage `json:"probes"`
}

// portOr returns the port the box's model-loader record gives, or def
// when it has no valid one.
func (p mentatPeer) portOr(def int) int {
	var rec struct {
		Port int `json:"port"`
	}
	if json.Unmarshal(p.Records[MentatRecordName], &rec) != nil || rec.Port <= 0 || rec.Port > 65535 {
		return def
	}
	return rec.Port
}

// candidates returns the hosts the box may be reached at, best first.
// First come the addresses preferred returns, then link_ip, node_ip and
// addrs. A working connection from this box uses link_ip: an IP, or the
// host name of a seed mentatd dialled. candidates normalizes each host
// (see normHost), lists it once, and leaves out what is not a usable host.
func (p mentatPeer) candidates(prefer []string) []string {
	var out []string
	seen := make(map[string]bool)
	hosts := append(p.preferred(prefer), p.LinkIP, p.NodeIP)
	for _, h := range append(hosts, p.Addrs...) {
		h, ok := normHost(h)
		if ok && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	return out
}

// preferred returns the box's addresses, in addrs order, that carry one
// of the tags in prefer and that this box's mentatd has an ok probe to
// from any of its own addresses. A tag alone is not enough: a box's
// ConnectX address may be on a cable to another box only. preferred
// returns nil when prefer is empty or the row lacks tags or probes, as
// from an older mentatd.
func (p mentatPeer) preferred(prefer []string) []string {
	if len(prefer) == 0 {
		return nil
	}
	var tags map[string][]string
	var probes map[string]map[string]struct {
		OK bool `json:"ok"`
	}
	if json.Unmarshal(p.AddrTags, &tags) != nil || json.Unmarshal(p.Probes, &probes) != nil {
		return nil
	}
	tagged := make(map[string]bool)
	for addr, ts := range tags {
		h, ok := normHost(addr)
		if ok && slices.ContainsFunc(ts, func(t string) bool { return slices.Contains(prefer, t) }) {
			tagged[h] = true
		}
	}
	reached := make(map[string]bool)
	for _, row := range probes {
		for addr, r := range row {
			if h, ok := normHost(addr); ok && r.OK {
				reached[h] = true
			}
		}
	}
	var out []string
	for _, addr := range p.Addrs {
		if h, ok := normHost(addr); ok && tagged[h] && reached[h] {
			out = append(out, h)
		}
	}
	return out
}

// normHost strips the brackets from an IPv6 address and unmaps an
// IPv4-mapped address. normHost reports whether h is a usable host: an IP
// other than the unspecified address, or a DNS host name.
func normHost(h string) (string, bool) {
	h = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(h), "["), "]")
	if a, err := netip.ParseAddr(h); err == nil {
		return a.Unmap().String(), !a.IsUnspecified()
	}
	return h, validHostname(h)
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
		port  int
	}
	var rows []row
	for _, id := range ids {
		p := table[id]
		if hosts := p.candidates(m.prefer); p.Alive && id != st.NodeID && len(hosts) > 0 {
			rows = append(rows, row{id, hosts, p.portOr(m.port)})
		}
	}
	chosen := make([]string, len(rows))
	var wg sync.WaitGroup
	for k, r := range rows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chosen[k] = net.JoinHostPort(m.choose(r.id, r.hosts, r.port), strconv.Itoa(r.port))
		}()
	}
	wg.Wait()
	seen := make(map[string]bool)
	var out []Peer
	for _, addr := range chosen {
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
// port there, and the first that connects is taken; if none does
// (model-loader is not up there yet, say) the first is. The answer is kept
// for probeTTL, or until mentatd lists other candidates or another port.
func (m *Mentat) choose(id string, hosts []string, port int) string {
	if len(hosts) == 1 {
		return hosts[0]
	}
	key := strings.Join(hosts, ",") + "/" + strconv.Itoa(port)
	m.mu.Lock()
	c, ok := m.probes[id]
	m.mu.Unlock()
	if ok && c.hosts == key && time.Since(c.at) < probeTTL {
		return c.host
	}
	host := hosts[0]
	for _, h := range hosts {
		conn, err := m.dial("tcp", net.JoinHostPort(h, strconv.Itoa(port)), probeTimeout)
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

// MentatRecordName is the name model-loader publishes its record under in
// mentatd, PUT /records/model-loader.
const MentatRecordName = "model-loader"

// MentatRecord is what model-loader publishes about its box through
// mentatd, for every other box's model-loader, and gr8, to read from
// their own mentatd.
type MentatRecord struct {
	// Port is the port model-loader serves its HTTP API on.
	Port  int      `json:"port"`
	Roles []string `json:"roles"`
	// Holdings are the revisions the box holds, whole ("ready") or the
	// part it selects ("partial"). /segments says which segments.
	Holdings []MentatHolding `json:"holdings"`
	// Truncated means mentatd had no room for every holding, and some
	// are left out.
	Truncated bool `json:"truncated,omitempty"`
}

// MentatHolding is one revision a box holds.
type MentatHolding struct {
	Model    string `json:"model"`
	Revision string `json:"revision"`
	State    string `json:"state"`
}

// Publish stores rec as this box's record in mentatd, or deletes it when
// rec is nil. Holdings are sorted, so an unchanged record is the same
// JSON, which mentatd takes at no cost however often it is sent. A record
// mentatd has no room for (413) is sent again with half its holdings,
// until it fits or has none, so this need not know mentatd's limit.
func (m *Mentat) Publish(ctx context.Context, rec *MentatRecord) error {
	if rec == nil {
		_, err := m.send(ctx, http.MethodDelete, nil)
		return err
	}
	r := *rec
	r.Holdings = append(make([]MentatHolding, 0, len(r.Holdings)), r.Holdings...)
	sort.Slice(r.Holdings, func(i, j int) bool {
		a, b := r.Holdings[i], r.Holdings[j]
		return a.Model < b.Model || a.Model == b.Model && a.Revision < b.Revision
	})
	for {
		body, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("mentat record: %w", err)
		}
		code, err := m.send(ctx, http.MethodPut, body)
		if code != http.StatusRequestEntityTooLarge || len(r.Holdings) == 0 {
			return err
		}
		r.Holdings, r.Truncated = r.Holdings[:len(r.Holdings)/2], true
	}
}

// send makes one request to the record's URL, and returns its status code
// and, for any but 204, an error.
func (m *Mentat) send(ctx context.Context, method string, body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, mentatTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, m.recordURL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("mentat record: %w", err)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("mentat record: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		why, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, fmt.Errorf("mentat record: %s %s: %s: %s", method, m.recordURL, resp.Status, strings.TrimSpace(string(why)))
	}
	return resp.StatusCode, nil
}
