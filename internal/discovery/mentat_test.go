package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A mentatd /status answer, cut down from a real one (status.rs,
// snapshot and peer_row) to the fields Mentat reads plus a few it must
// ignore.
const mentatStatusJSON = `{
  "proto": 7,
  "node_id": "aaaaaaaaaa",
  "node_ip": "10.0.0.1",
  "control_port": 6379,
  "peers": {
    "cccccccccc": {"node_ip": "10.0.0.3", "link_ip": "10.0.0.3", "alive": true, "stale": true, "http_port": 6380, "groups": null},
    "bbbbbbbbbb": {"node_ip": "172.16.0.2", "link_ip": "10.0.0.2", "addrs": ["172.16.0.2", "10.0.0.2"], "alive": true, "stale": false, "probes": {}},
    "dddddddddd": {"node_ip": "10.0.0.4", "link_ip": "10.0.0.4", "alive": false, "dead_since_ms": 1},
    "eeeeeeeeee": {"node_ip": "", "link_ip": "", "addrs": ["not a host", "10.0.0.5"], "alive": true},
    "ffffffffff": {"node_ip": "fd00::6", "link_ip": "", "alive": true},
    "gggggggggg": {"node_ip": "", "link_ip": "", "addrs": [], "alive": true},
    "hhhhhhhhhh": {"node_ip": "10.0.0.8", "link_ip": "10.0.0.2", "alive": true},
    "aaaaaaaaaa": {"node_ip": "10.0.0.1", "alive": true}
  },
  "clients": {}
}`

// fakeMentatd serves body as mentatd's /status.
func fakeMentatd(t *testing.T, status int, body string) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

// newMentat returns a Mentat for url whose probes connect to nothing, so
// each box is at its first candidate.
func newMentat(t *testing.T, url string) *Mentat {
	t.Helper()
	m, err := NewMentat(url, 7762)
	if err != nil {
		t.Fatal(err)
	}
	m.dial = dialOnly(nil)
	return m
}

// dialOnly returns a dial that connects only to the addresses in ok, and
// counts the attempts in tries.
func dialOnly(ok map[string]bool) func(string, string, time.Duration) (net.Conn, error) {
	return func(_, addr string, _ time.Duration) (net.Conn, error) {
		if ok[addr] {
			a, b := net.Pipe()
			b.Close()
			return a, nil
		}
		return nil, errors.New("refused")
	}
}

// A box mentatd reaches over one address but that runs model-loader on
// another is reached at the first candidate that takes a connection:
// link_ip, node_ip, then addrs. The choice is kept for probeTTL, unless
// the candidates change.
func TestMentatFallback(t *testing.T) {
	body := `{"node_id": "a", "peers": {"x": {"link_ip": "[fd00::9]", "node_ip": "10.9.9.9", "addrs": ["spark-9", "10.9.9.10", "::ffff:10.9.9.9"], "alive": true}}}`
	m := newMentat(t, fakeMentatd(t, http.StatusOK, body))
	var tries atomic.Int32
	reach := dialOnly(map[string]bool{"10.9.9.10:7762": true})
	m.dial = func(n, addr string, d time.Duration) (net.Conn, error) {
		tries.Add(1)
		return reach(n, addr, d)
	}
	want := []Peer{{Addr: "10.9.9.10:7762"}}
	for range 2 {
		got, err := m.Peers(context.Background())
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Peers = %v, %v; want %v", got, err, want)
		}
		m.mu.Lock()
		m.at = time.Time{} // fetch again; the probe stays
		m.mu.Unlock()
	}
	// fd00::9, 10.9.9.9 and spark-9 refused, 10.9.9.10 taken; once.
	if n := tries.Load(); n != 4 {
		t.Fatalf("%d connection attempts, want 4", n)
	}
	if got := (mentatPeer{LinkIP: "[fd00::9]", NodeIP: "::ffff:10.9.9.9", Addrs: []string{"10.9.9.9", "bad host", "0.0.0.0", "spark-9"}}).candidates(); !reflect.DeepEqual(got, []string{"fd00::9", "10.9.9.9", "spark-9"}) {
		t.Fatalf("candidates = %v", got)
	}
}

func TestMentatPeers(t *testing.T) {
	got, err := newMentat(t, fakeMentatd(t, http.StatusOK, mentatStatusJSON)+"/").Peers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Alive peers in node ID order, stale or not, each at its link_ip (the
	// address a working link from this box uses), else node_ip, else the
	// first IP in addrs; once each. Not dead ones, ones with no address,
	// or this box.
	want := []Peer{{Addr: "10.0.0.2:7762"}, {Addr: "10.0.0.3:7762"}, {Addr: "10.0.0.5:7762"}, {Addr: "[fd00::6]:7762"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Peers = %v, want %v", got, want)
	}
}

func TestMentatErrors(t *testing.T) {
	for name, c := range map[string]struct {
		url  string
		want string
	}{
		"down":     {"http://127.0.0.1:1", "mentat discovery"},
		"status":   {fakeMentatd(t, http.StatusServiceUnavailable, ""), "503"},
		"not json": {fakeMentatd(t, http.StatusOK, "ok\n"), "parse"},
		"no id":    {fakeMentatd(t, http.StatusOK, `{"peers": {}}`), "not a mentatd status"},
		// model-loader's own /status has a node_id too.
		"model-loader": {fakeMentatd(t, http.StatusOK, `{"node_id": "spark", "roles": ["peer"], "models": []}`), "not a mentatd status"},
		"null peers":   {fakeMentatd(t, http.StatusOK, `{"node_id": "a", "peers": null}`), "not a mentatd status"},
		"too big":      {fakeMentatd(t, http.StatusOK, `{"node_id": "a", "x": "`+strings.Repeat("x", mentatStatusLimit)+`"}`), "over"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newMentat(t, c.url).Peers(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Peers error = %v, want one with %q", err, c.want)
			}
		})
	}
}

// With no other boxes, mentatd's peer table is empty: no peers, no error.
func TestMentatAlone(t *testing.T) {
	got, err := newMentat(t, fakeMentatd(t, http.StatusOK, `{"node_id": "a", "peers": {}}`)).Peers(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("Peers = %v, %v; want none", got, err)
	}
}

func TestNewMentatRejects(t *testing.T) {
	for _, u := range []string{"", "127.0.0.1:6380", "ftp://127.0.0.1", "http://", "http:///status", "http://h/?x=1", "::",
		"http://127.0.0.1:65536", "http://127.0.0.1:0", "http://h:6380?", "http://u@h:6380", "http://h#x"} {
		if _, err := NewMentat(u, 7762); err == nil {
			t.Errorf("NewMentat(%q) accepted", u)
		}
	}
	for _, port := range []int{0, -1, 65536} {
		if _, err := NewMentat(DefaultMentatURL, port); err == nil {
			t.Errorf("NewMentat port %d accepted", port)
		}
	}
	for in, want := range map[string]string{
		"https://[fd00::1]:6380/": "https://[fd00::1]:6380/status",
		"http://h:6380/base/":     "http://h:6380/base/status",
		"http://h":                "http://h/status",
	} {
		m, err := NewMentat(in, 7762)
		if err != nil || m.statusURL != want {
			t.Errorf("NewMentat(%q): %v, status URL %v; want %s", in, err, m, want)
		}
	}
}

// Callers that overlap share one fetch, and its answer serves for
// mentatTTL; a caller whose ctx ends stops waiting without ending the
// fetch the others wait on.
func TestMentatSharesFetches(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		_, _ = w.Write([]byte(mentatStatusJSON))
	}))
	defer ts.Close()
	m := newMentat(t, ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	quit := make(chan error, 1)
	go func() {
		_, err := m.Peers(ctx)
		quit <- err
	}()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := m.Peers(context.Background())
			if err == nil && len(p) != 4 {
				err = errors.New("wrong peers")
			}
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-quit; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller: %v", err)
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Peers(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("mentatd asked %d times, want once", n)
	}
	m.mu.Lock()
	m.at = time.Now().Add(-mentatTTL)
	m.mu.Unlock()
	if _, err := m.Peers(context.Background()); err != nil || calls.Load() != 2 {
		t.Fatalf("after the TTL: %v, %d calls; want a second fetch", err, calls.Load())
	}
}

// A box's model-loader record gives the port to reach it at. A box with
// none, or with a record that does not give a valid port, is reached at
// the default, and a malformed record costs only that box its port.
func TestMentatRecordPort(t *testing.T) {
	body := `{"node_id": "a", "peers": {
	  "b": {"node_ip": "10.0.0.2", "alive": true, "records": {"model-loader": {"port": 9000}, "gr8": "x"}},
	  "c": {"node_ip": "10.0.0.3", "alive": true, "records": {"model-loader": "not an object"}},
	  "d": {"node_ip": "10.0.0.4", "alive": true, "records": {"model-loader": {"port": 70000}}},
	  "e": {"node_ip": "10.0.0.5", "alive": true}}}`
	got, err := newMentat(t, fakeMentatd(t, http.StatusOK, body)).Peers(context.Background())
	want := []Peer{{Addr: "10.0.0.2:9000"}, {Addr: "10.0.0.3:7762"}, {Addr: "10.0.0.4:7762"}, {Addr: "10.0.0.5:7762"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Peers = %v, %v; want %v", got, err, want)
	}
}

// Publish sends the record sorted, halves its holdings while mentatd has
// no room (413) and says it did, and deletes it for nil.
func TestMentatPublish(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var methods []string
	const room = 200
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/records/model-loader" {
			http.NotFound(w, r)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		methods, bodies = append(methods, r.Method), append(bodies, string(b))
		mu.Unlock()
		if len(b) > room {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	m := newMentat(t, ts.URL)

	small := &MentatRecord{Port: 7762, Roles: []string{"peer"}, Holdings: []MentatHolding{
		{"b/m", "r1", "ready"}, {"a/m", "r2", "partial"}, {"a/m", "r1", "ready"},
	}}
	if err := m.Publish(context.Background(), small); err != nil {
		t.Fatal(err)
	}
	if want := `{"port":7762,"roles":["peer"],"holdings":[{"model":"a/m","revision":"r1","state":"ready"},{"model":"a/m","revision":"r2","state":"partial"},{"model":"b/m","revision":"r1","state":"ready"}]}`; bodies[0] != want {
		t.Fatalf("sent %s, want %s", bodies[0], want)
	}
	if small.Holdings[0].Model != "b/m" {
		t.Fatal("Publish reordered the caller's holdings")
	}

	big := &MentatRecord{Port: 7762}
	for i := range 8 {
		big.Holdings = append(big.Holdings, MentatHolding{"org/model", strings.Repeat("r", 10) + string(rune('a'+i)), "ready"})
	}
	bodies = nil
	if err := m.Publish(context.Background(), big); err != nil {
		t.Fatal(err)
	}
	var sent MentatRecord
	last := bodies[len(bodies)-1]
	if err := json.Unmarshal([]byte(last), &sent); err != nil || len(last) > room || !sent.Truncated || len(sent.Holdings) == 0 {
		t.Fatalf("after %d tries sent %s; want one under %d bytes, truncated, with holdings", len(bodies), last, room)
	}

	// No holdings is an empty list, not null.
	if err := m.Publish(context.Background(), &MentatRecord{Port: 1}); err != nil || !strings.Contains(bodies[len(bodies)-1], `"holdings":[]`) {
		t.Fatalf("Publish with no holdings = %v, sent %s", err, bodies[len(bodies)-1])
	}

	// A record that does not fit with no holdings is an error.
	huge := &MentatRecord{Port: 7762, Roles: []string{strings.Repeat("x", room)}}
	if err := m.Publish(context.Background(), huge); err == nil || !strings.Contains(err.Error(), "413") {
		t.Fatalf("Publish of a record with no room = %v, want a 413 error", err)
	}

	if err := m.Publish(context.Background(), nil); err != nil || methods[len(methods)-1] != http.MethodDelete {
		t.Fatalf("Publish(nil) = %v, last method %s; want a DELETE", err, methods[len(methods)-1])
	}
}
