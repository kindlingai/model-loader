package discovery

import (
	"context"
	"errors"
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
    "eeeeeeeeee": {"node_ip": "", "link_ip": "", "addrs": ["not-an-ip", "10.0.0.5"], "alive": true},
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

func newMentat(t *testing.T, url string) *Mentat {
	t.Helper()
	m, err := NewMentat(url, 7762)
	if err != nil {
		t.Fatal(err)
	}
	return m
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
	for _, u := range []string{"", "127.0.0.1:6380", "ftp://127.0.0.1", "http://", "http:///status", "http://h/?x=1", "::"} {
		if _, err := NewMentat(u, 7762); err == nil {
			t.Errorf("NewMentat(%q) accepted", u)
		}
	}
	for _, port := range []int{0, -1, 65536} {
		if _, err := NewMentat(DefaultMentatURL, port); err == nil {
			t.Errorf("NewMentat port %d accepted", port)
		}
	}
	if _, err := NewMentat("https://[fd00::1]:6380/", 7762); err != nil {
		t.Errorf("NewMentat with an IPv6 host: %v", err)
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
