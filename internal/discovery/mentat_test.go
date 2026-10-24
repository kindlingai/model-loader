package discovery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// A mentatd /status answer, cut down from a real one to the fields Mentat
// reads plus a few it must ignore.
const mentatStatusJSON = `{
  "proto": 7,
  "node_id": "aaaaaaaaaa",
  "node_ip": "10.0.0.1",
  "control_port": 6379,
  "peers": {
    "cccccccccc": {"node_ip": "10.0.0.3", "alive": true, "stale": true, "http_port": 6380, "groups": null},
    "bbbbbbbbbb": {"node_ip": "10.0.0.2", "alive": true, "stale": false, "http_port": 6380, "probes": {}},
    "dddddddddd": {"node_ip": "10.0.0.4", "alive": false, "dead_since_ms": 1},
    "eeeeeeeeee": {"node_ip": "", "alive": true},
    "ffffffffff": {"node_ip": "fd00::6", "alive": true},
    "aaaaaaaaaa": {"node_ip": "10.0.0.1", "alive": true}
  },
  "clients": {}
}`

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

func TestMentatPeers(t *testing.T) {
	url := fakeMentatd(t, http.StatusOK, mentatStatusJSON)
	got, err := NewMentat(url+"/", 7762).Peers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Alive peers in node ID order, stale or not; not dead ones, ones with
	// no address, or this box.
	want := []Peer{{Addr: "10.0.0.2:7762"}, {Addr: "10.0.0.3:7762"}, {Addr: "[fd00::6]:7762"}}
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
		"not ours": {fakeMentatd(t, http.StatusOK, `{"peers": {}}`), "no node_id"},
		"too big":  {fakeMentatd(t, http.StatusOK, `{"node_id": "a", "x": "`+strings.Repeat("x", mentatStatusLimit)+`"}`), "over"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewMentat(c.url, 7762).Peers(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Peers error = %v, want one with %q", err, c.want)
			}
		})
	}
}

// With no other boxes, mentatd's peer table is empty: no peers, no error.
func TestMentatAlone(t *testing.T) {
	got, err := NewMentat(fakeMentatd(t, http.StatusOK, `{"node_id": "a", "peers": {}}`), 7762).Peers(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("Peers = %v, %v; want none", got, err)
	}
}
