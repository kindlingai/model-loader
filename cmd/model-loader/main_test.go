package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
)

func TestParseRoles(t *testing.T) {
	cases := []struct {
		mode    string
		want    []string
		wantErr bool
	}{
		{"root", []string{"root"}, false},
		{"peer", []string{"peer"}, false},
		{"both", []string{"root", "peer"}, false},
		{"", nil, true},
		{"bogus", nil, true},
	}
	for _, tc := range cases {
		got, err := parseRoles(tc.mode)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseRoles(%q): err = %v, wantErr %v", tc.mode, err, tc.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("parseRoles(%q) = %v, want %v", tc.mode, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parseRoles(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		}
	}
}

func TestParseFlagsRequiresModeAndCatalog(t *testing.T) {
	if _, err := parseFlags(nil); err == nil {
		t.Fatal("expected an error when -mode and -catalog are both missing")
	}
	if _, err := parseFlags([]string{"-mode=root"}); err == nil {
		t.Fatal("expected an error when -catalog is missing")
	}
	if _, err := parseFlags([]string{"-catalog=/tmp/catalog.yaml"}); err == nil {
		t.Fatal("expected an error when -mode is missing")
	}
	if _, err := parseFlags([]string{"-mode=bogus", "-catalog=/tmp/catalog.yaml"}); err == nil {
		t.Fatal("expected an error for an invalid -mode")
	}
	if _, err := parseFlags([]string{"-mode=root", "-catalog=/tmp/catalog.yaml", "-dedup=link"}); err == nil {
		t.Fatal("expected an error for an unknown -dedup")
	}
}

func TestParseFlagsK8sRequiresService(t *testing.T) {
	_, err := parseFlags([]string{"-mode=root", "-catalog=/tmp/catalog.yaml", "-discovery=k8s"})
	if err == nil {
		t.Fatal("expected an error when -discovery=k8s is set without -k8s-service")
	}
}

func TestParseFlagsVersionSkipsRequiredFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-version"})
	if err != nil {
		t.Fatalf("-version alone should parse without -mode/-catalog: %v", err)
	}
	if !cfg.showVersion {
		t.Fatal("showVersion = false, want true")
	}
}

func TestParseFlagsValid(t *testing.T) {
	cfg, err := parseFlags([]string{"-mode=both", "-catalog=/tmp/catalog.yaml", "-store=/tmp/store"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.mode != "both" || cfg.catalogPath != "/tmp/catalog.yaml" || cfg.storeDir != "/tmp/store" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestBuildDiscovererStatic(t *testing.T) {
	cfg := &config{discoveryKind: "static", staticPeers: "10.0.0.1:7762, 10.0.0.2:7762"}
	d, _, err := buildDiscoverer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.(*discovery.Static); !ok {
		t.Fatalf("got %T, want *discovery.Static", d)
	}
}

func TestBuildDiscovererStaticRejectsBadAddr(t *testing.T) {
	cfg := &config{discoveryKind: "static", staticPeers: "not-a-host-port"}
	if _, _, err := buildDiscoverer(cfg); err == nil {
		t.Fatal("expected an error for a malformed peer address")
	}
}

func TestBuildDiscovererK8s(t *testing.T) {
	cfg := &config{discoveryKind: "k8s", k8sService: "model-loader-headless", k8sPort: 7762}
	d, _, err := buildDiscoverer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.(*discovery.K8sDNS); !ok {
		t.Fatalf("got %T, want *discovery.K8sDNS", d)
	}
}

// On kindling-spark-os, peers come from the local mentatd with no static
// list; -peers adds more, such as a NAS with no mentatd.
func TestBuildDiscovererMentat(t *testing.T) {
	mentatd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"node_id": "a", "peers": {"b": {"node_ip": "10.0.0.2", "alive": true}}}`))
	}))
	defer mentatd.Close()
	cfg, err := parseFlags([]string{"-mode", "peer", "-catalog", "c.yaml", "-discovery", "mentat", "-mentat-url", mentatd.URL})
	if err != nil {
		t.Fatal(err)
	}
	for peers, want := range map[string][]string{
		"":              {"10.0.0.2:7762"},
		"nas.lan:7762":  {"10.0.0.2:7762", "nas.lan:7762"},
		"10.0.0.2:7762": {"10.0.0.2:7762"},
	} {
		cfg.staticPeers = peers
		d, _, err := buildDiscoverer(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.Peers(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var addrs []string
		for _, p := range got {
			addrs = append(addrs, p.Addr)
		}
		if !reflect.DeepEqual(addrs, want) {
			t.Fatalf("-peers %q: Peers = %v, want %v", peers, addrs, want)
		}
	}
	for _, bad := range [][]string{{"-mentat-port", "0"}, {"-mentat-url", "http://"}, {"-mentat-url", ""}, {"-mentat-url", "127.0.0.1:6380"}} {
		args := append([]string{"-mode", "peer", "-catalog", "c.yaml", "-discovery", "mentat", "-peers", "nas.lan:7762"}, bad...)
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}

// -peers adds boxes with -discovery=k8s too (a NAS root outside the
// cluster), and they stay listed when the backend fails.
func TestBuildDiscovererK8sPeers(t *testing.T) {
	cfg, err := parseFlags([]string{"-mode", "peer", "-catalog", "c.yaml", "-discovery", "k8s",
		"-k8s-service", "model-loader-headless.invalid", "-peers", "nas.lan:7762"})
	if err != nil {
		t.Fatal(err)
	}
	d, _, err := buildDiscoverer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Peers(context.Background())
	if err != nil || len(got) != 1 || got[0].Addr != "nas.lan:7762" {
		t.Fatalf("Peers = %v, %v; want nas.lan:7762", got, err)
	}
	cfg.staticPeers = "nas.lan"
	if _, _, err := buildDiscoverer(cfg); err == nil {
		t.Fatal("-peers with no port accepted")
	}
}

// The record holds what the box serves: revisions ready or partial, not
// ones still pending, downloading or failed.
func TestMentatRecord(t *testing.T) {
	st := server.StatusResponse{Roles: []string{"peer"}, Models: []server.ModelStatus{
		{Model: "a", Revision: "1", State: server.StateReady},
		{Model: "b", Revision: "1", State: server.StatePartial},
		{Model: "c", Revision: "1", State: server.StateDownloading},
		{Model: "d", Revision: "1", State: server.StateError},
	}}
	got := mentatRecord(st, 7762)
	want := &discovery.MentatRecord{Port: 7762, Roles: []string{"peer"}, Holdings: []discovery.MentatHolding{
		{Model: "a", Revision: "1", State: "ready"}, {Model: "b", Revision: "1", State: "partial"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mentatRecord = %+v, want %+v", got, want)
	}
}
