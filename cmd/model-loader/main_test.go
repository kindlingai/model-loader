package main

import (
	"testing"

	"github.com/kindlingai/model-loader/internal/discovery"
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
}

func TestParseFlagsK8sRequiresService(t *testing.T) {
	_, err := parseFlags([]string{"-mode=root", "-catalog=/tmp/catalog.yaml", "-discovery=k8s"})
	if err == nil {
		t.Fatal("expected an error when -discovery=k8s is set without -k8s-service")
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
	d, err := buildDiscoverer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.(*discovery.Static); !ok {
		t.Fatalf("got %T, want *discovery.Static", d)
	}
}

func TestBuildDiscovererStaticRejectsBadAddr(t *testing.T) {
	cfg := &config{discoveryKind: "static", staticPeers: "not-a-host-port"}
	if _, err := buildDiscoverer(cfg); err == nil {
		t.Fatal("expected an error for a malformed peer address")
	}
}

func TestBuildDiscovererK8s(t *testing.T) {
	cfg := &config{discoveryKind: "k8s", k8sService: "model-loader-headless", k8sPort: 7762}
	d, err := buildDiscoverer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.(*discovery.K8sDNS); !ok {
		t.Fatalf("got %T, want *discovery.K8sDNS", d)
	}
}
