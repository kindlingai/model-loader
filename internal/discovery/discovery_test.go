package discovery

import (
	"context"
	"errors"
	"sort"
	"testing"
)

func addrs(peers []Peer) []string {
	out := make([]string, len(peers))
	for i, p := range peers {
		out[i] = p.Addr
	}
	sort.Strings(out)
	return out
}

func TestStaticPeers(t *testing.T) {
	s := NewStatic([]string{"10.0.0.1:7762", "10.0.0.2:7762"})
	peers, err := s.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	got := addrs(peers)
	want := []string{"10.0.0.1:7762", "10.0.0.2:7762"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

type fakeResolver struct {
	hosts map[string][]string
	err   error
}

func (f fakeResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.hosts[host], nil
}

func TestK8sDNSPeers(t *testing.T) {
	k := &K8sDNS{
		resolver: fakeResolver{hosts: map[string][]string{
			"model-loader-headless.default.svc.cluster.local": {"10.1.2.3", "10.1.2.4"},
		}},
		service: "model-loader-headless.default.svc.cluster.local",
		port:    7762,
	}
	peers, err := k.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	got := addrs(peers)
	want := []string{"10.1.2.3:7762", "10.1.2.4:7762"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestK8sDNSPeersError(t *testing.T) {
	k := &K8sDNS{resolver: fakeResolver{err: errors.New("no such host")}, service: "missing", port: 1}
	if _, err := k.Peers(context.Background()); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestMultiDedupesAndTolerateFailures(t *testing.T) {
	good := NewStatic([]string{"10.0.0.1:7762", "10.0.0.2:7762"})
	overlap := NewStatic([]string{"10.0.0.2:7762", "10.0.0.3:7762"})
	failing := &K8sDNS{resolver: fakeResolver{err: errors.New("dns down")}, service: "x", port: 1}

	m := NewMulti(good, overlap, failing)
	peers, err := m.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	got := addrs(peers)
	want := []string{"10.0.0.1:7762", "10.0.0.2:7762", "10.0.0.3:7762"}
	if len(got) != 3 {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestMultiReturnsErrorOnlyWhenAllFail(t *testing.T) {
	failing1 := &K8sDNS{resolver: fakeResolver{err: errors.New("down")}, service: "x", port: 1}
	failing2 := &K8sDNS{resolver: fakeResolver{err: errors.New("down")}, service: "y", port: 1}
	m := NewMulti(failing1, failing2)
	if _, err := m.Peers(context.Background()); err == nil {
		t.Fatalf("expected error when every backend fails, got nil")
	}
}
