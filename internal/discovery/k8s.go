package discovery

import (
	"context"
	"fmt"
	"net"
	"strconv"
)

// hostLookuper is the subset of *net.Resolver used here, as an interface so
// tests can substitute a fake without a real DNS server.
type hostLookuper interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// K8sDNS discovers peers via a headless Service: Kubernetes answers an A/AAAA
// query for a headless Service's DNS name with every ready pod's IP
// directly, with no extra API server calls needed.
type K8sDNS struct {
	resolver hostLookuper
	service  string
	port     int
}

// NewK8sDNS builds a discoverer for a headless Service's DNS name (e.g.
// "model-loader-headless.default.svc.cluster.local"), combining each
// resolved IP with port.
func NewK8sDNS(service string, port int) *K8sDNS {
	return &K8sDNS{resolver: net.DefaultResolver, service: service, port: port}
}

func (k *K8sDNS) Peers(ctx context.Context) ([]Peer, error) {
	ips, err := k.resolver.LookupHost(ctx, k.service)
	if err != nil {
		return nil, fmt.Errorf("k8s discovery: lookup %s: %w", k.service, err)
	}
	peers := make([]Peer, len(ips))
	for i, ip := range ips {
		peers[i] = Peer{Addr: net.JoinHostPort(ip, strconv.Itoa(k.port))}
	}
	return peers, nil
}
