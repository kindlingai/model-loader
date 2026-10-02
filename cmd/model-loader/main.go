// Command model-loader runs one node of the model-loader mesh: it reconciles
// a catalog of models against local disk, pulling from LAN peers first and
// WAN only when root-eligible, and serves what it has to other nodes over
// HTTP. See docs/design.md and docs/protocol.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/daemon"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
)

// defaultPort matches docs/protocol.md.
const defaultPort = 7762

// version is set at release build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "model-loader:", err)
		os.Exit(1)
	}
}

type config struct {
	showVersion bool
	mode        string
	storeDir    string
	catalogPath string
	selPath     string
	listen      string
	nodeID      string

	discoveryKind string
	staticPeers   string
	k8sService    string
	k8sPort       int

	reconcileInterval time.Duration
	maxConcurrent     int
}

func parseFlags(args []string) (*config, error) {
	fs := flag.NewFlagSet("model-loader", flag.ContinueOnError)
	cfg := &config{}
	fs.StringVar(&cfg.mode, "mode", "", "node role: root, peer, or both (required)")
	fs.StringVar(&cfg.storeDir, "store", "/var/lib/model-loader", "local content store directory")
	fs.StringVar(&cfg.catalogPath, "catalog", "", "path to the fleet-wide catalog.yaml (required)")
	fs.StringVar(&cfg.selPath, "selection", "", "path to this node's selection.yaml (default: everything in the catalog)")
	fs.StringVar(&cfg.listen, "listen", fmt.Sprintf(":%d", defaultPort), "address to serve the HTTP API on")
	fs.StringVar(&cfg.nodeID, "node-id", "", "this node's identifier (default: hostname)")
	fs.StringVar(&cfg.discoveryKind, "discovery", "static", "peer discovery backend: static or k8s")
	fs.StringVar(&cfg.staticPeers, "peers", "", "comma-separated host:port list, for -discovery=static")
	fs.StringVar(&cfg.k8sService, "k8s-service", "", "headless Service DNS name, for -discovery=k8s")
	fs.IntVar(&cfg.k8sPort, "k8s-port", defaultPort, "port other nodes listen on, for -discovery=k8s")
	fs.DurationVar(&cfg.reconcileInterval, "reconcile-interval", 30*time.Second, "how often to re-check the catalog against local disk")
	fs.IntVar(&cfg.maxConcurrent, "max-concurrent-transfers", 2, "maximum simultaneous file downloads")
	fs.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if cfg.showVersion {
		return cfg, nil
	}

	var errs []error
	if _, err := parseRoles(cfg.mode); err != nil {
		errs = append(errs, err)
	}
	if cfg.catalogPath == "" {
		errs = append(errs, errors.New("-catalog is required"))
	}
	switch cfg.discoveryKind {
	case "static", "k8s":
	default:
		errs = append(errs, fmt.Errorf("-discovery: unknown backend %q (want static or k8s)", cfg.discoveryKind))
	}
	if cfg.discoveryKind == "k8s" && cfg.k8sService == "" {
		errs = append(errs, errors.New("-k8s-service is required when -discovery=k8s"))
	}
	return cfg, errors.Join(errs...)
}

func parseRoles(mode string) ([]string, error) {
	switch mode {
	case "root":
		return []string{"root"}, nil
	case "peer":
		return []string{"peer"}, nil
	case "both":
		return []string{"root", "peer"}, nil
	default:
		return nil, fmt.Errorf("-mode: must be root, peer, or both (got %q)", mode)
	}
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	if cfg.showVersion {
		fmt.Println("model-loader", version)
		return nil
	}
	roles, err := parseRoles(cfg.mode)
	if err != nil {
		return err
	}
	nodeID := cfg.nodeID
	if nodeID == "" {
		nodeID, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("determine node id: %w", err)
		}
	}

	cat, err := catalog.Load(cfg.catalogPath)
	if err != nil {
		return err
	}
	selection := &catalog.Selection{Wanted: []string{"*"}}
	if cfg.selPath != "" {
		selection, err = catalog.LoadSelection(cfg.selPath)
		if err != nil {
			return err
		}
	}

	st, err := store.Open(cfg.storeDir)
	if err != nil {
		return err
	}

	disc, err := buildDiscoverer(cfg)
	if err != nil {
		return err
	}

	d := daemon.New(daemon.Config{
		NodeID:                 nodeID,
		Roles:                  roles,
		Store:                  st,
		Catalog:                cat,
		Selection:              selection,
		Sources:                source.New(nil),
		Discoverer:             disc,
		MaxConcurrentTransfers: cfg.maxConcurrent,
	})

	srv := server.New(st, d)
	httpSrv := &http.Server{Addr: cfg.listen, Handler: srv}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("model-loader %s: node %q (%s) serving on %s", version, nodeID, strings.Join(roles, "+"), cfg.listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	go d.Run(ctx, cfg.reconcileInterval, func(err error) {
		log.Printf("model-loader: reconcile: %v", err)
	})

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down http server: %w", err)
	}
	return <-errCh
}

func buildDiscoverer(cfg *config) (discovery.Discoverer, error) {
	switch cfg.discoveryKind {
	case "k8s":
		return discovery.NewK8sDNS(cfg.k8sService, cfg.k8sPort), nil
	default:
		var addrs []string
		for _, a := range strings.Split(cfg.staticPeers, ",") {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if _, _, err := net.SplitHostPort(a); err != nil {
				return nil, fmt.Errorf("-peers: invalid address %q: %w", a, err)
			}
			addrs = append(addrs, a)
		}
		return discovery.NewStatic(addrs), nil
	}
}
