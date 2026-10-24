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
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/daemon"
	"github.com/kindlingai/model-loader/internal/discovery"
	"github.com/kindlingai/model-loader/internal/request"
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
	mentatURL     string
	mentatPort    int

	reconcileInterval time.Duration
	maxConcurrent     int
	pullWorkers       int
	verifyOnStart     bool
	localListen       string
	diskBudget        string
}

func parseFlags(args []string) (*config, error) {
	fs := flag.NewFlagSet("model-loader", flag.ContinueOnError)
	cfg := &config{}
	fs.StringVar(&cfg.mode, "mode", "", "node role: root, peer, or both (required)")
	fs.StringVar(&cfg.storeDir, "store", "/var/lib/model-loader", "local content store directory")
	fs.StringVar(&cfg.catalogPath, "catalog", "", "path to the fleet-wide catalog.yaml (required)")
	fs.StringVar(&cfg.selPath, "selection", "", "path to this node's selection.yaml, re-read every reconcile; a missing file selects nothing (default: everything in the catalog)")
	fs.StringVar(&cfg.listen, "listen", fmt.Sprintf(":%d", defaultPort), "address to serve the HTTP API on")
	fs.StringVar(&cfg.nodeID, "node-id", "", "this node's identifier (default: hostname)")
	fs.StringVar(&cfg.discoveryKind, "discovery", "static", "peer discovery backend: static, k8s, or mentat (the local mentatd's peer table, on kindling-spark-os)")
	fs.StringVar(&cfg.staticPeers, "peers", "", "comma-separated host:port list, for -discovery=static; with -discovery=mentat, more peers to add, such as a NAS that runs no mentatd")
	fs.StringVar(&cfg.mentatURL, "mentat-url", discovery.DefaultMentatURL, "the local mentatd's HTTP side port, for -discovery=mentat")
	fs.IntVar(&cfg.mentatPort, "mentat-port", defaultPort, "port model-loader listens on on every box, for -discovery=mentat")
	fs.StringVar(&cfg.k8sService, "k8s-service", "", "headless Service DNS name, for -discovery=k8s")
	fs.IntVar(&cfg.k8sPort, "k8s-port", defaultPort, "port other nodes listen on, for -discovery=k8s")
	fs.DurationVar(&cfg.reconcileInterval, "reconcile-interval", 30*time.Second, "how often to re-check the catalog against local disk")
	fs.IntVar(&cfg.maxConcurrent, "max-concurrent-transfers", 2, "maximum simultaneous segment transfers")
	fs.IntVar(&cfg.pullWorkers, "pull-workers", 4, "root only: maximum segments fetched from origins at once for pull-through")
	fs.BoolVar(&cfg.verifyOnStart, "verify-on-start", false, "re-hash every held segment of each model once at start, and fetch again any that no longer match")
	fs.StringVar(&cfg.diskBudget, "disk-budget", "", "most the store may hold, as bytes (500G, 2T) or a percentage of its filesystem (80%); fetches evict unwanted segments the fleet keeps copies of to stay under it. Empty: no budget, nothing evicted")
	fs.StringVar(&cfg.localListen, "local-listen", "unix:model-loader.sock", "where to serve the local request API: unix:PATH (relative to -store) or host:port; empty to turn it off")
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
	case "static", "k8s", "mentat":
	default:
		errs = append(errs, fmt.Errorf("-discovery: unknown backend %q (want static, k8s or mentat)", cfg.discoveryKind))
	}
	if cfg.discoveryKind == "mentat" && (cfg.mentatPort <= 0 || cfg.mentatPort > 65535) {
		errs = append(errs, fmt.Errorf("-mentat-port: %d is not a port", cfg.mentatPort))
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
		if errors.Is(err, fs.ErrNotExist) {
			// A node with no models picked yet is valid, not a crash; the
			// daemon re-reads the file every reconcile.
			log.Printf("model-loader: warning: %v; selecting no models until it exists", err)
			selection, err = &catalog.Selection{}, nil
		}
		if err != nil {
			return err
		}
	}

	st, err := store.Open(cfg.storeDir)
	if err != nil {
		return err
	}

	budget, err := store.ParseBudget(cfg.diskBudget, cfg.storeDir)
	if err != nil {
		return err
	}

	reqs, err := request.Open(filepath.Join(cfg.storeDir, "requests"))
	if err != nil {
		if reqs == nil {
			return err
		}
		log.Printf("model-loader: warning: %v", err) // the rest still load.
	}

	disc, err := buildDiscoverer(cfg)
	if err != nil {
		return err
	}

	d := daemon.New(daemon.Config{
		NodeID:        nodeID,
		Roles:         roles,
		Store:         st,
		Catalog:       cat,
		Selection:     selection,
		SelectionPath: cfg.selPath,
		// HF_TOKEN, as in huggingface_hub, authenticates gated or private
		// Hugging Face repos. Only a root ever uses it.
		Sources:                source.New(nil, strings.TrimSpace(os.Getenv("HF_TOKEN"))),
		Discoverer:             disc,
		MaxConcurrentTransfers: cfg.maxConcurrent,
		PullWorkers:            cfg.pullWorkers,
		VerifyOnStart:          cfg.verifyOnStart,
		Requests:               reqs,
		DiskBudget:             budget,
	})

	srv := server.New(st, d)
	if d.IsWANEligible() {
		// A root fetches from the origin what a peer asks it for and it
		// does not hold yet.
		srv.SetPuller(d)
	}
	httpSrv := &http.Server{Addr: cfg.listen, Handler: srv}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		log.Printf("model-loader %s: node %q (%s) serving on %s", version, nodeID, strings.Join(roles, "+"), cfg.listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	if cfg.localListen != "" {
		ln, err := listenLocal(cfg.localListen, cfg.storeDir)
		if err != nil {
			return err
		}
		localSrv := &http.Server{Handler: server.NewLocal(d)}
		defer localSrv.Close()
		go func() {
			log.Printf("model-loader: local request API on %s", ln.Addr())
			if err := localSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("local api: %w", err)
			}
		}()
	}

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
	case "mentat":
		m := discovery.NewMentat(cfg.mentatURL, cfg.mentatPort)
		if strings.TrimSpace(cfg.staticPeers) == "" {
			return m, nil
		}
		static, err := buildDiscoverer(&config{staticPeers: cfg.staticPeers})
		if err != nil {
			return nil, err
		}
		return discovery.NewMulti(m, static), nil
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

// listenLocal listens on addr for the local request API: "unix:PATH" is a
// unix socket, PATH relative to storeDir unless absolute; anything else is
// a TCP host:port.
//
// For a socket, an exclusive lock on PATH.lock is taken first and held
// until the listener closes, so two processes starting at once cannot both
// remove and bind the path. With the lock held, a socket left by an
// earlier run is removed, but only a socket that nothing answers on: a
// path that is not a socket, or one an older model-loader without the
// lock is serving, is an error. The socket is mode 0660, so the owner and
// its group (the consumer's, in a deployment) can use it.
func listenLocal(addr, storeDir string) (net.Listener, error) {
	path, ok := strings.CutPrefix(addr, "unix:")
	if !ok {
		return net.Listen("tcp", addr)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(storeDir, path)
	}
	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("local api: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("local api: %s is in use by another process", path)
	}
	ln, err := listenSocket(path)
	if err != nil {
		lock.Close()
		return nil, err
	}
	return &lockedListener{Listener: ln, lock: lock}, nil
}

func listenSocket(path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&fs.ModeSocket == 0 {
			return nil, fmt.Errorf("local api: %s exists and is not a socket", path)
		}
		if c, err := net.DialTimeout("unix", path, time.Second); err == nil {
			c.Close()
			return nil, fmt.Errorf("local api: %s is in use by another process", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("local api: remove stale socket: %w", err)
		}
	}
	// The mode is set before anything is served on it. (The umask is not
	// changed for this: it is the whole process's.)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("local api: %w", err)
	}
	if err := os.Chmod(path, 0o660); err != nil {
		ln.Close()
		return nil, fmt.Errorf("local api: %w", err)
	}
	return ln, nil
}

// lockedListener releases its socket's lock once it is closed.
type lockedListener struct {
	net.Listener
	lock *os.File
}

func (l *lockedListener) Close() error {
	err := l.Listener.Close()
	l.lock.Close()
	return err
}
