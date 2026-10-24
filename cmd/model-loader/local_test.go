package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestListenLocal(t *testing.T) {
	dir, err := os.MkdirTemp("", "ml") // short: unix socket paths are limited
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "file")
	os.WriteFile(file, []byte("keep"), 0o644)
	if _, err := listenLocal("unix:file", dir); err == nil {
		t.Fatal("listened over a regular file")
	}
	if got, _ := os.ReadFile(file); string(got) != "keep" {
		t.Fatal("regular file removed")
	}

	ln, err := listenLocal("unix:s.sock", dir)
	if err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(filepath.Join(dir, "s.sock")); info.Mode().Perm() != 0o660 {
		t.Fatalf("socket mode %v, want 0660", info.Mode().Perm())
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	if _, err := listenLocal("unix:s.sock", dir); err == nil {
		t.Fatal("took over a live socket")
	}
	ln.(*lockedListener).Listener.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close() // stale now
	ln2, err := listenLocal("unix:s.sock", dir)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	ln2.Close()

	// Many starting at once over one stale socket: exactly one wins.
	stale, err := net.Listen("unix", filepath.Join(dir, "r.sock"))
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	stale.Close()
	for round := range 20 {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var won []net.Listener
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if ln, err := listenLocal("unix:r.sock", dir); err == nil {
					mu.Lock()
					won = append(won, ln)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if len(won) != 1 {
			t.Fatalf("round %d: %d listeners won, want 1", round, len(won))
		}
		won[0].(*lockedListener).Listener.(*net.UnixListener).SetUnlinkOnClose(false)
		won[0].Close()
	}
}

// -check passes only for a local API that answers: not a missing socket,
// a stale one no process serves, or a server that answers an error.
func TestCheckLocal(t *testing.T) {
	dir, err := os.MkdirTemp("", "ml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := checkLocal("unix:s.sock", dir); err == nil {
		t.Fatal("check passed with no socket")
	}

	ln, err := listenLocal("unix:s.sock", dir)
	if err != nil {
		t.Fatal(err)
	}
	status := http.StatusOK
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/requests" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	})}
	go srv.Serve(ln)
	if err := checkLocal("unix:s.sock", dir); err != nil {
		t.Fatalf("check of a served socket: %v", err)
	}
	if err := checkLocal("unix:"+filepath.Join(dir, "s.sock"), "/nowhere"); err != nil {
		t.Fatalf("check by absolute path: %v", err)
	}
	status = http.StatusInternalServerError
	if err := checkLocal("unix:s.sock", dir); err == nil {
		t.Fatal("check passed on a 500")
	}

	// A socket left behind: the file is there, nothing serves it.
	stale := filepath.Join(dir, "stale.sock")
	l, err := net.Listen("unix", stale)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	l.Close()
	if _, err := os.Stat(stale); err != nil {
		t.Fatal(err)
	}
	if err := checkLocal("unix:stale.sock", dir); err == nil {
		t.Fatal("check passed on a stale socket")
	}
	srv.Close()
}

// -check needs no other flags, as a probe has none of the daemon's.
func TestCheckFlag(t *testing.T) {
	cfg, err := parseFlags([]string{"-check", "unix:/run/x.sock"})
	if err != nil || cfg.check != "unix:/run/x.sock" {
		t.Fatalf("parseFlags: %+v, %v", cfg, err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/requests" {
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	addr := strings.TrimPrefix(ts.URL, "http://")
	if err := run([]string{"-check", addr}); err != nil {
		t.Fatalf("run -check of a served API: %v", err)
	}
	ts.Close()
	if err := run([]string{"-check", addr}); err == nil || !strings.Contains(err.Error(), "check "+addr) {
		t.Fatalf("run -check with nothing listening: %v, want a check error", err)
	}
}
