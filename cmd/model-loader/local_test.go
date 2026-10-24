package main

import (
	"net"
	"os"
	"path/filepath"
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
