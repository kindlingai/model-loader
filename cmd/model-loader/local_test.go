package main

import (
	"net"
	"os"
	"path/filepath"
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
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close() // stale now
	ln2, err := listenLocal("unix:s.sock", dir)
	if err != nil {
		t.Fatalf("stale socket not replaced: %v", err)
	}
	ln2.Close()
}
