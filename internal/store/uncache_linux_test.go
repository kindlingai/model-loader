//go:build linux

package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentPages reports how many of path's pages are in page cache.
func residentPages(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(data)
	pageSize := os.Getpagesize()
	vec := make([]byte, (len(data)+pageSize-1)/pageSize)
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		t.Fatal(errno)
	}
	n := 0
	for _, v := range vec {
		n += int(v & 1)
	}
	return n
}

// cachedFile writes content to a durable file and reads it back, so every
// page starts out resident the way it is right after a download.
func cachedFile(t *testing.T, content []byte) string {
	t.Helper()
	dir := t.TempDir()
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	if fs.Type == unix.TMPFS_MAGIC {
		t.Skip("tmpfs pages are not page cache; fadvise cannot drop them")
	}
	path := filepath.Join(dir, "weights.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if residentPages(t, path) == 0 {
		t.Skip("filesystem does not keep this file in page cache")
	}
	return path
}

// Hashing a file to build or check a manifest must not leave it in page
// cache: on unified-memory hardware that cache competes with GPU-visible RAM.
func TestHashReadsDropPageCache(t *testing.T) {
	content := bytes.Repeat([]byte("w"), 4<<20)

	path := cachedFile(t, content)
	m := &Manifest{Model: "m", Revision: "r", Files: []FileManifest{{Path: "weights.bin", Size: int64(len(content))}}}
	if err := m.BuildSegmentHashes(func(string) (io.ReadCloser, error) { return os.Open(path) }); err != nil {
		t.Fatal(err)
	}
	if n := residentPages(t, path); n != 0 {
		t.Fatalf("BuildSegmentHashes left %d pages resident, want 0", n)
	}

	for name, verify := range map[string]func(f *os.File) (bool, error){
		"VerifySegment":   func(f *os.File) (bool, error) { return VerifySegment(f, m.Files[0], 0) },
		"VerifyWholeFile": func(f *os.File) (bool, error) { return VerifyWholeFile(f, m.Files[0]) },
	} {
		path := cachedFile(t, content)
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := verify(f)
		f.Close()
		if err != nil || !ok {
			t.Fatalf("%s = %v, %v; want true", name, ok, err)
		}
		if n := residentPages(t, path); n != 0 {
			t.Fatalf("%s left %d pages resident, want 0", name, n)
		}
	}
}
