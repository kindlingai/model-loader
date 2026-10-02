//go:build linux

package store

import (
	"os"

	"golang.org/x/sys/unix"
)

// Uncache advises the kernel to drop the given byte range of f's page cache
// (after it has been fsync'd by the caller). Large downloads would otherwise
// leave the whole transferred file resident in page cache; on unified-memory
// hardware that competes directly with GPU-visible RAM, so clean pages are
// dropped as soon as they're durable rather than left for the kernel to
// reclaim under pressure.
func Uncache(f *os.File, offset, length int64) error {
	return unix.Fadvise(int(f.Fd()), offset, length, unix.FADV_DONTNEED)
}
