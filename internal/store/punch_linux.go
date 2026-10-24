//go:build linux

package store

import (
	"os"

	"golang.org/x/sys/unix"
)

// punchHole frees the disk blocks under [offset, offset+length) of f,
// keeping its size: the range then reads as zeros.
func punchHole(f *os.File, offset, length int64) error {
	return unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, offset, length)
}
