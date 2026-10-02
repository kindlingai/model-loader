//go:build !linux

package store

import "os"

// Uncache is a no-op on platforms without posix_fadvise(DONTNEED) (notably
// macOS, used for local development). Production targets are Linux.
func Uncache(f *os.File, offset, length int64) error {
	return nil
}
