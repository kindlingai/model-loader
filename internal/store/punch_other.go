//go:build !linux

package store

import "os"

// punchHole writes zeros over [offset, offset+length) of f on platforms
// without a portable hole punch (notably macOS, used for development). It
// does not free disk space there; production targets are Linux.
func punchHole(f *os.File, offset, length int64) error {
	_, err := f.WriteAt(make([]byte, length), offset)
	return err
}
