//go:build !linux && !darwin

package store

import (
	"errors"
	"os"
)

// punchHole is not supported here: the space stays allocated, and the
// store's accounting, which counts allocated blocks, still charges it.
func punchHole(f *os.File, offset, length int64) error {
	return errors.New("punching holes is not supported on this platform")
}
