package store

import (
	"os"
	"testing"
)

// TestMain keeps per-segment bookkeeping (a chunk of one segment) for the
// tiny segments most tests here use; tests of small segments set
// ChunkSize themselves.
func TestMain(m *testing.M) {
	ChunkSize = 1
	os.Exit(m.Run())
}
