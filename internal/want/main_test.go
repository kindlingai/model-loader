package want

import (
	"os"
	"testing"

	"github.com/kindlingai/model-loader/internal/store"
)

// TestMain keeps per-segment bookkeeping (a chunk of one segment) for the
// tiny segments most tests here use; tests of small segments set
// ChunkSize themselves.
func TestMain(m *testing.M) {
	store.ChunkSize = 1
	os.Exit(m.Run())
}
