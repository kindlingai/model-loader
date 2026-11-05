package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/kindlingai/model-loader/internal/transfer"
)

// The minute's log line names each stage per transfer, so the stage that
// holds the slots stands out (#49).
func TestFormatPullStats(t *testing.T) {
	got := formatPullStats(transfer.StatsSnapshot{
		Transfers: 60, Bytes: 60 << 26,
		Wait: 6 * time.Second, Read: 60 * time.Second, Write: 3 * time.Second,
		Hash: 12 * time.Second, Sync: 48 * time.Second, Save: 600 * time.Millisecond,
	}, time.Minute)
	for _, want := range []string{"60 transfers", "3840 MiB", "64.0 MiB/s", "wait 100ms", "read 1s", "write 50ms", "hash 200ms", "sync 800ms", "save 10ms"} {
		if !strings.Contains(got, want) {
			t.Errorf("%q does not contain %q", got, want)
		}
	}
}
