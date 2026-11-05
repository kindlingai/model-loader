package daemon

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kindlingai/model-loader/internal/transfer"
)

// pullStatsEvery is how often a root logs where its origin transfers
// spent their time, when it made any.
const pullStatsEvery = time.Minute

// logPullStats logs, every period until ctx ends, what d's pull-through
// transfers did in it: how many, how fast, and per transfer how long each
// stage took (kindlingai/model-loader#49). A stage much slower than read
// is what holds the slots.
func (d *Daemon) logPullStats(ctx context.Context, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if s := d.pullStats.Take(); s.Transfers > 0 {
			log.Print(formatPullStats(s, period))
		}
	}
}

func formatPullStats(s transfer.StatsSnapshot, period time.Duration) string {
	per := func(v time.Duration) time.Duration {
		return (v / time.Duration(s.Transfers)).Round(time.Millisecond)
	}
	mib := float64(s.Bytes) / (1 << 20)
	return fmt.Sprintf("model-loader: pull-through, last %s: %d transfers, %.0f MiB, %.1f MiB/s; per transfer: wait %s, read %s, write %s, hash %s, sync %s, save %s",
		period, s.Transfers, mib, mib/period.Seconds(),
		per(s.Wait), per(s.Read), per(s.Write), per(s.Hash), per(s.Sync), per(s.Save))
}
