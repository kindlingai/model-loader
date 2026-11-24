package daemon

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/server"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/transfer"
)

// relaySegmentsTTL is how long a relay reuses what a node it relays for
// said it holds, so a burst of askers costs that node one /segments.
const relaySegmentsTTL = 5 * time.Second

type cachedRelayed struct {
	holder *server.RelayedHolder // nil: the node did not answer, or holds nothing
	at     time.Time
}

// RelayedSegments implements server.Relayer: what each node this one
// relays for (Config.RelayFor) holds of model@revision. Each is asked with
// transfer.WithRelayed, so it answers with what it holds itself.
func (d *Daemon) RelayedSegments(ctx context.Context, model, revision string) []server.RelayedHolder {
	out := make([]*server.RelayedHolder, len(d.cfg.RelayFor))
	var wg sync.WaitGroup
	for k, addr := range d.cfg.RelayFor {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[k] = d.relayedAt(ctx, addr, model, revision)
		}()
	}
	wg.Wait()
	var holders []server.RelayedHolder
	for _, h := range out {
		if h != nil {
			holders = append(holders, *h)
		}
	}
	return holders
}

// relayedAt returns what the node at addr holds of model@revision, from
// an answer at most relaySegmentsTTL old, or nil.
func (d *Daemon) relayedAt(ctx context.Context, addr, model, revision string) *server.RelayedHolder {
	key := addr + "\x00" + model + "\x00" + revision
	d.relayMu.Lock()
	c, ok := d.relayed[key]
	d.relayMu.Unlock()
	if ok && time.Since(c.at) < relaySegmentsTTL {
		return c.holder
	}
	v, _ := d.once(ctx, "relayed "+key, func() (any, error) {
		// Its own clock: the asker that started it may go away while
		// others wait on it.
		qctx, cancel := context.WithTimeout(transfer.WithRelayed(context.WithoutCancel(ctx)), availabilityTimeout)
		defer cancel()
		var h *server.RelayedHolder
		resp, err := d.cfg.Peers.Segments(qctx, addr, model, revision)
		if err == nil && resp.Model == model && resp.Revision == revision && resp.NodeID != d.cfg.NodeID && len(resp.Files) > 0 {
			h = &server.RelayedHolder{NodeID: resp.NodeID, Files: resp.Files}
		}
		d.relayMu.Lock()
		d.relayed[key] = cachedRelayed{h, time.Now()}
		d.relayMu.Unlock()
		return h, nil
	})
	h, _ := v.(*server.RelayedHolder)
	return h
}

// Relay implements server.Relayer. It holds one of this node's
// -max-concurrent-transfers slots, at the asker's priority, from before
// it asks a node it relays for until the caller calls the function it
// returns. It asks the nodes the cached answers say hold a segment of
// path first, then the others, and returns the first that serves the
// range. ErrNotRelayed means none did; any other error, that one failed
// and none served it.
func (d *Daemon) Relay(ctx context.Context, model, revision, path string, r *http.Request) (*http.Response, func(), error) {
	if len(d.cfg.RelayFor) == 0 {
		return nil, nil, server.ErrNotRelayed
	}
	addrs := d.relayOrder(model, revision, path)
	if len(addrs) == 0 {
		return nil, nil, server.ErrNotRelayed
	}
	p, _ := strconv.Atoi(r.Header.Get(server.HeaderPriority))
	if err := d.limit.Acquire(ctx, p); err != nil {
		return nil, nil, err
	}
	var errs []error
	for _, addr := range addrs {
		resp, err := d.cfg.Peers.RelayBlob(ctx, addr, model, revision, path, r)
		if err == nil {
			return resp, d.limit.Release, nil
		}
		if errors.Is(err, store.ErrCheckFailed) {
			// The holder's bytes are what they are; another holder's
			// would be checked the same way.
			d.limit.Release()
			return nil, nil, err
		}
		if !errors.Is(err, transfer.ErrRangeUnavailable) {
			errs = append(errs, err)
		}
	}
	d.limit.Release()
	if len(errs) > 0 {
		return nil, nil, errors.Join(errs...)
	}
	return nil, nil, server.ErrNotRelayed
}

// relayOrder returns the nodes this one relays for to ask for path of
// model@revision: those whose last answer holds some of it, then those
// with no answer cached. One whose cached answer holds none of the file
// is left out; it is asked again once the answer is relaySegmentsTTL old.
func (d *Daemon) relayOrder(model, revision, path string) []string {
	var holders, unknown []string
	d.relayMu.Lock()
	defer d.relayMu.Unlock()
	for _, addr := range d.cfg.RelayFor {
		c, ok := d.relayed[addr+"\x00"+model+"\x00"+revision]
		switch {
		case !ok || time.Since(c.at) >= relaySegmentsTTL:
			unknown = append(unknown, addr)
		case c.holder != nil:
			if _, has := c.holder.Files[path]; has {
				holders = append(holders, addr)
			}
		}
	}
	return append(holders, unknown...)
}

// ParseRelayFor parses -relay, a comma-separated host:port list.
func ParseRelayFor(list string) []string {
	var out []string
	for _, a := range strings.Split(list, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
