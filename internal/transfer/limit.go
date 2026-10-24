package transfer

import (
	"container/heap"
	"context"
	"sync"
)

// Limiter bounds how many transfers run at once on a node, and admits the
// waiting transfer with the highest priority first (the oldest among
// equals) when a slot comes free. A running transfer is never stopped for
// a higher one.
type Limiter struct {
	mu      sync.Mutex
	size    int
	used    int
	seq     uint64
	waiting waiters
}

// NewLimiter returns a Limiter of n slots, at least one.
func NewLimiter(n int) *Limiter { return &Limiter{size: max(1, n)} }

// Cap returns how many slots l has.
func (l *Limiter) Cap() int { return l.size }

// Acquire waits for a slot, or for ctx to end. The caller releases a slot
// it got with Release.
func (l *Limiter) Acquire(ctx context.Context, priority int) error {
	l.mu.Lock()
	if l.used < l.size && len(l.waiting) == 0 {
		l.used++
		l.mu.Unlock()
		return nil
	}
	w := &waiter{priority: priority, seq: l.seq, ready: make(chan struct{})}
	l.seq++
	heap.Push(&l.waiting, w)
	l.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		defer l.mu.Unlock()
		if w.index < 0 {
			// Granted just as ctx ended: pass the slot on.
			l.releaseLocked()
		} else {
			heap.Remove(&l.waiting, w.index)
		}
		return ctx.Err()
	}
}

// Release gives back a slot, to the highest-priority waiter if there is
// one.
func (l *Limiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseLocked()
}

func (l *Limiter) releaseLocked() {
	if len(l.waiting) > 0 {
		w := heap.Pop(&l.waiting).(*waiter)
		close(w.ready) // the slot moves to w; used is unchanged.
		return
	}
	l.used--
}

type waiter struct {
	priority int
	seq      uint64
	index    int // in waiters; -1 once granted
	ready    chan struct{}
}

type waiters []*waiter

func (h waiters) Len() int { return len(h) }
func (h waiters) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].seq < h[j].seq
}
func (h waiters) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *waiters) Push(x any) {
	w := x.(*waiter)
	w.index = len(*h)
	*h = append(*h, w)
}
func (h *waiters) Pop() any {
	old := *h
	w := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	w.index = -1
	return w
}
