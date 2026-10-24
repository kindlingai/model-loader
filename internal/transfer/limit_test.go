package transfer

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A freed slot goes to the waiting transfer with the highest priority,
// then the oldest.
func TestLimiterPriority(t *testing.T) {
	l := NewLimiter(1)
	l.Acquire(context.Background(), 0)
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	for _, p := range []int{0, 10, 5, 10} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Acquire(context.Background(), p)
			mu.Lock()
			order = append(order, p)
			mu.Unlock()
			l.Release()
		}()
		time.Sleep(20 * time.Millisecond) // queue in this order
	}
	l.Release()
	wg.Wait()
	if len(order) != 4 || order[0] != 10 || order[1] != 10 || order[2] != 5 || order[3] != 0 {
		t.Fatalf("admitted in order %v, want [10 10 5 0]", order)
	}
}

// A waiter whose context ends leaves the queue, and a slot granted as it
// ends is passed on rather than lost.
func TestLimiterCancel(t *testing.T) {
	l := NewLimiter(1)
	l.Acquire(context.Background(), 0)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- l.Acquire(ctx, 1) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("Acquire = nil after cancel")
	}
	l.Release()
	c2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := l.Acquire(c2, 0); err != nil {
		t.Fatalf("slot lost after a cancelled waiter: %v", err)
	}
	// Cancel and release at once, many times: the one slot is never lost
	// or doubled. The test holds it at the start of each round.
	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan error, 1)
		go func() { got <- l.Acquire(ctx, 0) }()
		go cancel()
		l.Release()
		if err := <-got; err == nil {
			l.Release() // the waiter won the slot; give it back
		}
		c, cc := context.WithTimeout(context.Background(), time.Second)
		if err := l.Acquire(c, 0); err != nil {
			t.Fatal("slot lost when a waiter was cancelled as it was granted")
		}
		cc()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used != 1 || len(l.waiting) != 0 {
		t.Fatalf("used %d, waiting %d; want 1, 0", l.used, len(l.waiting))
	}
}
