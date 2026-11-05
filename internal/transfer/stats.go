package transfer

import (
	"crypto/sha256"
	"hash"
	"io"
	"sync/atomic"
	"time"

	"github.com/kindlingai/model-loader/internal/store"
)

// Stats adds up where segment transfers spend their time, so an operator
// can see which stage holds a slot (kindlingai/model-loader#49). Its zero
// value is ready to use, and it is safe for concurrent use.
type Stats struct {
	transfers, bytes                    atomic.Int64
	wait, read, write, hash, sync, save atomic.Int64 // nanoseconds
}

// StatsSnapshot is what a Stats added up since the last Take.
type StatsSnapshot struct {
	// Transfers is how many segment transfers (runs) were made.
	Transfers int64
	// Bytes is how many bytes they read from their sources.
	Bytes int64
	// Wait is the time spent waiting for a transfer slot; Read reading
	// from sources; Write writing to the store; Hash hashing; Sync
	// syncing and dropping from page cache, after the slot was given
	// back; Save marking segments held and saving state.
	Wait, Read, Write, Hash, Sync, Save time.Duration
}

// Take returns what s added up and starts it from zero.
func (s *Stats) Take() StatsSnapshot {
	d := func(v *atomic.Int64) time.Duration { return time.Duration(v.Swap(0)) }
	return StatsSnapshot{
		Transfers: s.transfers.Swap(0), Bytes: s.bytes.Swap(0),
		Wait: d(&s.wait), Read: d(&s.read), Write: d(&s.write), Hash: d(&s.hash), Sync: d(&s.sync), Save: d(&s.save),
	}
}

type timedReader struct {
	r io.Reader
	t *atomic.Int64
}

func (r timedReader) Read(p []byte) (int, error) {
	t := time.Now()
	n, err := r.r.Read(p)
	r.t.Add(int64(time.Since(t)))
	return n, err
}

type timedWriter struct {
	w io.Writer
	t *atomic.Int64
}

func (w timedWriter) Write(p []byte) (int, error) {
	t := time.Now()
	n, err := w.w.Write(p)
	w.t.Add(int64(time.Since(t)))
	return n, err
}

// segmentHasher hashes the bytes written to it as segments first to
// first+n-1 of fm, in order: one sha256 per segment in sums once each is
// written whole.
type segmentHasher struct {
	fm    store.FileManifest
	pos   int64 // offset in the file of the next byte
	next  int   // the segment being hashed
	last  int
	h     hash.Hash
	sums  [][sha256.Size]byte
	stats *Stats
}

func newSegmentHasher(fm store.FileManifest, first, n int, stats *Stats) *segmentHasher {
	start, _ := fm.SegmentBounds(first)
	stats.transfers.Add(1)
	return &segmentHasher{fm: fm, pos: start, next: first, last: first + n - 1, h: sha256.New(), stats: stats}
}

func (s *segmentHasher) Write(p []byte) (int, error) {
	t := time.Now()
	defer func() { s.stats.hash.Add(int64(time.Since(t))) }()
	written := 0
	for len(p) > 0 && s.next <= s.last {
		_, end := s.fm.SegmentBounds(s.next)
		k := int(min(int64(len(p)), end-s.pos))
		s.h.Write(p[:k])
		s.pos += int64(k)
		p = p[k:]
		written += k
		if s.pos == end {
			var sum [sha256.Size]byte
			s.h.Sum(sum[:0])
			s.sums = append(s.sums, sum)
			s.h.Reset()
			s.next++
		}
	}
	// Past the last segment: the caller's limit keeps this from
	// happening; take it without hashing, so the copy reports the count.
	return written + len(p), nil
}

// done returns the sha256 of every segment, once all were written whole;
// nil if one was not. A segment of no bytes is whole with none written.
func (s *segmentHasher) done() [][sha256.Size]byte {
	for s.next <= s.last {
		if _, end := s.fm.SegmentBounds(s.next); s.pos != end {
			return nil
		}
		var sum [sha256.Size]byte
		s.h.Sum(sum[:0])
		s.sums = append(s.sums, sum)
		s.h.Reset()
		s.next++
	}
	return s.sums
}
