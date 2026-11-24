package transfer

import (
	"crypto/sha256"
	"hash"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/kindlingai/model-loader/internal/store"
)

// Reorder bounds the memory runs that land ahead of their file's in-order
// whole-file hash (see wholeHash) wait in until it reaches them. Every
// transfer given the same Reorder shares it.
type Reorder struct {
	mu   sync.Mutex
	free int64
}

// NewReorder returns a Reorder of n bytes.
func NewReorder(n int64) *Reorder { return &Reorder{free: n} }

var reorderBufs sync.Pool // *[]byte

// get returns an empty buffer for n bytes, or nil if r has no room.
func (r *Reorder) get(n int64) *[]byte {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	ok := r.free >= n
	if ok {
		r.free -= n
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}
	b, _ := reorderBufs.Get().(*[]byte)
	if b == nil || int64(cap(*b)) < n {
		s := make([]byte, 0, n)
		b = &s
	}
	return b
}

func (r *Reorder) put(b *[]byte, n int64) {
	*b = (*b)[:0]
	reorderBufs.Put(b)
	r.mu.Lock()
	r.free += n
	r.mu.Unlock()
}

const noHole = math.MaxInt

// wholeHash is the sha256 of a file's first segments, taken from the
// bytes as they stream in rather than read back from the store: on a
// network store that read crosses the link the origin download uses
// (#52). It advances in file order. A run that starts where it stands is
// hashed as it streams; one that lands ahead waits in memory (Reorder)
// until it gets there. FinishFile reads from the store only what it did
// not reach.
//
// Every byte it hashed is a byte the store holds: a run that lands over
// segments it already hashed (fetched again after a drop, or twice at
// once) starts it over.
type wholeHash struct {
	mu   sync.Mutex
	path string
	fm   store.FileManifest
	h    hash.Hash // sha256 of the segments before next
	next int
	// epoch counts resets; a run streaming into a clone of h taken in an
	// earlier one cannot extend h.
	epoch   uint64
	spec    bool // a run at next is streaming into a clone of h
	writing int  // runs streaming now
	// hole is the first segment at or after next that was written
	// without being hashed or kept, so the hash cannot pass it until it is
	// written again; noHole if none is known. Nothing past it is kept.
	hole    int
	pending map[int]*wholeRun // runs landed ahead of next, by first segment
	calls   map[int][][2]int  // the runs of each FetchFile fetching the file now
	seq     int
}

// wholes holds a wholeHash for each file being fetched, by its path in
// the store.
//
// shortcut: a file fetched in part and never finished keeps its entry (a
// few hundred bytes, no buffers) until the process ends; sweep idle ones
// if roots ever touch millions of files.
var wholes struct {
	sync.Mutex
	m map[string]*wholeHash
}

// wholeFor returns the wholeHash for fm at path, nil if fm has no
// whole-file sha256 to check.
func wholeFor(path string, fm store.FileManifest) *wholeHash {
	if fm.SHA256 == "" || fm.NumSegments() == 0 {
		return nil
	}
	wholes.Lock()
	defer wholes.Unlock()
	if w := wholes.m[path]; w != nil && w.same(fm) {
		return w
	}
	if wholes.m == nil {
		wholes.m = make(map[string]*wholeHash)
	}
	w := &wholeHash{path: path, fm: fm, h: sha256.New(), hole: noHole}
	wholes.m[path] = w
	return w
}

func (w *wholeHash) same(fm store.FileManifest) bool {
	return w.fm.Size == fm.Size && w.fm.SegmentSize == fm.SegmentSize && w.fm.SHA256 == fm.SHA256
}

// takeWhole removes path's wholeHash and returns a sha256 of fm's first
// from bytes, as they streamed in; from is 0 if none were hashed, or if a
// run is writing to the file now.
func takeWhole(path string, fm store.FileManifest) (hash.Hash, int64) {
	wholes.Lock()
	w := wholes.m[path]
	delete(wholes.m, path)
	wholes.Unlock()
	if w == nil || !w.same(fm) {
		return sha256.New(), 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.reset() // gives back what it holds; runs still in flight end on it harmlessly.
	if w.writing > 0 {
		return sha256.New(), 0 // a held segment is being written again.
	}
	h, from := cloneHash(w.h), fm.Size
	if w.next < fm.NumSegments() {
		from, _ = fm.SegmentBounds(w.next)
	}
	return h, from
}

func cloneHash(h hash.Hash) hash.Hash {
	c, err := h.(hash.Cloner).Clone()
	if err != nil {
		panic(err) // sha256 always clones.
	}
	return c
}

// expect records that a FetchFile is fetching runs of the file, of which
// done marks those held when it started, until the returned func is
// called. Runs that land ahead are kept only while the hash's next segment
// is among those some FetchFile is fetching.
func (w *wholeHash) expect(runs [][2]int, done []bool) func() {
	if w == nil {
		return func() {}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.next < len(done) && done[w.next] {
		w.hole = min(w.hole, w.next) // held, and not hashed: from before a restart, say.
	}
	if w.calls == nil {
		w.calls = make(map[int][][2]int)
	}
	id := w.seq
	w.seq++
	w.calls[id] = runs
	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.calls, id)
		w.settle()
		if len(w.calls) == 0 && w.writing == 0 && w.next == 0 && len(w.pending) == 0 {
			wholes.Lock()
			if wholes.m[w.path] == w {
				delete(wholes.m, w.path)
			}
			wholes.Unlock()
		}
	}
}

// coming reports whether a FetchFile is fetching the hash's next segment.
func (w *wholeHash) coming() bool {
	for _, runs := range w.calls {
		i := sort.Search(len(runs), func(j int) bool { return runs[j][1] > w.next })
		if i < len(runs) && runs[i][0] <= w.next {
			return true
		}
	}
	return false
}

// wholeRun is one attempt at writing segments first to end-1 to the
// file: it streams into a clone of the hash when it starts where the hash
// stands, else into a buffer while Reorder has room, else nowhere.
type wholeRun struct {
	w          *wholeHash
	first, end int
	length     int64
	spec       bool
	epoch      uint64
	h          hash.Hash
	buf        *[]byte
	reorder    *Reorder
	stats      *Stats
}

// begin starts a run of n segments from first, length bytes. A nil w
// returns a nil run, which takes writes and does nothing.
func (w *wholeHash) begin(first, n int, length int64, reorder *Reorder, stats *Stats) *wholeRun {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writing++
	r := &wholeRun{w: w, first: first, end: first + n, length: length, stats: stats}
	switch {
	case first == w.next && !w.spec:
		w.spec, r.spec, r.epoch, r.h = true, true, w.epoch, cloneHash(w.h)
	case first > w.next && first < w.hole:
		if r.buf = reorder.get(length); r.buf != nil {
			r.reorder = reorder
		}
	}
	return r
}

func (r *wholeRun) Write(p []byte) (int, error) {
	if r == nil || (r.h == nil && r.buf == nil) {
		return len(p), nil
	}
	t := time.Now()
	if r.h != nil {
		r.h.Write(p)
	} else if len(*r.buf)+len(p) <= cap(*r.buf) {
		*r.buf = append(*r.buf, p...)
	}
	r.stats.hash.Add(int64(time.Since(t)))
	return len(p), nil
}

// drop gives back r's buffer.
func (r *wholeRun) drop() {
	if r.buf != nil {
		r.reorder.put(r.buf, r.length)
		r.buf = nil
	}
}

// done ends r: ok means every byte of it was written and its segments
// verified.
func (r *wholeRun) done(ok bool) {
	if r == nil {
		return
	}
	w := r.w
	t := time.Now()
	w.mu.Lock()
	defer func() {
		w.mu.Unlock()
		r.stats.hash.Add(int64(time.Since(t)))
	}()
	w.writing--
	if r.spec {
		w.spec = false
	}
	if r.buf != nil && int64(len(*r.buf)) != r.length {
		r.drop()
	}
	if !ok {
		r.drop()
		w.settle()
		return
	}
	if r.first < w.next || w.overlapsPending(r) {
		w.reset() // bytes it hashed, or holds, were written again.
	}
	switch {
	case r.h != nil && r.epoch == w.epoch && r.first == w.next:
		w.h, w.next = r.h, r.end
	case r.buf != nil && r.first == w.next:
		w.h.Write(*r.buf)
		w.next = r.end
		r.drop()
	case r.buf != nil && r.first < w.hole && w.coming():
		if w.pending == nil {
			w.pending = make(map[int]*wholeRun)
		}
		w.pending[r.first] = r
	default:
		r.drop()
		w.hole = min(w.hole, r.first)
	}
	w.settle()
}

func (w *wholeHash) overlapsPending(r *wholeRun) bool {
	for _, q := range w.pending {
		if q.first < r.end && r.first < q.end {
			return true
		}
	}
	return false
}

// settle hashes the pending runs the hash has reached, then gives back
// those it cannot reach: past a hole, or all of them once nothing is
// fetching its next segment.
func (w *wholeHash) settle() {
	for q := w.pending[w.next]; q != nil; q = w.pending[w.next] {
		delete(w.pending, w.next)
		w.h.Write(*q.buf)
		w.next = q.end
		q.drop()
	}
	if w.next > w.hole {
		w.hole = noHole // written again and hashed.
	}
	coming := w.coming()
	for k, q := range w.pending {
		if k < w.next {
			w.reset() // a run it holds overlaps one it hashed.
			return
		}
		if !coming || k > w.hole {
			delete(w.pending, k)
			q.drop()
			w.hole = min(w.hole, k)
		}
	}
}

// reset starts the hash over from the first byte.
func (w *wholeHash) reset() {
	for _, q := range w.pending {
		q.drop()
	}
	w.pending = nil
	w.h, w.next, w.hole = sha256.New(), 0, noHole
	w.epoch++
}
