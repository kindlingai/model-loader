package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kindlingai/model-loader/internal/catalog"
	"github.com/kindlingai/model-loader/internal/source"
	"github.com/kindlingai/model-loader/internal/store"
	"github.com/kindlingai/model-loader/internal/xxh64"
)

// Fetcher opens a byte range of one file. length <= 0 means to EOF.
// Implementations should return ErrRangeUnavailable when they don't have the
// requested range yet, so FetchFile can fall back to the next source instead
// of failing outright.
type Fetcher interface {
	Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
}

// WANFetcher adapts a WAN source.Source, bound to one catalog model, to the
// Fetcher interface.
type WANFetcher struct {
	Source source.Source
	Model  catalog.Model
}

// Open reads from the model's origin. What it returns claims
// store.TrustOrigin (see Claimer).
func (w WANFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	rc, err := w.Source.Open(ctx, w.Model, path, offset, length)
	if err != nil {
		return nil, err
	}
	return claimed{ReadCloser: rc, origin: true}, nil
}

// claimed is a reader with a source's claims about the segments it reads:
// one sha256 and trust mode per segment, from a peer, or that this node
// is reading them from the model's origin itself.
type claimed struct {
	io.ReadCloser
	sums, trusts []string
	origin       bool
}

func (c claimed) Claims() ([]string, []string, bool) { return c.sums, c.trusts, c.origin }

// PeerFetcher adapts a PeerClient bound to one peer address and
// model/revision to the Fetcher interface.
type PeerFetcher struct {
	Client          *PeerClient
	Addr            string
	Model, Revision string
}

func (p PeerFetcher) Open(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return p.Client.OpenBlob(ctx, p.Addr, p.Model, p.Revision, path, offset, length, nil)
}

// OpenChecked asks the peer for the range, passing checks for it to verify
// first (a root fetching the range from its origin does).
func (p PeerFetcher) OpenChecked(ctx context.Context, path string, offset, length int64, checks []store.RangeCheck) (io.ReadCloser, error) {
	return p.Client.OpenBlob(ctx, p.Addr, p.Model, p.Revision, path, offset, length, checks)
}

// Options tunes FetchFile. The zero value fetches every segment, trying
// every fetcher in order, one segment at a time.
type Options struct {
	// Need marks the segments to fetch. nil means every segment.
	Need []bool
	// Order, if set, returns the indices into the fetchers to try for
	// segment i, in order. A fetcher it leaves out is not tried for that
	// segment.
	Order func(i int) []int
	// Limit, if set, is shared with every other transfer on the node:
	// each segment transfer holds one of its slots while it runs, asked
	// for at Priority. Its size is also how many segments of this file are
	// fetched at once, so different segments can come from different
	// sources in parallel.
	Limit    *Limiter
	Priority int
	// Checks are a consumer's checksums for ranges of the file. Each is
	// checked once every segment it covers is held, unless all of them
	// were verified against the manifest; one that fails drops the
	// segments it covers. A source that takes checks (see CheckedFetcher)
	// is given those inside the segment it is asked for.
	Checks []store.RangeCheck
	// SaveSoon saves the state at most once a second
	// (store.SaveStateSoon) as segments land; a file of small segments
	// always does. At the end a file of small segments is saved soon too,
	// unless a check dropped a segment; any other file is saved then. A
	// root's pull-through sets it: the peer it fills is served from
	// memory, and a whole save of a release-sized state per segment would
	// cap the root at a few segments a second (#49). A local fetch does
	// not: its consumer reads the state file once it is ready.
	SaveSoon bool
	// Wait, if set, bounds waiting for a Limit slot instead of ctx: a
	// root's pull-through waits for a slot only as long as the request
	// for it does, but finishes a transfer once it has started.
	Wait context.Context
	// Timeout, if set, bounds each transfer from when it has its Limit
	// slot until its bytes are in, not counting the wait for the slot.
	Timeout time.Duration
	// Flush, if set, bounds how many transfers sync what they wrote at
	// once, across every transfer that shares it. A transfer gives its
	// Limit slot back once its bytes are written, and syncs them while
	// another uses the slot; it takes a Flush slot first, so a store
	// slower than the sources holds the sources back.
	Flush *Limiter
	// Stats, if set, adds up where each segment transfer spends its time.
	Stats *Stats
}

// Claimer is implemented by a reader whose source vouches for the
// segments it reads: for each, in order, the sha256 it holds for it and
// how it verified it (a store.Trust constant); "" for one it does not
// vouch for. origin means this node reads them from the model's origin
// itself, over TLS: they are TrustOrigin with no sha256 to match.
type Claimer interface {
	Claims() (sha256s, trusts []string, origin bool)
}

// maxRunSegments bounds how many segments one transfer asks a source for
// at once (see FetchFile), so a source's claims for them fit in a header.
const maxRunSegments = 256

// errUnclaimed is a run of several segments of a file with no segment
// hashes, from a source that vouched for none of them. A source too old to
// vouch for several segments at once does so for one; the run is fetched
// again segment by segment.
var errUnclaimed = errors.New("no segment hash in the manifest, and the source claims none")

// CheckedFetcher is a Fetcher that can pass a consumer's checksums for
// ranges inside the requested one on to its source, so the source checks
// them before it answers.
type CheckedFetcher interface {
	OpenChecked(ctx context.Context, path string, offset, length int64, checks []store.RangeCheck) (io.ReadCloser, error)
}

// FetchFile downloads the not-yet-done segments of fm that opts.Need marks
// into the store. Consecutive segments of one chunk (see store.ChunkSize)
// with the same sources are fetched as one range, at most maxRunSegments;
// a run that fails is fetched again segment by segment. For each segment
// (or run) it tries the fetchers opts.Order names,
// falling back to the next one when a fetcher doesn't have it yet or
// fails. Each segment is verified as soon as it lands: against the
// manifest when fm has segment hashes, else against the sha256 its source
// claims for it (see Claimer). Once every segment of the file is done,
// needed or not, the whole file is verified once more and marked complete;
// until then the file is partial, a sparse file with every held segment at
// its own offset. rs is updated and persisted to st as progress is made,
// so a restart resumes from the last saved segment rather than from
// scratch. rs may be shared with other fetches into the same revision.
func FetchFile(ctx context.Context, st *store.Store, model, revision string, fm store.FileManifest, rs *store.RevisionState, fetchers []Fetcher, opts Options) error {
	need := opts.Need
	fileState := rs.FileState(fm)
	if fileState.Complete {
		return nil
	}
	done := make([]bool, fm.NumSegments())
	copy(done, fileState.SegmentsDone) // state from other segments: keep what lines up.
	if need != nil && len(need) != fm.NumSegments() {
		return fmt.Errorf("fetch %s: need has %d segments, manifest %d", fm.Path, len(need), fm.NumSegments())
	}
	var todo []int
	for i, d := range done {
		if !d && (need == nil || need[i]) {
			todo = append(todo, i)
		}
	}
	if len(todo) > 0 && len(fetchers) == 0 {
		return fmt.Errorf("fetch %s: no sources available", fm.Path)
	}

	// Each segment transfer opens the destination once it has a slot and
	// closes it when the segment is done, so files waiting for the
	// transfer limit hold no descriptors, between segments or before.
	openFile := func() (*os.File, error) {
		f, err := st.EnsureFile(model, revision, fm)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", fm.Path, err)
		}
		return f, nil
	}
	// A transfer writes through a descriptor of its own, opened
	// write-only: an NFS client reads back from the server each page a
	// write covers only part of when the file is open for reading, and
	// those reads wait behind the segments being flushed (#37). Nothing
	// is read back: each segment is hashed as it streams in.
	openWriter := func() (*os.File, error) {
		f, err := openFile()
		if err != nil {
			return nil, err
		}
		name := f.Name()
		f.Close()
		w, err := os.OpenFile(name, os.O_WRONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", fm.Path, err)
		}
		return w, nil
	}

	order := opts.Order
	if order == nil {
		all := make([]int, len(fetchers))
		for k := range all {
			all[k] = k
		}
		order = func(int) []int { return all }
	}
	workers := 1
	if opts.Limit != nil {
		// Twice the slots: a transfer gives its slot back once its bytes
		// are in, and flushes them to the store while another one uses
		// the slot (see flush).
		workers = 2 * opts.Limit.Cap()
	}
	runs := groupRuns(fm, todo, order)
	workers = min(workers, max(1, len(runs)))

	var (
		mu       sync.Mutex // guards dead and firstErr
		dead     = make([]bool, len(fetchers))
		firstErr error
		wg       sync.WaitGroup
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	// A file of small segments saves its state at most once a second as
	// segments land (see store.SaveStateSoon), and once at the end.
	save := st.SaveState
	if fm.Small() || opts.SaveSoon {
		save = st.SaveStateSoon
	}
	wait := ctx
	if opts.Wait != nil {
		wait = opts.Wait
	}
	stats := opts.Stats
	if stats == nil {
		stats = &Stats{}
	}
	var marked atomic.Bool
	// download fetches segments first to first+n-1 into f, segment by
	// segment if the run fails as a whole. It holds the transfer's slot.
	download := func(ctx context.Context, f *os.File, first, n int) ([]landed, error) {
		fetch := func(first, n int) (landed, error) {
			start, _ := fm.SegmentBounds(first)
			_, end := fm.SegmentBounds(first + n - 1)
			sums, trusts, err := fetchRun(ctx, f, fm, first, n, fetchers, order(first), checksOverlapping(opts.Checks, start, end), dead, &mu, stats)
			return landed{first, n, sums, trusts}, err
		}
		l, err := fetch(first, n)
		if err == nil {
			return []landed{l}, nil
		}
		if n == 1 || ctx.Err() != nil {
			return nil, err
		}
		// One at a time: a source too old to vouch for a run, or one bad
		// segment, fails only that one.
		var got []landed
		var errs []error
		for i := first; i < first+n; i++ {
			if rs.SegmentTrust(fm, i) != "" {
				continue
			}
			l, err := fetch(i, 1)
			if err != nil {
				errs = append(errs, fmt.Errorf("segment %d: %w", i, err))
				continue
			}
			got = append(got, l)
		}
		return got, errors.Join(errs...)
	}
	// flush makes what download wrote durable, drops it from page cache
	// and marks it held. It runs after the transfer has given its slot
	// back: on a network store a sync takes long enough that the origin
	// connection would otherwise sit idle each time (#36, #49).
	flush := func(f *os.File, got []landed) error {
		if len(got) == 0 {
			return nil
		}
		t := time.Now()
		if err := syncFile(f); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
		for _, l := range got {
			start, _ := fm.SegmentBounds(l.first)
			_, end := fm.SegmentBounds(l.first + l.n - 1)
			if err := store.Uncache(f, start, end-start); err != nil {
				return fmt.Errorf("uncache: %w", err)
			}
		}
		stats.sync.Add(int64(time.Since(t)))
		t = time.Now()
		defer func() { stats.save.Add(int64(time.Since(t))) }()
		for _, l := range got {
			if fm.Small() && !fm.Hashed() {
				// The sha256 of each is what a later copier or re-verify
				// checks it against: on disk before the segment is held.
				if err := st.WriteSegmentHashes(model, revision, fm, l.first, l.sums); err != nil {
					return fmt.Errorf("save segment hashes: %w", err)
				}
			}
			for k := range l.n {
				rs.MarkSegment(fm, l.first+k, hex.EncodeToString(l.sums[k][:]), l.trusts[k])
			}
		}
		marked.Store(true)
		if err := save(rs); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		return nil
	}
	queue := make(chan [2]int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range queue {
				if opts.Limit != nil {
					t := time.Now()
					err := opts.Limit.Acquire(wait, opts.Priority)
					stats.wait.Add(int64(time.Since(t)))
					if err != nil {
						fail(err)
						continue
					}
				}
				tctx, cancel := ctx, context.CancelFunc(func() {})
				if opts.Timeout > 0 {
					tctx, cancel = context.WithTimeout(ctx, opts.Timeout)
				}
				f, err := openWriter()
				var got []landed
				if err == nil {
					got, err = download(tctx, f, r[0], r[1]-r[0])
				}
				cancel()
				flushing := opts.Flush != nil && len(got) > 0
				if flushing {
					if ferr := opts.Flush.Acquire(context.WithoutCancel(ctx), opts.Priority); ferr != nil {
						flushing = false // cannot happen: it does not end
					}
				}
				if opts.Limit != nil {
					opts.Limit.Release()
				}
				if f != nil {
					if ferr := flush(f, got); ferr != nil {
						err = errors.Join(err, ferr)
					}
					f.Close()
				}
				if flushing {
					opts.Flush.Release()
				}
				if err != nil {
					fail(fmt.Errorf("fetch %s segments %d-%d: %w", fm.Path, r[0], r[1]-1, err))
				}
			}
		}()
	}
	for _, r := range runs {
		queue <- r
	}
	close(queue)
	wg.Wait()
	if firstErr != nil && (ctx.Err() != nil || len(PendingChecks(fm, rs, opts.Checks)) == 0) {
		if marked.Load() && (fm.Small() || opts.SaveSoon) {
			// What landed, before giving up.
			if opts.SaveSoon {
				_ = st.SaveStateSoon(rs)
			} else {
				_ = st.SaveState(rs)
			}
		}
		return firstErr // nothing that landed is left to judge.
	}

	f, err := openFile()
	if err != nil {
		return errors.Join(firstErr, err)
	}
	defer f.Close()
	// Every transfer synced and uncached what it wrote (see flush).
	// Checks run even when a segment failed: a check over segments that
	// did land (another request's range, say) is judged now, not held
	// back by the one that failed.
	err = ApplyChecks(f, fm, rs, opts.Checks)
	finalSave := st.SaveState
	if opts.SaveSoon && fm.Small() && err == nil {
		finalSave = st.SaveStateSoon // nothing dropped: disk may lag.
	}
	if serr := finalSave(rs); err == nil && serr != nil {
		err = serr
	}
	if err != nil {
		err = fmt.Errorf("fetch %s: %w", fm.Path, err)
	}
	if err := errors.Join(firstErr, err); err != nil {
		return err
	}
	return FinishFile(st, f, fm, rs)
}

// FinishFile verifies fm whole and marks it complete once every segment is
// held. A file with segments still missing is left partial. If the whole
// file fails its check, every segment not verified against manifest
// segment hashes is dropped, so it is fetched again rather than served. If
// segments the manifest verified are left, fetching them again cannot
// change them; the file is marked WholeFailed and nothing of it is served.
func FinishFile(st *store.Store, f *os.File, fm store.FileManifest, rs *store.RevisionState) error {
	unlock := rs.LockFile(fm.Path)
	defer unlock()
	fs := rs.FileState(fm)
	if fs.Complete || len(fs.SegmentsDone) != fm.NumSegments() || !allDone(fs.SegmentsDone) {
		return nil
	}
	failed := fmt.Errorf("fetch %s: whole-file verification failed after every segment passed", fm.Path)
	if fs.WholeFailed {
		return failed // checked before; the bytes have not changed since.
	}
	ok, err := store.VerifyWholeFile(f, fm)
	if err != nil {
		return fmt.Errorf("fetch %s: verify whole file: %w", fm.Path, err)
	}
	if !ok {
		for i := range fs.SegmentsDone {
			if fs.TrustOf(i) != store.TrustManifest || !fm.Hashed() {
				rs.DropSegment(fm, i)
			}
		}
		rs.SetWholeFailed(fm)
		_ = st.SaveState(rs)
		return failed
	}
	if !rs.SetComplete(fm) {
		return nil // a segment was dropped meanwhile; it is fetched again.
	}
	return st.SaveState(rs)
}

// pending reports whether ApplyChecks would check c now, given fs: every
// segment it covers is held, not every one of those was verified against
// the manifest, and it has not matched these bytes before.
func pending(fm store.FileManifest, fs store.FileState, c store.RangeCheck) bool {
	if fs.HasChecked(c) {
		return false
	}
	first, last := c.Segments(fm)
	manifest := true
	for i := first; i <= last; i++ {
		t := fs.TrustOf(i)
		if t == "" {
			return false
		}
		manifest = manifest && t == store.TrustManifest
	}
	return !manifest
}

// PendingChecks returns those of checks that ApplyChecks would check now.
func PendingChecks(fm store.FileManifest, rs *store.RevisionState, checks []store.RangeCheck) []store.RangeCheck {
	if len(checks) == 0 {
		return nil
	}
	var out []store.RangeCheck
	rs.View(fm, func(fs *store.FileState) {
		for _, c := range checks {
			if pending(fm, *fs, c) {
				out = append(out, c)
			}
		}
	})
	return out
}

// ApplyChecks checks each of checks that is pending against the held
// bytes. When every segment a check covers was verified against the
// manifest, the bytes are the origin's and a mismatch is the consumer's
// mistake, so it is not checked. A range that matches is recorded, and its
// segments' trust goes up to store.TrustRequester. One that does not (or
// cannot be read, as in a file cut short) drops its segments that were not
// verified against the manifest, so they are fetched again, and
// ApplyChecks returns an error naming it. Each check is judged against the
// state as it is then, so one check's drop is seen by the next. rs is not
// saved.
func ApplyChecks(f *os.File, fm store.FileManifest, rs *store.RevisionState, checks []store.RangeCheck) error {
	unlock := rs.LockFile(fm.Path)
	defer unlock()
	var errs []error
	for _, c := range checks {
		if !pending(fm, rs.FileState(fm), c) {
			continue
		}
		ok, err := store.CheckRange(f, c)
		if err != nil {
			ok = false
		}
		fs := rs.FileState(fm)
		first, last := c.Segments(fm)
		for i := first; i <= last; i++ {
			switch {
			case fs.TrustOf(i) == store.TrustManifest:
			case ok:
				rs.SetTrust(fm, i, store.TrustRequester)
			default:
				rs.DropSegment(fm, i)
			}
		}
		switch {
		case ok:
			rs.AddChecked(fm, c)
		case err != nil:
			errs = append(errs, fmt.Errorf("range offset %d length %d: %w: %w", c.Offset, c.Length, store.ErrCheckFailed, err))
		default:
			errs = append(errs, fmt.Errorf("range offset %d length %d: %w: xxh64 %s", c.Offset, c.Length, store.ErrCheckFailed, xxh64.Format(c.XXH64)))
		}
	}
	return errors.Join(errs...)
}

// checksOverlapping returns the checks that share a byte with
// [start, end). A source that takes checks fetches every segment they span
// and checks them before it answers, so a check spanning segments is
// verified at the root too.
func checksOverlapping(checks []store.RangeCheck, start, end int64) []store.RangeCheck {
	var out []store.RangeCheck
	for _, c := range checks {
		if c.Overlaps(start, end) {
			out = append(out, c)
		}
	}
	return out
}

func allDone(segments []bool) bool {
	for _, done := range segments {
		if !done {
			return false
		}
	}
	return true
}

// copyBufs holds the buffers transfers copy through: larger than
// io.Copy's 32 KiB, so a segment reaches the store in fewer writes.
var copyBufs = sync.Pool{New: func() any { b := make([]byte, 1<<20); return &b }}

// syncFile is f.Sync; tests replace it to see when a transfer syncs.
var syncFile = (*os.File).Sync

// landed is a run of segments a transfer downloaded and verified, not yet
// synced or marked held.
type landed struct {
	first, n int
	sums     [][sha256.Size]byte
	trusts   []string
}

// syncAndUncache fsyncs f, then advises the kernel to drop its page cache.
// Only data that's durable on disk should ever be dropped from cache, hence
// the fsync first.
func syncAndUncache(f *os.File) error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if err := store.Uncache(f, 0, info.Size()); err != nil {
		return fmt.Errorf("uncache: %w", err)
	}
	return nil
}

// groupRuns groups todo, ascending segment indices, into half-open runs of
// consecutive segments in one chunk with the same sources, at most
// maxRunSegments each. A file of 64 MiB segments has one segment a chunk,
// so one segment a run.
func groupRuns(fm store.FileManifest, todo []int, order func(int) []int) [][2]int {
	var runs [][2]int
	var cur []int // the order of the last run
	for _, i := range todo {
		if n := len(runs); n > 0 {
			r := &runs[n-1]
			if o := order(i); r[1] == i && fm.ChunkOf(i) == fm.ChunkOf(r[0]) && i-r[0] < maxRunSegments && slices.Equal(cur, o) {
				r[1] = i + 1
				continue
			} else {
				runs = append(runs, [2]int{i, i + 1})
				cur = o
				continue
			}
		}
		runs = append(runs, [2]int{i, i + 1})
		cur = order(i)
	}
	return runs
}

// fetchRun tries the fetchers order names, in turn, for segments first
// to first+n-1, returning at the first success each segment's sha256 and
// how it was verified. A fetcher reporting ErrRangeUnavailable, or a
// consumer checksum that failed (store.ErrCheckFailed), is skipped for
// this run only. One that fails outright (a peer that went away mid-copy,
// a dropped connection, data that fails verification) is marked in dead
// and skipped for the rest of the file, so the copy fails over to the
// next peer or the root instead of failing, and doesn't wait on a dead
// address again for every segment. mu guards dead.
//
// At most the run's own bytes are written, so a source that sends too
// much cannot overwrite a neighbouring segment another worker holds. Each
// segment is hashed as it streams in; the caller syncs the run before it
// marks it held.
func fetchRun(ctx context.Context, f *os.File, fm store.FileManifest, first, n int, fetchers []Fetcher, order []int, checks []store.RangeCheck, dead []bool, mu *sync.Mutex, stats *Stats) ([][sha256.Size]byte, []string, error) {
	start, _ := fm.SegmentBounds(first)
	_, end := fm.SegmentBounds(first + n - 1)
	length := end - start
	isDead := func(k int) bool {
		mu.Lock()
		defer mu.Unlock()
		return dead[k]
	}
	kill := func(k int) {
		mu.Lock()
		dead[k] = true
		mu.Unlock()
	}
	var lastErr error
	for _, k := range order {
		if k < 0 || k >= len(fetchers) || isDead(k) {
			continue
		}
		var rc io.ReadCloser
		var err error
		if cf, ok := fetchers[k].(CheckedFetcher); ok && len(checks) > 0 {
			rc, err = cf.OpenChecked(ctx, fm.Path, start, length, checks)
		} else {
			rc, err = fetchers[k].Open(ctx, fm.Path, start, length)
		}
		if errors.Is(err, ErrRangeUnavailable) || errors.Is(err, store.ErrCheckFailed) {
			// Not this source's fault: a checksum that failed is about
			// that range, so the source stays usable for other segments.
			lastErr = err
			continue
		}
		if err != nil {
			kill(k)
			lastErr = err
			continue
		}
		var claimSums, claimTrusts []string
		origin := false
		if c, ok := rc.(Claimer); ok {
			claimSums, claimTrusts, origin = c.Claims()
		}

		// Write at most the run's own bytes, so a source that sends too
		// much cannot overwrite a neighbouring segment that is held. Each
		// segment is hashed as it is written, not read back from the
		// store: on NFS that read is as slow as the write.
		hs := newSegmentHasher(fm, first, n, stats)
		dst := io.MultiWriter(timedWriter{io.NewOffsetWriter(f, start), &stats.write}, hs)
		buf := copyBufs.Get().(*[]byte)
		copied, copyErr := io.CopyBuffer(dst, timedReader{io.LimitReader(rc, length), &stats.read}, *buf)
		copyBufs.Put(buf)
		stats.bytes.Add(copied)
		extra := 0
		if copyErr == nil && copied == length {
			// Only a full, good copy is probed for more: a failed one is
			// dropped at once, not held open waiting on the source.
			extra, _ = rc.Read(make([]byte, 1))
		}
		closeErr := rc.Close()
		var bad error
		switch {
		case copyErr != nil:
			bad = copyErr
		case copied != length:
			bad = fmt.Errorf("short read: wanted %d bytes, got %d", length, copied)
		case extra > 0:
			bad = fmt.Errorf("source sent more than the %d bytes asked for", length)
		case closeErr != nil:
			bad = closeErr
		}
		if bad != nil {
			kill(k)
			lastErr = bad
			continue
		}

		sums := hs.done()
		if len(sums) != n {
			return nil, nil, fmt.Errorf("hash %s segments %d-%d: %d of %d segments written whole", fm.Path, first, first+n-1, len(sums), n)
		}
		trusts := make([]string, n)
		for j := range n {
			i := first + j
			sum := hex.EncodeToString(sums[j][:])
			var claimSum, claimTrust string
			if j < len(claimSums) && j < len(claimTrusts) {
				claimSum, claimTrust = claimSums[j], claimTrusts[j]
			}
			switch {
			case fm.Hashed():
				if sum == fm.SegmentSHA256[i] {
					trusts[j] = store.TrustManifest
				} else {
					bad = fmt.Errorf("segment %d failed hash verification", i)
				}
			case claimSum != "":
				if sum != claimSum {
					bad = fmt.Errorf("segment %d does not match the sha256 its source claims", i)
				} else if claimTrust != store.TrustManifest {
					// A claimed manifest is only the source's word: it is
					// held as origin until this node checks the whole
					// file (see store.RevisionState.SetComplete).
					trusts[j] = claimTrust
				}
			case origin:
				trusts[j] = store.TrustOrigin // fetched from the origin by this node.
			case n > 1:
				// Not the source's fault yet: it may vouch for one
				// segment at a time.
				return nil, nil, errUnclaimed
			default:
				bad = errUnclaimed
			}
			if bad != nil {
				break
			}
			if trusts[j] == "" {
				trusts[j] = store.TrustOrigin
			}
		}
		if bad != nil {
			kill(k)
			lastErr = bad
			continue
		}
		return sums, trusts, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no source left to try")
	}
	return nil, nil, lastErr
}
