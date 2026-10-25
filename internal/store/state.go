package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// How a held segment was verified.
const (
	// TrustManifest: its sha256 matched the manifest. A segment of a file
	// whose manifest entry has segment hashes is always verified this way.
	TrustManifest = "manifest"
	// TrustOrigin: a root fetched it from the model's origin (over TLS)
	// for a file it had no segment hashes for, and recorded its sha256.
	// A node that copies it from that root checks it against that sha256
	// and keeps the root's trust mode.
	TrustOrigin = "origin"
	// TrustRequester: as TrustOrigin, and every range a consumer supplied
	// a checksum for in the segment matched it.
	TrustRequester = "requester"
)

// Trust modes in memory, one byte per segment. The zero value is
// TrustManifest, so a file verified whole needs none.
const (
	trustManifest uint8 = iota
	trustOrigin
	trustRequester
)

func trustCode(t string) uint8 {
	switch t {
	case "", TrustManifest:
		return trustManifest
	case TrustRequester:
		return trustRequester
	default:
		// TrustOrigin, or a mode this version does not know: the weakest
		// a held segment can have, never a stronger one than it was.
		return trustOrigin
	}
}

func trustName(c uint8) string {
	switch c {
	case trustOrigin:
		return TrustOrigin
	case trustRequester:
		return TrustRequester
	default:
		return TrustManifest
	}
}

// FileState tracks download progress for one file within a revision:
// which segments have been verified so far, and how.
//
// On disk (see MarshalJSON) a file has held, the held segments as
// half-open index ranges. A file of 64 MiB segments also has
// segments_done, one bool per segment, as before. A file of smaller
// segments (FileManifest.Small) has only held, keeps its segments' sha256
// in a side file (Store.WriteSegmentHashes) and its trust as ranges, so
// a state file stays small however many segments a file has.
type FileState struct {
	Path     string
	Complete bool
	// SegmentsDone[i] is true once segment i has been written and
	// verified.
	SegmentsDone []bool
	// SegmentSHA256[i] is segment i's sha256, for a file of 64 MiB
	// segments whose manifest entry has no segment hashes (see
	// FileManifest.Hashed). Nil otherwise: a file of smaller segments
	// keeps them in a side file.
	SegmentSHA256 []string
	// trust[i] is how segment i was verified, a trust code. Nil, or a
	// zero entry, means TrustManifest.
	trust []uint8
	// Checked lists the consumer checksums (RangeCheck.Key) that matched
	// the bytes held, so each is checked once, not on every pass.
	Checked []string
	// Used[c] is when chunk c (see FileManifest.ChunkOf; a segment, for a
	// file of 64 MiB segments) was last fetched or served, in Unix
	// seconds; 0 if never recorded. Eviction removes the oldest first.
	Used []int64
	// WholeFailed means every segment was held and the whole file did not
	// match its sha256, yet the segments matched the manifest's segment
	// hashes, so fetching them again cannot help: the manifest disagrees
	// with itself. Nothing of the file is served while it is set.
	WholeFailed bool
	// small is set for a file of segments smaller than a chunk; it picks
	// the encoding on disk.
	small bool
}

// fileStateJSON is a FileState on disk.
type fileStateJSON struct {
	Path     string `json:"path"`
	Complete bool   `json:"complete"`
	// SegmentsDone is written for a file of 64 MiB segments only.
	SegmentsDone *[]bool `json:"segments_done,omitempty"`
	// Held is every held segment, as half-open index ranges.
	Held          [][2]int            `json:"held"`
	SegmentSHA256 []string            `json:"segment_sha256,omitempty"`
	Trust         []string            `json:"trust,omitempty"`
	TrustRuns     map[string][][2]int `json:"trust_runs,omitempty"`
	Checked       []string            `json:"checked,omitempty"`
	Used          []int64             `json:"used,omitempty"`
	WholeFailed   bool                `json:"whole_failed,omitempty"`
}

// MarshalJSON writes fs as the store contract in docs/design.md has it.
func (fs FileState) MarshalJSON() ([]byte, error) {
	j := fileStateJSON{
		Path: fs.Path, Complete: fs.Complete, Held: Runs(fs.SegmentsDone),
		SegmentSHA256: fs.SegmentSHA256, Checked: fs.Checked, Used: fs.Used, WholeFailed: fs.WholeFailed,
	}
	if j.Held == nil {
		j.Held = [][2]int{}
	}
	if fs.small {
		if fs.trust != nil {
			for c := trustOrigin; c <= trustRequester; c++ {
				var runs [][2]int
				for _, r := range Runs(fs.SegmentsDone) {
					for i := r[0]; i < r[1]; i++ {
						if i < len(fs.trust) && fs.trust[i] == c {
							if n := len(runs); n > 0 && runs[n-1][1] == i {
								runs[n-1][1] = i + 1
							} else {
								runs = append(runs, [2]int{i, i + 1})
							}
						}
					}
				}
				if runs != nil {
					if j.TrustRuns == nil {
						j.TrustRuns = make(map[string][][2]int)
					}
					j.TrustRuns[trustName(c)] = runs
				}
			}
		}
	} else {
		done := fs.SegmentsDone
		j.SegmentsDone = &done
		if fs.trust != nil {
			j.Trust = make([]string, len(fs.trust))
			for i, c := range fs.trust {
				if c != trustManifest {
					j.Trust[i] = trustName(c)
				}
			}
		}
	}
	return json.Marshal(j)
}

// UnmarshalJSON reads what MarshalJSON, or an older version, wrote.
func (fs *FileState) UnmarshalJSON(b []byte) error {
	var j fileStateJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*fs = FileState{
		Path: j.Path, Complete: j.Complete, SegmentSHA256: j.SegmentSHA256,
		Checked: j.Checked, Used: j.Used, WholeFailed: j.WholeFailed,
	}
	if j.SegmentsDone != nil {
		fs.SegmentsDone = *j.SegmentsDone
	} else {
		fs.small = true
		fs.SegmentsDone = fromRuns(j.Held, 0)
	}
	n := len(fs.SegmentsDone)
	if j.Trust != nil {
		fs.trust = make([]uint8, n)
		for i, t := range j.Trust {
			if i < n {
				fs.trust[i] = trustCode(t)
			}
		}
	}
	for name, runs := range j.TrustRuns {
		c := trustCode(name)
		if c == trustManifest {
			continue
		}
		if fs.trust == nil {
			fs.trust = make([]uint8, n)
		}
		for _, r := range runs {
			for i := max(r[0], 0); i < r[1] && i < n; i++ {
				fs.trust[i] = c
			}
		}
	}
	return nil
}

// Runs packs done into half-open index ranges of held segments.
func Runs(done []bool) [][2]int {
	var out [][2]int
	for i := 0; i < len(done); i++ {
		if !done[i] {
			continue
		}
		j := i
		for j < len(done) && done[j] {
			j++
		}
		out = append(out, [2]int{i, j})
		i = j
	}
	return out
}

// fromRuns unpacks half-open index ranges into one bool per segment, at
// least n of them. Ranges that are not well formed, or past
// maxSegmentsPerFile, are left out.
func fromRuns(runs [][2]int, n int) []bool {
	for _, r := range runs {
		if r[0] >= 0 && r[0] < r[1] && r[1] <= maxSegmentsPerFile {
			n = max(n, r[1])
		}
	}
	done := make([]bool, n)
	for _, r := range runs {
		if r[0] >= 0 && r[0] < r[1] && r[1] <= maxSegmentsPerFile {
			for i := r[0]; i < r[1]; i++ {
				done[i] = true
			}
		}
	}
	return done
}

// SegmentTrust returns how held segment i of fm was verified, or "" if it
// is not held, without copying the file's state.
func (rs *RevisionState) SegmentTrust(fm FileManifest, i int) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if fs := rs.find(fm.Path); fs != nil {
		return fs.TrustOf(i)
	}
	return ""
}

// SetWholeFailed records that fm failed its whole-file check with every
// segment still held.
func (rs *RevisionState) SetWholeFailed(fm FileManifest) {
	rs.update(fm, func(fs *FileState) {
		fs.WholeFailed = allDone(fs.SegmentsDone)
	})
}

func allDone(done []bool) bool {
	for _, d := range done {
		if !d {
			return false
		}
	}
	return true
}

// HasChecked reports whether c matched the held bytes before.
func (fs FileState) HasChecked(c RangeCheck) bool {
	k := c.Key()
	for _, x := range fs.Checked {
		if x == k {
			return true
		}
	}
	return false
}

// TrustOf returns how segment i was verified, or "" if it is not held.
func (fs FileState) TrustOf(i int) string {
	if i < 0 || i >= len(fs.SegmentsDone) || !fs.SegmentsDone[i] {
		return ""
	}
	if i < len(fs.trust) {
		return trustName(fs.trust[i])
	}
	return TrustManifest
}

// SegmentHash returns the sha256 segment i must match: the manifest's when
// it has one, else the one recorded in the state when the segment was
// fetched. It is "" when there is neither; a file of small segments keeps
// what it recorded in a side file (see Store.SegmentSums).
func (fs FileState) SegmentHash(fm FileManifest, i int) string {
	if fm.Hashed() {
		if i < len(fm.SegmentSHA256) {
			return fm.SegmentSHA256[i]
		}
		return ""
	}
	if i < len(fs.SegmentSHA256) {
		return fs.SegmentSHA256[i]
	}
	return ""
}

func (fs FileState) clone() FileState {
	fs.SegmentsDone = append([]bool(nil), fs.SegmentsDone...)
	if fs.SegmentSHA256 != nil {
		fs.SegmentSHA256 = append([]string(nil), fs.SegmentSHA256...)
	}
	if fs.trust != nil {
		fs.trust = append([]uint8(nil), fs.trust...)
	}
	if fs.Checked != nil {
		fs.Checked = append([]string(nil), fs.Checked...)
	}
	if fs.Used != nil {
		fs.Used = append([]int64(nil), fs.Used...)
	}
	return fs
}

// fit resizes fs's per-segment slices to fm's segments, and its use
// times to fm's chunks, keeping what lines up: state saved against a
// manifest with other segments is kept where it can be and fetched again
// where it can't.
func (fs *FileState) fit(fm FileManifest) {
	n := fm.NumSegments()
	fs.small = fm.Small()
	if len(fs.SegmentsDone) != n {
		out := make([]bool, n)
		copy(out, fs.SegmentsDone)
		fs.SegmentsDone = out
	}
	if fs.SegmentSHA256 != nil && len(fs.SegmentSHA256) != n {
		out := make([]string, n)
		copy(out, fs.SegmentSHA256)
		fs.SegmentSHA256 = out
	}
	if fs.trust != nil && len(fs.trust) != n {
		out := make([]uint8, n)
		copy(out, fs.trust)
		fs.trust = out
	}
	if c := fm.NumChunks(); fs.Used != nil && len(fs.Used) != c {
		out := make([]int64, c)
		copy(out, fs.Used)
		fs.Used = out
	}
}

// LastUsed returns when chunk c (see FileManifest.ChunkOf) was last
// fetched or served, in Unix seconds, or 0 if never recorded.
func (fs FileState) LastUsed(c int) int64 {
	if c >= 0 && c < len(fs.Used) {
		return fs.Used[c]
	}
	return 0
}

// BytesDone estimates completed bytes for progress reporting, treating each
// done segment as full-size except the last.
func (fs FileState) BytesDone(fm FileManifest) int64 {
	var total int64
	for _, r := range Runs(fs.SegmentsDone) {
		start, _ := fm.SegmentBounds(r[0])
		_, end := fm.SegmentBounds(r[1] - 1)
		total += end - start
	}
	return total
}

// ReadyRanges returns the contiguous byte ranges that are safe to serve to a
// peer: runs of consecutive verified segments, coalesced into one range
// each.
func (fs FileState) ReadyRanges(fm FileManifest) []Range {
	if fs.WholeFailed {
		return nil
	}
	var ranges []Range
	for _, r := range Runs(fs.SegmentsDone) {
		start, _ := fm.SegmentBounds(r[0])
		_, end := fm.SegmentBounds(r[1] - 1)
		ranges = append(ranges, Range{Start: start, End: end})
	}
	return ranges
}

// Range is a half-open byte range [Start, End).
type Range struct {
	Start, End int64
}

// Contains reports whether the range [start, end) is fully inside r.
func (r Range) Contains(start, end int64) bool {
	return start >= r.Start && end <= r.End
}

// RevisionState is the per-file download state for one model revision. Its
// methods are safe for concurrent use; a node shares one RevisionState per
// revision between everything that fetches into it. Read or change Files
// directly only on a RevisionState no other goroutine can see yet.
type RevisionState struct {
	mu         sync.Mutex             // guards every field
	saveMu     sync.Mutex             // serializes Store.SaveState, so the newest wins
	locks      map[string]*sync.Mutex // see LockFile
	index      map[string]int         // Files by path; see find
	lastSave   time.Time              // see Store.SaveStateSoon; guarded by saveMu
	flush      *time.Timer            // a SaveStateSoon save to come; guarded by saveMu
	closed     bool                   // see Close; guarded by saveMu
	gen        uint64                 // bumped at every change; see Gen
	Model      string                 `json:"model"`
	Revision   string                 `json:"revision"`
	Files      []FileState            `json:"files"`
	UpdatedAt  time.Time              `json:"updated_at"`
	LastSource string                 `json:"last_source,omitempty"`
	// Origin is a root's progress downloading the revision from its origin,
	// before it has a manifest. It is dropped once the manifest is saved.
	Origin []OriginFile `json:"origin,omitempty"`
}

// OriginFile is a root's progress downloading one file from its origin. The
// first BytesDone bytes are on disk and synced, SegmentSHA256 holds their
// segment hashes, and SHA256State is the whole-file hash over them, so a
// restart resumes both the download and the hashing at BytesDone. SHA256 is
// the origin's digest, so progress against a file the origin has since
// replaced is discarded; once Complete, it is the digest of what was
// downloaded, and FileManifest is the file's manifest entry.
type OriginFile struct {
	FileManifest
	BytesDone   int64  `json:"bytes_done"`
	SHA256State []byte `json:"sha256_state,omitempty"`
	Complete    bool   `json:"complete"`
}

// Complete reports whether every file in the revision is complete.
func (rs *RevisionState) Complete() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.Files) == 0 {
		return false
	}
	for _, f := range rs.Files {
		if !f.Complete {
			return false
		}
	}
	return true
}

// BytesDone sums completed bytes across every file.
func (rs *RevisionState) BytesDone(m *Manifest) int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var total int64
	for _, fs := range rs.Files {
		if fm, ok := m.Find(fs.Path); ok {
			total += fs.BytesDone(fm)
		}
	}
	return total
}

// FileState returns a copy of the state for one file, or empty state if
// there is none yet.
func (rs *RevisionState) FileState(m FileManifest) FileState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if fs := rs.find(m.Path); fs != nil {
		return fs.clone()
	}
	return FileState{Path: m.Path, SegmentsDone: make([]bool, m.NumSegments()), small: m.Small()}
}

// View calls fn with the state of one file, or empty state if there is
// none yet, without copying it. fn must not keep it, change it, or call
// rs's methods.
func (rs *RevisionState) View(m FileManifest, fn func(fs *FileState)) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if fs := rs.find(m.Path); fs != nil {
		fn(fs)
		return
	}
	fn(&FileState{Path: m.Path, small: m.Small()})
}

// RangeReady reports whether [start, end) of fm may be served: every
// segment it touches is held, and the file has not failed its whole-file
// check.
func (rs *RevisionState) RangeReady(fm FileManifest, start, end int64) bool {
	if start < 0 || end > fm.Size || start >= end {
		return false
	}
	ok := true
	rs.View(fm, func(fs *FileState) {
		if fs.WholeFailed {
			ok = false
			return
		}
		for i := fm.SegmentIndex(start); i <= fm.SegmentIndex(end-1); i++ {
			if i >= len(fs.SegmentsDone) || !fs.SegmentsDone[i] {
				ok = false
				return
			}
		}
	})
	return ok
}

// SetFileState replaces (or inserts) the state for a file.
func (rs *RevisionState) SetFileState(fs FileState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.gen++
	rs.setLocked(fs.clone())
}

// find returns the state of the file at path, or nil. rs.mu is held.
func (rs *RevisionState) find(path string) *FileState {
	i, ok := rs.index[path]
	if ok && i < len(rs.Files) && rs.Files[i].Path == path {
		return &rs.Files[i]
	}
	if !ok && len(rs.index) == len(rs.Files) {
		return nil
	}
	// Files was set directly (see the type's comment): index it again.
	rs.index = make(map[string]int, len(rs.Files))
	for i := range rs.Files {
		rs.index[rs.Files[i].Path] = i
	}
	if i, ok := rs.index[path]; ok {
		return &rs.Files[i]
	}
	return nil
}

func (rs *RevisionState) setLocked(fs FileState) {
	if cur := rs.find(fs.Path); cur != nil {
		*cur = fs
		return
	}
	rs.Files = append(rs.Files, fs)
	rs.index = nil
}

// Gen returns a number that changes whenever what rs holds may have: a
// caller can keep what it worked out from rs until it does.
func (rs *RevisionState) Gen() uint64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.gen
}

// update applies fn to fm's state, created and fitted to fm's segments,
// atomically.
func (rs *RevisionState) update(fm FileManifest, fn func(*FileState)) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.gen++
	fs := rs.find(fm.Path)
	if fs == nil {
		rs.Files = append(rs.Files, FileState{Path: fm.Path})
		rs.index = nil
		fs = &rs.Files[len(rs.Files)-1]
	}
	fs.fit(fm)
	fn(fs)
}

// MarkSegment records segment i of fm as held, verified by trust. sum is
// its sha256; it is kept in the state only for a file of 64 MiB segments
// whose manifest entry has no segment hashes. A file of smaller segments
// keeps it in a side file, which the caller writes first (see
// Store.WriteSegmentHashes).
func (rs *RevisionState) MarkSegment(fm FileManifest, i int, sum, trust string) {
	now := time.Now().Unix()
	rs.update(fm, func(fs *FileState) {
		fs.SegmentsDone[i] = true
		fs.touch(fm, fm.ChunkOf(i), now)
		if !fm.Hashed() && !fm.Small() {
			// Kept whatever the trust: it is all a later copier or a
			// re-verify has to check this segment against.
			if fs.SegmentSHA256 == nil {
				fs.SegmentSHA256 = make([]string, len(fs.SegmentsDone))
			}
			fs.SegmentSHA256[i] = sum
		}
		fs.setTrust(i, trustCode(trust))
	})
}

// setTrust records segment i's trust code, keeping state small for files
// whose every segment is TrustManifest.
func (fs *FileState) setTrust(i int, c uint8) {
	if c == trustManifest && fs.trust == nil {
		return
	}
	if fs.trust == nil {
		fs.trust = make([]uint8, len(fs.SegmentsDone))
	}
	fs.trust[i] = c
}

func (fs *FileState) touch(fm FileManifest, c int, now int64) {
	if fs.Used == nil {
		fs.Used = make([]int64, fm.NumChunks())
	}
	if c >= 0 && c < len(fs.Used) {
		fs.Used[c] = now
	}
}

// Touch records segments first to last of fm as used now, as when they
// are served. The time is kept in memory until the state is next saved.
func (rs *RevisionState) Touch(fm FileManifest, first, last int) {
	now := time.Now().Unix()
	last = min(last, fm.NumSegments()-1)
	if first = max(first, 0); first > last {
		return
	}
	rs.update(fm, func(fs *FileState) {
		for c := fm.ChunkOf(first); c <= fm.ChunkOf(last); c++ {
			fs.touch(fm, c, now)
		}
	})
}

// LockFile locks the file at path against other checks of its held bytes:
// a whole-file check and a consumer checksum check of the same file must
// not interleave, or one could mark complete what the other just dropped.
// It returns the unlock.
func (rs *RevisionState) LockFile(path string) func() {
	rs.mu.Lock()
	if rs.locks == nil {
		rs.locks = make(map[string]*sync.Mutex)
	}
	l := rs.locks[path]
	if l == nil {
		l = &sync.Mutex{}
		rs.locks[path] = l
	}
	rs.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// AddChecked records that c matched the held bytes of fm.
func (rs *RevisionState) AddChecked(fm FileManifest, c RangeCheck) {
	rs.update(fm, func(fs *FileState) {
		if !fs.HasChecked(c) {
			fs.Checked = append(fs.Checked, c.Key())
		}
	})
}

// DropSegment records segment i of fm as not held, and the file as not
// complete. Checks that matched are forgotten, as the bytes will change.
func (rs *RevisionState) DropSegment(fm FileManifest, i int) {
	rs.update(fm, func(fs *FileState) {
		fs.SegmentsDone[i], fs.Complete, fs.WholeFailed = false, false, false
		fs.Checked = nil
		if i < len(fs.SegmentSHA256) {
			fs.SegmentSHA256[i] = ""
		}
		if i < len(fs.trust) {
			fs.trust[i] = trustManifest
		}
	})
}

// EvictSegment records segment i of fm as not held because eviction
// removed it, not because it was bad: only the checks that cover it are
// forgotten, so a request whose checked range is elsewhere in the file
// stays ready.
func (rs *RevisionState) EvictSegment(fm FileManifest, i int) {
	rs.EvictSegments(fm, []int{i})
}

// EvictSegments is EvictSegment for each of segs, at once.
func (rs *RevisionState) EvictSegments(fm FileManifest, segs []int) {
	rs.update(fm, func(fs *FileState) {
		var gone []Range
		for _, i := range segs {
			if i < 0 || i >= len(fs.SegmentsDone) {
				continue
			}
			fs.SegmentsDone[i], fs.Complete = false, false
			if i < len(fs.SegmentSHA256) {
				fs.SegmentSHA256[i] = ""
			}
			if i < len(fs.trust) {
				fs.trust[i] = trustManifest
			}
			start, end := fm.SegmentBounds(i)
			if n := len(gone); n > 0 && gone[n-1].End == start {
				gone[n-1].End = end
			} else {
				gone = append(gone, Range{start, end})
			}
		}
		kept := fs.Checked[:0]
		for _, k := range fs.Checked {
			var off, n int64
			if _, err := fmt.Sscanf(k, "%d+%d=", &off, &n); err == nil && overlapsAny(RangeCheck{Offset: off, Length: n}, gone) {
				continue
			}
			kept = append(kept, k)
		}
		fs.Checked = kept
	})
}

func overlapsAny(c RangeCheck, rs []Range) bool {
	for _, r := range rs {
		if c.Overlaps(r.Start, r.End) {
			return true
		}
	}
	return false
}

// SetTrust records how held segment i of fm was verified.
func (rs *RevisionState) SetTrust(fm FileManifest, i int, trust string) {
	rs.update(fm, func(fs *FileState) {
		if !fs.SegmentsDone[i] {
			return
		}
		fs.setTrust(i, trustCode(trust))
	})
}

// SetComplete marks fm complete: every segment held, and the whole file
// checked.
//
// A file whose manifest entry has no segment hashes was verified against
// the origin's whole-file sha256 by then, so its segments' trust becomes
// TrustManifest: they are the origin's bytes, and a consumer checksum that
// does not match them is the consumer's mistake, not damage. It reports
// false, changing nothing, if a segment was dropped since the caller
// looked.
func (rs *RevisionState) SetComplete(fm FileManifest) bool {
	ok := false
	rs.update(fm, func(fs *FileState) {
		for _, d := range fs.SegmentsDone {
			if !d {
				return
			}
		}
		ok, fs.Complete = true, true
		if !fm.Hashed() && fm.SHA256 != "" {
			fs.trust = nil // every entry TrustManifest
		}
	})
	return ok
}

// snapshot returns a deep copy of rs to save, stamped now, and whether
// any file has small segments.
func (rs *RevisionState) snapshot() (*RevisionState, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.UpdatedAt = time.Now().UTC()
	snap := &RevisionState{
		Model: rs.Model, Revision: rs.Revision, UpdatedAt: rs.UpdatedAt,
		LastSource: rs.LastSource, Origin: append([]OriginFile(nil), rs.Origin...),
	}
	small := false
	for _, fs := range rs.Files {
		snap.Files = append(snap.Files, fs.clone())
		small = small || fs.small
	}
	return snap, small
}

// NewRevisionState initializes empty state for every file in a manifest.
func NewRevisionState(m *Manifest) *RevisionState {
	rs := &RevisionState{Model: m.Model, Revision: m.Revision}
	for _, f := range m.Files {
		rs.Files = append(rs.Files, FileState{
			Path:         f.Path,
			SegmentsDone: make([]bool, f.NumSegments()),
			small:        f.Small(),
		})
	}
	return rs
}
