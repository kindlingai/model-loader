package store

import (
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

// FileState tracks download progress for one file within a revision:
// which segments have been verified so far, and how.
type FileState struct {
	Path     string `json:"path"`
	Complete bool   `json:"complete"`
	// SegmentsDone[i] is true once segment i has been written and
	// verified.
	SegmentsDone []bool `json:"segments_done"`
	// SegmentSHA256[i] is segment i's sha256, for a file whose manifest
	// entry has no segment hashes (see FileManifest.Hashed). Nil
	// otherwise.
	SegmentSHA256 []string `json:"segment_sha256,omitempty"`
	// Trust[i] is how segment i was verified, one of the Trust constants.
	// Nil, or an empty entry, means TrustManifest.
	Trust []string `json:"trust,omitempty"`
	// Checked lists the consumer checksums (RangeCheck.Key) that matched
	// the bytes held, so each is checked once, not on every pass.
	Checked []string `json:"checked,omitempty"`
	// Used[i] is when segment i was last fetched or served, in Unix
	// seconds; 0 if never recorded. Eviction removes the oldest first.
	Used []int64 `json:"used,omitempty"`
	// WholeFailed means every segment was held and the whole file did not
	// match its sha256, yet the segments matched the manifest's segment
	// hashes, so fetching them again cannot help: the manifest disagrees
	// with itself. Nothing of the file is served while it is set.
	WholeFailed bool `json:"whole_failed,omitempty"`
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
	if i >= len(fs.SegmentsDone) || !fs.SegmentsDone[i] {
		return ""
	}
	if i < len(fs.Trust) && fs.Trust[i] != "" {
		return fs.Trust[i]
	}
	return TrustManifest
}

// SegmentHash returns the sha256 segment i must match: the manifest's when
// it has one, else the one recorded when the segment was fetched. It is
// "" when there is neither.
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
	if fs.Trust != nil {
		fs.Trust = append([]string(nil), fs.Trust...)
	}
	if fs.Checked != nil {
		fs.Checked = append([]string(nil), fs.Checked...)
	}
	if fs.Used != nil {
		fs.Used = append([]int64(nil), fs.Used...)
	}
	return fs
}

// fit resizes fs's per-segment slices to n segments, keeping what lines
// up: state saved against a manifest with other segments is kept where it
// can be and fetched again where it can't.
func (fs *FileState) fit(n int) {
	resize := func(b []bool) []bool {
		out := make([]bool, n)
		copy(out, b)
		return out
	}
	resizeS := func(b []string) []string {
		if b == nil {
			return nil
		}
		out := make([]string, n)
		copy(out, b)
		return out
	}
	if len(fs.SegmentsDone) != n {
		fs.SegmentsDone = resize(fs.SegmentsDone)
	}
	if fs.SegmentSHA256 != nil && len(fs.SegmentSHA256) != n {
		fs.SegmentSHA256 = resizeS(fs.SegmentSHA256)
	}
	if fs.Trust != nil && len(fs.Trust) != n {
		fs.Trust = resizeS(fs.Trust)
	}
	if fs.Used != nil && len(fs.Used) != n {
		out := make([]int64, n)
		copy(out, fs.Used)
		fs.Used = out
	}
}

// LastUsed returns when segment i was last fetched or served, in Unix
// seconds, or 0 if never recorded.
func (fs FileState) LastUsed(i int) int64 {
	if i < len(fs.Used) {
		return fs.Used[i]
	}
	return 0
}

// BytesDone estimates completed bytes for progress reporting, treating each
// done segment as full-size except the last.
func (fs FileState) BytesDone(fm FileManifest) int64 {
	var total int64
	for i, done := range fs.SegmentsDone {
		if !done {
			continue
		}
		start, end := fm.SegmentBounds(i)
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
	var cur *Range
	for i, done := range fs.SegmentsDone {
		if !done {
			cur = nil
			continue
		}
		start, end := fm.SegmentBounds(i)
		if cur != nil && cur.End == start {
			cur.End = end
		} else {
			ranges = append(ranges, Range{Start: start, End: end})
			cur = &ranges[len(ranges)-1]
		}
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
	return FileState{Path: m.Path, SegmentsDone: make([]bool, m.NumSegments())}
}

// SetFileState replaces (or inserts) the state for a file.
func (rs *RevisionState) SetFileState(fs FileState) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.setLocked(fs.clone())
}

func (rs *RevisionState) find(path string) *FileState {
	for i := range rs.Files {
		if rs.Files[i].Path == path {
			return &rs.Files[i]
		}
	}
	return nil
}

func (rs *RevisionState) setLocked(fs FileState) {
	if cur := rs.find(fs.Path); cur != nil {
		*cur = fs
		return
	}
	rs.Files = append(rs.Files, fs)
}

// update applies fn to fm's state, created and fitted to fm's segments,
// atomically.
func (rs *RevisionState) update(fm FileManifest, fn func(*FileState)) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	fs := rs.find(fm.Path)
	if fs == nil {
		rs.Files = append(rs.Files, FileState{Path: fm.Path})
		fs = &rs.Files[len(rs.Files)-1]
	}
	fs.fit(fm.NumSegments())
	fn(fs)
}

// MarkSegment records segment i of fm as held, verified by trust. sum is
// its sha256; it is kept only for a file whose manifest entry has no
// segment hashes.
func (rs *RevisionState) MarkSegment(fm FileManifest, i int, sum, trust string) {
	rs.update(fm, func(fs *FileState) {
		fs.SegmentsDone[i] = true
		fs.touch(i, time.Now().Unix())
		if !fm.Hashed() {
			// Kept whatever the trust: it is all a later copier or a
			// re-verify has to check this segment against.
			if fs.SegmentSHA256 == nil {
				fs.SegmentSHA256 = make([]string, len(fs.SegmentsDone))
			}
			fs.SegmentSHA256[i] = sum
		}
		if trust == TrustManifest && fs.Trust == nil {
			return // the default; keep state small for whole models.
		}
		if fs.Trust == nil {
			fs.Trust = make([]string, len(fs.SegmentsDone))
		}
		fs.Trust[i] = trust
	})
}

func (fs *FileState) touch(i int, now int64) {
	if fs.Used == nil {
		fs.Used = make([]int64, len(fs.SegmentsDone))
	}
	if i < len(fs.Used) {
		fs.Used[i] = now
	}
}

// Touch records segments first to last of fm as used now, as when they
// are served. The time is kept in memory until the state is next saved.
func (rs *RevisionState) Touch(fm FileManifest, first, last int) {
	now := time.Now().Unix()
	rs.update(fm, func(fs *FileState) {
		for i := max(first, 0); i <= last && i < len(fs.SegmentsDone); i++ {
			fs.touch(i, now)
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
		if i < len(fs.Trust) {
			fs.Trust[i] = ""
		}
	})
}

// EvictSegment records segment i of fm as not held because eviction
// removed it, not because it was bad: only the checks that cover it are
// forgotten, so a request whose checked range is elsewhere in the file
// stays ready.
func (rs *RevisionState) EvictSegment(fm FileManifest, i int) {
	start, end := fm.SegmentBounds(i)
	rs.update(fm, func(fs *FileState) {
		fs.SegmentsDone[i], fs.Complete = false, false
		kept := fs.Checked[:0]
		for _, k := range fs.Checked {
			var off, n int64
			if _, err := fmt.Sscanf(k, "%d+%d=", &off, &n); err == nil && (RangeCheck{Offset: off, Length: n}).Overlaps(start, end) {
				continue
			}
			kept = append(kept, k)
		}
		fs.Checked = kept
		if i < len(fs.SegmentSHA256) {
			fs.SegmentSHA256[i] = ""
		}
		if i < len(fs.Trust) {
			fs.Trust[i] = ""
		}
	})
}

// SetTrust records how held segment i of fm was verified.
func (rs *RevisionState) SetTrust(fm FileManifest, i int, trust string) {
	rs.update(fm, func(fs *FileState) {
		if !fs.SegmentsDone[i] {
			return
		}
		if fs.Trust == nil {
			fs.Trust = make([]string, len(fs.SegmentsDone))
		}
		fs.Trust[i] = trust
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
			fs.Trust = nil // every entry TrustManifest
		}
	})
	return ok
}

// snapshot returns a deep copy of rs to save, stamped now.
func (rs *RevisionState) snapshot() *RevisionState {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.UpdatedAt = time.Now().UTC()
	snap := &RevisionState{
		Model: rs.Model, Revision: rs.Revision, UpdatedAt: rs.UpdatedAt,
		LastSource: rs.LastSource, Origin: append([]OriginFile(nil), rs.Origin...),
	}
	for _, fs := range rs.Files {
		snap.Files = append(snap.Files, fs.clone())
	}
	return snap
}

// NewRevisionState initializes empty state for every file in a manifest.
func NewRevisionState(m *Manifest) *RevisionState {
	rs := &RevisionState{Model: m.Model, Revision: m.Revision}
	for _, f := range m.Files {
		rs.Files = append(rs.Files, FileState{
			Path:         f.Path,
			SegmentsDone: make([]bool, f.NumSegments()),
		})
	}
	return rs
}
