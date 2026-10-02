package store

import "time"

// FileState tracks download progress for one file within a revision:
// which segments have been verified against the manifest so far.
type FileState struct {
	Path     string `json:"path"`
	Complete bool   `json:"complete"`
	// SegmentsDone[i] is true once segment i has been written and its
	// hash checked against the manifest's SegmentSHA256[i].
	SegmentsDone []bool `json:"segments_done"`
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

// RevisionState is the per-file download state for one model revision.
type RevisionState struct {
	Model      string      `json:"model"`
	Revision   string      `json:"revision"`
	Files      []FileState `json:"files"`
	UpdatedAt  time.Time   `json:"updated_at"`
	LastSource string      `json:"last_source,omitempty"`
}

// Complete reports whether every file in the revision is complete.
func (rs *RevisionState) Complete() bool {
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
	var total int64
	for _, fs := range rs.Files {
		if fm, ok := m.Find(fs.Path); ok {
			total += fs.BytesDone(fm)
		}
	}
	return total
}

// FileState looks up (and lazily initializes) the state for one file.
func (rs *RevisionState) FileState(m FileManifest) FileState {
	for _, fs := range rs.Files {
		if fs.Path == m.Path {
			return fs
		}
	}
	return FileState{Path: m.Path, SegmentsDone: make([]bool, m.NumSegments())}
}

// SetFileState replaces (or inserts) the state for a file.
func (rs *RevisionState) SetFileState(fs FileState) {
	for i := range rs.Files {
		if rs.Files[i].Path == fs.Path {
			rs.Files[i] = fs
			return
		}
	}
	rs.Files = append(rs.Files, fs)
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
