// Package store manages the local on-disk content store: the materialized
// file tree a node holds, the manifests describing what each model/revision
// should contain, and the per-file download state needed to resume and to
// safely serve partially-downloaded files to peers.
//
// The materialized files are the canonical store — there is no separate
// permanent chunk-blob layer alongside them. A manifest records each file's
// whole-file hash and, optionally, per-segment hashes so a file can be
// verified and served in pieces before it is wholly present.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// DefaultSegmentSize is the granularity at which a file's integrity is
// checked incrementally, letting a partially-downloaded file be verified and
// re-served before it completes.
const DefaultSegmentSize int64 = 64 << 20 // 64 MiB

// FileManifest describes one file within a model revision.
type FileManifest struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	// SHA256 is the whole-file digest, the final authority on correctness.
	SHA256 string `json:"sha256"`
	// SegmentSHA256, when present, holds one digest per SegmentSize-sized
	// chunk (the last one may be shorter), allowing early detection of
	// corruption and safe partial serving.
	SegmentSHA256 []string `json:"segment_sha256,omitempty"`
	SegmentSize   int64    `json:"segment_size,omitempty"`
}

// NumSegments returns how many segments this file is divided into. A
// zero-length file has none: there is nothing to fetch or verify per segment,
// only the whole-file hash of the empty content.
func (f FileManifest) NumSegments() int {
	if f.SegmentSize <= 0 || f.Size <= 0 {
		return len(f.SegmentSHA256)
	}
	return int(1 + (f.Size-1)/f.SegmentSize) // no overflow near MaxInt64
}

// SegmentBounds returns the byte range [start, end) of segment i.
func (f FileManifest) SegmentBounds(i int) (start, end int64) {
	size := f.SegmentSize
	if size <= 0 {
		size = f.Size
	}
	start = int64(i) * size
	if start > f.Size || start < 0 {
		return f.Size, f.Size // past the end: no bytes, never an overflow
	}
	end = start + min(size, f.Size-start)
	return start, end
}

// SegmentIndex returns the segment that holds byte off.
func (f FileManifest) SegmentIndex(off int64) int {
	if f.SegmentSize <= 0 {
		return 0
	}
	return int(off / f.SegmentSize)
}

// Manifest describes every file in a model revision.
type Manifest struct {
	Model    string         `json:"model"`
	Revision string         `json:"revision"`
	Files    []FileManifest `json:"files"`
}

// Validate checks that m is self-consistent: every path is a clean,
// relative, slash-separated path inside the revision and appears once, and
// every file has one segment hash per SegmentSize bytes. A manifest comes
// from another node, so a bad one must fail here rather than index out of
// range or write outside the store later.
func (m *Manifest) Validate() error {
	seen := make(map[string]bool, len(m.Files))
	for _, f := range m.Files {
		if f.Path == "" || path.IsAbs(f.Path) || path.Clean(f.Path) != f.Path || f.Path == ".." || strings.HasPrefix(f.Path, "../") || strings.Contains(f.Path, "\\") {
			return fmt.Errorf("manifest %s@%s: invalid path %q", m.Model, m.Revision, f.Path)
		}
		if seen[f.Path] {
			return fmt.Errorf("manifest %s@%s: duplicate path %q", m.Model, m.Revision, f.Path)
		}
		seen[f.Path] = true
		if f.Size < 0 {
			return fmt.Errorf("manifest %s@%s: %s: negative size", m.Model, m.Revision, f.Path)
		}
		want := 0
		if f.Size > 0 {
			if f.SegmentSize <= 0 {
				return fmt.Errorf("manifest %s@%s: %s: no segment size", m.Model, m.Revision, f.Path)
			}
			want = f.NumSegments()
			if want > maxSegmentsPerFile {
				return fmt.Errorf("manifest %s@%s: %s: %d segments, more than %d", m.Model, m.Revision, f.Path, want, maxSegmentsPerFile)
			}
		}
		// No segment hashes at all is a root's manifest for a file it does
		// not hold whole yet (see Hashed).
		if len(f.SegmentSHA256) != want && len(f.SegmentSHA256) != 0 {
			return fmt.Errorf("manifest %s@%s: %s: %d segment hashes, want %d", m.Model, m.Revision, f.Path, len(f.SegmentSHA256), want)
		}
	}
	return nil
}

// maxSegmentsPerFile bounds a file's segments, so a manifest from another
// node cannot make this one allocate per-segment state without bound. At
// 64 MiB segments it is 1 PiB per file.
const maxSegmentsPerFile = 1 << 24

// Hashed reports whether f lists a sha256 for every segment. A root
// publishes a manifest before it holds a file whole only for pull-through
// (see docs/design.md); that file has no segment hashes, and a node checks
// each segment against the sha256 the root recorded when it fetched it.
func (f FileManifest) Hashed() bool {
	return len(f.SegmentSHA256) == f.NumSegments()
}

// TotalSize sums every file's size.
func (m *Manifest) TotalSize() int64 {
	var total int64
	for _, f := range m.Files {
		total += f.Size
	}
	return total
}

// Find looks up a file by its manifest-relative path.
func (m *Manifest) Find(path string) (FileManifest, bool) {
	for _, f := range m.Files {
		if f.Path == path {
			return f, true
		}
	}
	return FileManifest{}, false
}

// BuildSegmentHashes computes SegmentSHA256 for every file by reading it
// from disk at root, in segments of each file's SegmentSize, or
// DefaultSegmentSize where that is not set. When openFile returns an
// *os.File, each segment is dropped from page cache as soon as it is
// hashed, so hashing a multi-hundred-GB model keeps at most one segment
// resident.
func (m *Manifest) BuildSegmentHashes(openFile func(relPath string) (io.ReadCloser, error)) error {
	for i := range m.Files {
		f := &m.Files[i]
		rc, err := openFile(f.Path)
		if err != nil {
			return fmt.Errorf("open %s: %w", f.Path, err)
		}
		var hashed func(off, n int64) error
		if file, ok := rc.(*os.File); ok {
			hashed = func(off, n int64) error { return Uncache(file, off, n) }
		}
		if f.SegmentSize <= 0 {
			f.SegmentSize = DefaultSegmentSize
		}
		hashes, whole, err := hashSegments(rc, f.Size, f.SegmentSize, hashed)
		closeErr := rc.Close()
		if err != nil {
			return fmt.Errorf("hash %s: %w", f.Path, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close %s: %w", f.Path, closeErr)
		}
		f.SegmentSHA256 = hashes
		f.SHA256 = whole
	}
	return nil
}

// HashFile computes the segment hashes and whole-file hash of f's first
// size bytes, as BuildSegmentHashes does, dropping each segment from page
// cache once hashed and calling progress with the bytes hashed so far.
func HashFile(f *os.File, size int64, progress func(int64)) (segments []string, whole string, err error) {
	return hashSegments(f, size, DefaultSegmentSize, func(off, n int64) error {
		progress(off + n)
		return Uncache(f, off, n)
	})
}

// hashSegments reads size bytes from r, hashing each segSize segment and the
// whole. hashed, if non-nil, runs after each segment with its byte range.
func hashSegments(r io.Reader, size, segSize int64, hashed func(off, n int64) error) (segments []string, whole string, err error) {
	wholeHash := sha256.New()
	for off := int64(0); off < size; {
		n := min(segSize, size-off)
		segHash := sha256.New()
		w := io.MultiWriter(wholeHash, segHash)
		copied, err := io.CopyN(w, r, n)
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if copied != n {
			return nil, "", fmt.Errorf("short read: wanted %d bytes, got %d", n, copied)
		}
		if hashed != nil {
			if err := hashed(off, n); err != nil {
				return nil, "", fmt.Errorf("uncache: %w", err)
			}
		}
		segments = append(segments, hex.EncodeToString(segHash.Sum(nil)))
		off += n
	}
	return segments, hex.EncodeToString(wholeHash.Sum(nil)), nil
}

// marshalJSONAtomic writes v as JSON to path, replacing any existing file
// only once the write is complete, so a reader never observes a partial
// manifest or state file.
func marshalJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func unmarshalJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
