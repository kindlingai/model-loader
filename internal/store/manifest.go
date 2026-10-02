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

// NumSegments returns how many segments this file is divided into, treating
// a file with no segment hashes as a single segment spanning the whole file.
func (f FileManifest) NumSegments() int {
	if len(f.SegmentSHA256) > 0 {
		return len(f.SegmentSHA256)
	}
	return 1
}

// SegmentBounds returns the byte range [start, end) of segment i.
func (f FileManifest) SegmentBounds(i int) (start, end int64) {
	size := f.SegmentSize
	if size <= 0 {
		size = f.Size
	}
	start = int64(i) * size
	end = start + size
	if end > f.Size || len(f.SegmentSHA256) == 0 {
		end = f.Size
	}
	return start, end
}

// Manifest describes every file in a model revision.
type Manifest struct {
	Model    string         `json:"model"`
	Revision string         `json:"revision"`
	Files    []FileManifest `json:"files"`
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
// from disk at root. It is used when publishing a manifest for files this
// node fetched itself (e.g. in root mode, after a WAN download completes).
// When openFile returns an *os.File, its pages are dropped from page cache
// once hashed, so hashing a multi-hundred-GB model doesn't leave it resident.
func (m *Manifest) BuildSegmentHashes(openFile func(relPath string) (io.ReadCloser, error)) error {
	for i := range m.Files {
		f := &m.Files[i]
		rc, err := openFile(f.Path)
		if err != nil {
			return fmt.Errorf("open %s: %w", f.Path, err)
		}
		hashes, whole, err := hashSegments(rc, f.Size, DefaultSegmentSize)
		if err == nil {
			if file, ok := rc.(*os.File); ok {
				if uerr := Uncache(file, 0, f.Size); uerr != nil {
					err = fmt.Errorf("uncache: %w", uerr)
				}
			}
		}
		closeErr := rc.Close()
		if err != nil {
			return fmt.Errorf("hash %s: %w", f.Path, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close %s: %w", f.Path, closeErr)
		}
		f.SegmentSize = DefaultSegmentSize
		f.SegmentSHA256 = hashes
		f.SHA256 = whole
	}
	return nil
}

func hashSegments(r io.Reader, size, segSize int64) (segments []string, whole string, err error) {
	wholeHash := sha256.New()
	var remaining = size
	for remaining > 0 {
		n := segSize
		if n > remaining {
			n = remaining
		}
		segHash := sha256.New()
		w := io.MultiWriter(wholeHash, segHash)
		copied, err := io.CopyN(w, r, n)
		if err != nil && err != io.EOF {
			return nil, "", err
		}
		if copied != n {
			return nil, "", fmt.Errorf("short read: wanted %d bytes, got %d", n, copied)
		}
		segments = append(segments, hex.EncodeToString(segHash.Sum(nil)))
		remaining -= n
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
