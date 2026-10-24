package server

import (
	"context"
	"errors"

	"github.com/kindlingai/model-loader/internal/store"
)

// Headers for pull-through and for verifying segments of a file that has
// no segment hashes in the manifest.
const (
	// HeaderRangeXXH64, on a blob request, carries a consumer's checksums
	// for ranges inside the requested one (see store.FormatChecks). A node
	// that fetches the range from its origin checks them before it serves
	// it.
	HeaderRangeXXH64 = "X-Range-XXH64"
	// HeaderSegmentSHA256, on a blob response for exactly one segment of a
	// file with no segment hashes in the manifest, is the sha256 the node
	// holds for that segment.
	HeaderSegmentSHA256 = "X-Segment-SHA256"
	// HeaderSegmentTrust goes with HeaderSegmentSHA256: how the node
	// verified the segment, one of the store.Trust constants.
	HeaderSegmentTrust = "X-Segment-Trust"
)

// ErrNotPullable means a Puller will not fetch that model or range: it is
// not in its catalog, or it is being downloaded whole and is published
// only when done.
var ErrNotPullable = errors.New("not available for pull-through")

// Puller fetches what a node does not hold yet from the model's origin,
// on demand. Only a root has one.
type Puller interface {
	// Manifest returns the manifest for model@revision, publishing one
	// from the origin's file list if there is none yet.
	Manifest(ctx context.Context, model, revision string) (*store.Manifest, error)
	// Pull returns once every segment of the file at path that
	// [start, end) touches is held, fetching the missing ones from the
	// origin, and every check is verified against the held bytes.
	Pull(ctx context.Context, model, revision, path string, start, end int64, checks []store.RangeCheck) error
}

// SetPuller makes s fetch from the origin, through p, a manifest or range
// it does not hold yet, instead of answering 404 or 416.
func (s *Server) SetPuller(p Puller) { s.puller = p }
