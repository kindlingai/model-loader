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
	// HeaderNoPull, on a blob request, asks a root to serve only what it
	// holds and not to fetch from its origin. A root filling its own
	// pull-through from LAN nodes sends it, so two roots never wait on
	// each other's pulls.
	HeaderNoPull = "X-No-Pull"
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

// HeaderPriority, on a blob request, is the priority of the request it
// serves on the asking node (see the local request API). A root asks for a
// pull-through slot at it, so a higher one fetched through the root is
// not stuck behind lower ones there either.
const HeaderPriority = "X-Priority"

type priorityKey struct{}

// WithPriority marks ctx with the priority p of the work it is for.
func WithPriority(ctx context.Context, p int) context.Context {
	return context.WithValue(ctx, priorityKey{}, p)
}

// Priority returns the priority ctx was marked with, or 0.
func Priority(ctx context.Context) int {
	p, _ := ctx.Value(priorityKey{}).(int)
	return p
}

// SetPuller makes s fetch from the origin, through p, a manifest or range
// it does not hold yet, instead of answering 404 or 416.
func (s *Server) SetPuller(p Puller) { s.puller = p }
