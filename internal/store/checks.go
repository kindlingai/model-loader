package store

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/kindlingai/model-loader/internal/xxh64"
)

// RangeCheck is a consumer's checksum for Length bytes of a file at
// Offset: their XXH64 with seed 0. gr8 has one for every expert half in
// its index slices.
type RangeCheck struct {
	Offset int64
	Length int64
	XXH64  uint64
}

// Segments returns the indices of the segments of fm that c covers.
func (c RangeCheck) Segments(fm FileManifest) (first, last int) {
	return fm.SegmentIndex(c.Offset), fm.SegmentIndex(c.Offset + c.Length - 1)
}

// Inside reports whether c lies within [start, end).
func (c RangeCheck) Inside(start, end int64) bool {
	return c.Offset >= start && c.Offset+c.Length <= end
}

// CheckRange reports whether the bytes of c on disk in f match its
// checksum, dropping their pages from page cache afterwards.
func CheckRange(f *os.File, c RangeCheck) (bool, error) {
	d := xxh64.New(0)
	if err := hashUncached(f, c.Offset, c.Length, d); err != nil {
		return false, err
	}
	return d.Sum64() == c.XXH64, nil
}

// FormatChecks writes checks as an HTTP header value:
// "offset+length=xxh64,..." with the checksum in hex.
func FormatChecks(checks []RangeCheck) string {
	parts := make([]string, len(checks))
	for i, c := range checks {
		parts[i] = fmt.Sprintf("%d+%d=%s", c.Offset, c.Length, xxh64.Format(c.XXH64))
	}
	return strings.Join(parts, ",")
}

// maxHeaderChecks bounds how many checks one request may carry.
const maxHeaderChecks = 4096

// ParseChecks reads a header value FormatChecks wrote.
func ParseChecks(v string) ([]RangeCheck, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	parts := strings.Split(v, ",")
	if len(parts) > maxHeaderChecks {
		return nil, fmt.Errorf("checks: %d, more than %d", len(parts), maxHeaderChecks)
	}
	out := make([]RangeCheck, 0, len(parts))
	for _, p := range parts {
		rng, sum, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			return nil, fmt.Errorf("checks: %q: want offset+length=xxh64", p)
		}
		os, ls, ok := strings.Cut(rng, "+")
		if !ok {
			return nil, fmt.Errorf("checks: %q: want offset+length=xxh64", p)
		}
		off, err1 := strconv.ParseInt(os, 10, 64)
		n, err2 := strconv.ParseInt(ls, 10, 64)
		x, err3 := xxh64.Parse(sum)
		if err1 != nil || err2 != nil || err3 != nil || off < 0 || n <= 0 || off+n < off {
			return nil, fmt.Errorf("checks: %q: want offset+length=xxh64", p)
		}
		out = append(out, RangeCheck{Offset: off, Length: n, XXH64: x})
	}
	return out, nil
}
