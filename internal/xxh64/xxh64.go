// Package xxh64 is XXH64, the 64-bit xxHash, with seed 0 by default. gr8
// records an XXH64 of every expert half in its index slices; a consumer can
// pass those to model-loader so it verifies the ranges it fetches before it
// holds or serves them (see store.RangeCheck).
package xxh64

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

const (
	p1 uint64 = 11400714785074694791
	p2 uint64 = 14029467366897019727
	p3 uint64 = 1609587929392839161
	p4 uint64 = 9650029242287828579
	p5 uint64 = 2870177450012600261
)

func round(acc, lane uint64) uint64 {
	acc += lane * p2
	acc = bits.RotateLeft64(acc, 31)
	return acc * p1
}

func merge(h, v uint64) uint64 {
	h ^= round(0, v)
	return h*p1 + p4
}

// Digest is a streaming XXH64. Its zero value is not ready; use New.
type Digest struct {
	seed           uint64
	v1, v2, v3, v4 uint64
	total          uint64
	buf            [32]byte
	n              int
}

// New returns a Digest with the given seed.
func New(seed uint64) *Digest {
	d := &Digest{}
	d.Reset(seed)
	return d
}

// Reset clears d and sets its seed.
func (d *Digest) Reset(seed uint64) {
	*d = Digest{seed: seed, v1: seed + p1 + p2, v2: seed + p2, v3: seed, v4: seed - p1}
}

func (d *Digest) stripe(b []byte) {
	d.v1 = round(d.v1, binary.LittleEndian.Uint64(b[0:]))
	d.v2 = round(d.v2, binary.LittleEndian.Uint64(b[8:]))
	d.v3 = round(d.v3, binary.LittleEndian.Uint64(b[16:]))
	d.v4 = round(d.v4, binary.LittleEndian.Uint64(b[24:]))
}

// Write adds b to the hash. It never fails.
func (d *Digest) Write(b []byte) (int, error) {
	n := len(b)
	d.total += uint64(n)
	if d.n > 0 {
		c := copy(d.buf[d.n:], b)
		d.n += c
		b = b[c:]
		if d.n < 32 {
			return n, nil
		}
		d.stripe(d.buf[:])
		d.n = 0
	}
	for len(b) >= 32 {
		d.stripe(b)
		b = b[32:]
	}
	d.n = copy(d.buf[:], b)
	return n, nil
}

// Sum64 returns the hash of everything written so far.
func (d *Digest) Sum64() uint64 {
	var h uint64
	if d.total >= 32 {
		h = bits.RotateLeft64(d.v1, 1) + bits.RotateLeft64(d.v2, 7) + bits.RotateLeft64(d.v3, 12) + bits.RotateLeft64(d.v4, 18)
		h = merge(h, d.v1)
		h = merge(h, d.v2)
		h = merge(h, d.v3)
		h = merge(h, d.v4)
	} else {
		h = d.seed + p5
	}
	h += d.total
	b := d.buf[:d.n]
	for len(b) >= 8 {
		h ^= round(0, binary.LittleEndian.Uint64(b))
		h = bits.RotateLeft64(h, 27)*p1 + p4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b)) * p1
		h = bits.RotateLeft64(h, 23)*p2 + p3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * p5
		h = bits.RotateLeft64(h, 11) * p1
	}
	h ^= h >> 33
	h *= p2
	h ^= h >> 29
	h *= p3
	h ^= h >> 32
	return h
}

// Sum64 returns the XXH64 of b with seed 0.
func Sum64(b []byte) uint64 {
	d := New(0)
	d.Write(b)
	return d.Sum64()
}

// Parse reads a checksum written as hex, with or without a 0x prefix, the
// way model-loader's selection and request API take it.
func Parse(s string) (uint64, error) {
	t := strings.TrimPrefix(strings.ToLower(s), "0x")
	if t == "" || len(t) > 16 {
		return 0, fmt.Errorf("xxh64 %q: want 1 to 16 hex digits", s)
	}
	v, err := strconv.ParseUint(t, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("xxh64 %q: want hex digits", s)
	}
	return v, nil
}

// Format writes v as 16 hex digits.
func Format(v uint64) string { return fmt.Sprintf("%016x", v) }
