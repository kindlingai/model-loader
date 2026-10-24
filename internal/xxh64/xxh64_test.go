package xxh64

import (
	"bytes"
	"testing"
)

// The vectors gr8 checks in engine/loader-rs/src/xxh64.rs.
func TestKnownVectors(t *testing.T) {
	for _, tc := range []struct {
		in   string
		seed uint64
		want uint64
	}{
		{"", 0, 0xEF46DB3751D8E999},
		{"a", 0, 0xD24EC4F1A98C6E5B},
		{"abc", 0, 0x44BC2CF5AD770999},
		{"Nobody inspects the spammish repetition", 0, 0xFBCEA83C8A378BF1},
		{"", 1, 0xD5AFBA1336A3BE4B},
	} {
		d := New(tc.seed)
		d.Write([]byte(tc.in))
		if got := d.Sum64(); got != tc.want {
			t.Errorf("xxh64(%q, %d) = %#x, want %#x", tc.in, tc.seed, got, tc.want)
		}
	}
}

func TestStreamingEqualsOneShot(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdefghijklmnopqrstuvwxyz!"), 37)
	want := Sum64(data)
	for _, step := range []int{1, 3, 7, 31, 32, 33, 100} {
		d := New(0)
		for i := 0; i < len(data); i += step {
			d.Write(data[i:min(i+step, len(data))])
		}
		if got := d.Sum64(); got != want {
			t.Errorf("step %d: %#x, want %#x", step, got, want)
		}
	}
}

func TestParseFormat(t *testing.T) {
	for in, want := range map[string]uint64{"ef46db3751d8e999": 0xEF46DB3751D8E999, "0xEF46DB3751D8E999": 0xEF46DB3751D8E999, "1": 1} {
		if got, err := Parse(in); err != nil || got != want {
			t.Errorf("Parse(%q) = %#x, %v", in, got, err)
		}
	}
	for _, in := range []string{"", "0x", "xyz", "12345678901234567"} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q): want an error", in)
		}
	}
	if Format(1) != "0000000000000001" {
		t.Errorf("Format(1) = %q", Format(1))
	}
}
