package transfer

import (
	"context"
	"os"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/kindlingai/model-loader/internal/store"
)

// A transfer writes through a write-only descriptor and hashes what it
// writes as it streams in: on NFS a descriptor open for reading makes the
// client read back pages a write covers only part of (#37), and reading
// a segment back to hash it costs as much as writing it (#49).
func TestFetchFileWritesWriteOnly(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("0123456789abcdefghij")
	fm := buildManifestFile("w.bin", content, 10)
	m := &store.Manifest{Model: "m", Revision: "r1", Files: []store.FileManifest{fm}}
	var modes []int
	hookSync(t, func(f *os.File) error {
		fl, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
		if err != nil {
			return err
		}
		modes = append(modes, fl&unix.O_ACCMODE)
		return f.Sync()
	})
	rs := store.NewRevisionState(m)
	if err := FetchFile(context.Background(), st, "m", "r1", fm, rs, []Fetcher{&fakeFetcher{content: content, ready: 20}}, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(modes) == 0 {
		t.Fatal("no transfer synced")
	}
	for _, mode := range modes {
		if mode != unix.O_WRONLY {
			t.Fatalf("transfer synced a descriptor opened with access mode %d, want O_WRONLY", mode)
		}
	}
	if !rs.FileState(fm).Complete {
		t.Fatal("file not complete")
	}
}
