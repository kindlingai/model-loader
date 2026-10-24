//go:build darwin

package store

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// fpunchhole is struct fpunchhole from <sys/fcntl.h>.
type fpunchhole struct {
	flags    uint32
	reserved uint32
	offset   int64
	length   int64
}

// punchHole frees the disk blocks under [offset, offset+length) of f with
// F_PUNCHHOLE (APFS), keeping its size. The range is shrunk to whole
// filesystem blocks, as F_PUNCHHOLE requires; a partial block at either
// end stays allocated.
func punchHole(f *os.File, offset, length int64) error {
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	bs := int64(st.Blksize)
	if bs <= 0 {
		bs = 4096
	}
	start := (offset + bs - 1) / bs * bs
	end := (offset + length) / bs * bs
	if end <= start {
		return nil
	}
	arg := fpunchhole{offset: start, length: end - start}
	_, _, errno := unix.Syscall(unix.SYS_FCNTL, f.Fd(), unix.F_PUNCHHOLE, uintptr(unsafe.Pointer(&arg)))
	if errno != 0 {
		return errno
	}
	return nil
}
