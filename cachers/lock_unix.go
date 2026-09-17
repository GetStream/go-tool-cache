//go:build unix

package cachers

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

type fileLock struct {
	file *os.File
}

func acquireFileLock(path string, exclusive, nonblocking bool) (*fileLock, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, false, err
	}
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	if nonblocking {
		how |= unix.LOCK_NB
	}
	if err := unix.Flock(int(f.Fd()), how); err != nil {
		f.Close()
		if nonblocking && (errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &fileLock{file: f}, true, nil
}

func (l *fileLock) downgrade() error {
	return unix.Flock(int(l.file.Fd()), unix.LOCK_SH)
}

func (l *fileLock) close() error {
	return errors.Join(unix.Flock(int(l.file.Fd()), unix.LOCK_UN), l.file.Close())
}
