//go:build !unix && !windows

package cachers

import "os"

type fileLock struct {
	file *os.File
}

func acquireFileLock(path string, _, _ bool) (*fileLock, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, false, err
	}
	return &fileLock{file: f}, true, nil
}

func (l *fileLock) downgrade() error { return nil }
func (l *fileLock) close() error     { return l.file.Close() }
