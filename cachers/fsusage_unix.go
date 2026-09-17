//go:build unix

package cachers

import "golang.org/x/sys/unix"

func filesystemUsage(path string) (used, total uint64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	blockSize := uint64(stat.Bsize)
	total = stat.Blocks * blockSize
	available := stat.Bavail * blockSize
	return total - available, total, nil
}
