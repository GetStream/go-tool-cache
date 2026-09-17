//go:build !unix

package cachers

// Filesystem watermarks are unavailable on this platform. MaxBytes eviction
// remains active.
func filesystemUsage(string) (used, total uint64, err error) {
	return 0, 0, nil
}
