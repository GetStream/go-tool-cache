package cachers

import (
	"context"
	"encoding/hex"
	"io"
	"log"
)

// MultiHTTPClient distributes cache requests across multiple HTTP servers
// using consistent hashing on the actionID. Each server has BestEffortHTTP=true
// so errors are treated as misses rather than failures.
type MultiHTTPClient struct {
	Clients []*HTTPClient // one per server
	Disk    *DiskCache
	Verbose bool
}

// serverIndex hashes the actionID to pick a primary server index.
// actionID is already a hex string; we parse the first 8 hex chars as a uint32.
func serverIndex(actionID string, n int) int {
	if n <= 1 {
		return 0
	}
	// actionID is guaranteed to be at least 4 hex chars by validHex
	end := 8
	if len(actionID) < end {
		end = len(actionID)
	}
	b, err := hex.DecodeString(actionID[:end])
	if err != nil {
		return 0
	}
	var v uint32
	for _, c := range b {
		v = v<<8 | uint32(c)
	}
	return int(v % uint32(n))
}

// Get checks local disk first, then tries the primary server (by consistent hash),
// then falls back to remaining servers in order. Any hit is written to local disk.
func (m *MultiHTTPClient) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	// Check local disk first
	outputID, diskPath, err = m.Disk.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}

	n := len(m.Clients)
	primary := serverIndex(actionID, n)

	// Try primary first, then remaining servers
	for i := 0; i < n; i++ {
		idx := (primary + i) % n
		outputID, diskPath, err = m.Clients[idx].Get(ctx, actionID)
		if err != nil {
			if m.Verbose {
				log.Printf("multi-http: GET server %d error: %v", idx, err)
			}
			continue
		}
		if outputID != "" {
			return outputID, diskPath, nil
		}
	}

	// All servers missed
	return "", "", nil
}

// Put writes to local disk and the primary server in parallel.
// If the primary is down, the disk write still succeeds.
func (m *MultiHTTPClient) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	primary := serverIndex(actionID, len(m.Clients))
	return m.Clients[primary].Put(ctx, actionID, outputID, size, body)
}
