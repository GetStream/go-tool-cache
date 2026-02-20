package cachers

import (
	"context"
	"encoding/hex"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const retryInterval = 30 * time.Second

type serverHealth struct {
	healthy   atomic.Bool
	lastRetry atomic.Int64 // unix nanoseconds
}

// MultiHTTPClient distributes cache requests across multiple HTTP servers
// using consistent hashing on the actionID. It tracks per-server health,
// logging only state transitions instead of every failed request.
type MultiHTTPClient struct {
	Clients []*HTTPClient // one per server
	Disk    *DiskCache
	Verbose bool

	once   sync.Once
	health []serverHealth
}

func (m *MultiHTTPClient) initHealth() {
	m.once.Do(func() {
		m.health = make([]serverHealth, len(m.Clients))
		for i := range m.health {
			m.health[i].healthy.Store(true)
		}
		for i := range m.Clients {
			m.Clients[i].ConnCallback = func(err error) {
				if err != nil {
					m.markUnhealthy(i)
				} else {
					m.markHealthy(i)
				}
			}
		}
	})
}

func (m *MultiHTTPClient) markUnhealthy(idx int) {
	if m.health[idx].healthy.CompareAndSwap(true, false) {
		healthy := m.countHealthy()
		log.Printf("go-cacher: server %s unreachable, %d/%d servers available",
			m.Clients[idx].BaseURL, healthy, len(m.Clients))
	}
}

func (m *MultiHTTPClient) markHealthy(idx int) {
	if m.health[idx].healthy.CompareAndSwap(false, true) {
		healthy := m.countHealthy()
		log.Printf("go-cacher: server %s recovered, %d/%d servers available",
			m.Clients[idx].BaseURL, healthy, len(m.Clients))
	}
}

func (m *MultiHTTPClient) countHealthy() int {
	n := 0
	for i := range m.health {
		if m.health[i].healthy.Load() {
			n++
		}
	}
	return n
}

func (m *MultiHTTPClient) shouldTry(idx int) bool {
	if m.health[idx].healthy.Load() {
		return true
	}
	last := m.health[idx].lastRetry.Load()
	now := time.Now().UnixNano()
	if now-last > int64(retryInterval) {
		m.health[idx].lastRetry.Store(now)
		return true
	}
	return false
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
	m.initHealth()

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

// Put writes to local disk and the primary server.
// Unhealthy servers are skipped unless due for a retry probe.
func (m *MultiHTTPClient) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	m.initHealth()
	primary := serverIndex(actionID, len(m.Clients))

	if !m.shouldTry(primary) {
		return m.Disk.Put(ctx, actionID, outputID, size, body)
	}

	return m.Clients[primary].Put(ctx, actionID, outputID, size, body)
}
