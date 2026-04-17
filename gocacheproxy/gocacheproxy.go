// Package gocacheproxy is an HTTP reverse proxy that routes cache requests
// to backend gocached servers using consistent hashing on actionID.
package gocacheproxy

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Backend is a single gocached server.
type Backend struct {
	URL     string
	healthy atomic.Bool
}

// Proxy routes cache requests to backends using consistent hashing.
//
// The proxy is best-effort: GET returns 200 (hit) or 404 (miss/error);
// PUT always returns 204 even if the upload ultimately failed, because a
// cache failure should never break the build — the compiler will just
// re-cache next time. All errors are logged and counted so we can observe
// cache health separately from the runner-visible outcome.
type Proxy struct {
	Backends []*Backend
	Client   *http.Client
	// Retries is the number of retries after the initial attempt on PUT.
	// Total attempts = 1 + Retries. Set to 0 to disable retries.
	Retries int
	// MaxInflightBytes is the soft cap on total buffered PUT body bytes.
	// A PUT whose Content-Length would push us over is dropped silently
	// (logged and counted). 0 disables the check.
	MaxInflightBytes int64
	Verbose          bool

	stats         stats
	inflightBytes atomic.Int64
}

type stats struct {
	GetHit     atomic.Int64
	GetMiss    atomic.Int64
	GetErr     atomic.Int64 // transport error or unexpected status from backend
	PutOK      atomic.Int64
	PutErr     atomic.Int64 // retries exhausted
	PutDropped atomic.Int64 // shed before attempting, due to inflight budget
}

const healthPath = "/health"

// ServerIndex hashes the actionID to pick a primary backend index.
// Duplicated from cachers/multi_http.go to avoid importing that package.
func ServerIndex(actionID string, n int) int {
	if n <= 1 {
		return 0
	}
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

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == healthPath:
		p.handleHealth(w, r)
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/action/"):
		actionID := strings.TrimPrefix(r.URL.Path, "/action/")
		p.proxyGet(w, r, actionID)
	case r.Method == "PUT":
		// PUT /<actionID>/<outputID>
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		if len(parts) != 2 {
			http.Error(w, "bad URI", http.StatusBadRequest)
			return
		}
		p.proxyPut(w, r, parts[0])
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// proxyGet forwards a GET /action/<actionID> to the primary backend only.
// On transport error or unexpected status, returns 404 (cache miss) so that
// the runner just rebuilds. Never returns 5xx to the caller.
func (p *Proxy) proxyGet(w http.ResponseWriter, r *http.Request, actionID string) {
	primary := ServerIndex(actionID, len(p.Backends))
	b := p.Backends[primary]

	resp, err := p.forward(r, b, r.URL.RequestURI(), nil)
	if err != nil {
		p.stats.GetErr.Add(1)
		log.Printf("GET %s backend %d (%s) error: %v", r.URL.Path, primary, b.URL, err)
		http.NotFound(w, r)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		p.stats.GetHit.Add(1)
	case http.StatusNotFound:
		p.stats.GetMiss.Add(1)
	default:
		p.stats.GetErr.Add(1)
		log.Printf("GET %s backend %d (%s) unexpected status %d",
			r.URL.Path, primary, b.URL, resp.StatusCode)
	}
	copyResponse(w, resp)
}

// proxyPut forwards a PUT to the primary backend with retries. The body is
// buffered so it can be resent. Never returns 5xx to the caller: on exhausted
// retries we log and return 204. A failed PUT just means the next build will
// re-upload.
func (p *Proxy) proxyPut(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.Backends)
	primary := ServerIndex(actionID, n)
	b := p.Backends[primary]

	// Shed under memory pressure: reserve inflight budget before reading the
	// full body into memory.
	reserve := r.ContentLength
	if reserve < 0 {
		reserve = 32 << 20 // unknown size → conservative reservation
	}
	if !p.reserveInflight(reserve) {
		p.stats.PutDropped.Add(1)
		log.Printf("PUT %s dropped: inflight %d+%d > %d (bytes)",
			r.URL.Path, p.inflightBytes.Load(), reserve, p.MaxInflightBytes)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	defer p.releaseInflight(reserve)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.stats.PutErr.Add(1)
		log.Printf("PUT %s read body error: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	attempts := 1 + p.Retries
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		resp, err := p.forward(r, b, r.URL.RequestURI(), body)
		if err == nil {
			if attempt > 1 {
				log.Printf("PUT %s backend %d (%s) ok on attempt %d/%d (%d bytes)",
					r.URL.Path, primary, b.URL, attempt, attempts, len(body))
			}
			p.stats.PutOK.Add(1)
			copyResponse(w, resp)
			resp.Body.Close()
			return
		}
		lastErr = err
		log.Printf("PUT %s backend %d (%s) attempt %d/%d error (%d bytes): %v",
			r.URL.Path, primary, b.URL, attempt, attempts, len(body), err)
		if attempt < attempts {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}
	}
	p.stats.PutErr.Add(1)
	log.Printf("PUT %s backend %d (%s) failed after %d attempts (%d bytes); last error: %v",
		r.URL.Path, primary, b.URL, attempts, len(body), lastErr)
	w.WriteHeader(http.StatusNoContent)
}

// LogStats periodically logs aggregate counters. Blocks forever; run in a
// goroutine.
func (p *Proxy) LogStats(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		h := p.stats.GetHit.Load()
		m := p.stats.GetMiss.Load()
		e := p.stats.GetErr.Load()
		po := p.stats.PutOK.Load()
		pe := p.stats.PutErr.Load()
		pd := p.stats.PutDropped.Load()
		var hitRate float64
		if hm := h + m; hm > 0 {
			hitRate = float64(h) * 100 / float64(hm)
		}
		log.Printf("stats: gets=%d hits=%d misses=%d errors=%d hit_rate=%.1f%% | puts=%d ok=%d errors=%d dropped=%d inflight_bytes=%d",
			h+m+e, h, m, e, hitRate, po+pe+pd, po, pe, pd, p.inflightBytes.Load())
	}
}

// reserveInflight atomically reserves n bytes of inflight PUT budget. Returns
// false if the reservation would exceed MaxInflightBytes. When
// MaxInflightBytes is 0, always succeeds.
func (p *Proxy) reserveInflight(n int64) bool {
	if p.MaxInflightBytes <= 0 {
		return true
	}
	for {
		cur := p.inflightBytes.Load()
		if cur+n > p.MaxInflightBytes {
			return false
		}
		if p.inflightBytes.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// releaseInflight releases a previously-reserved inflight budget.
func (p *Proxy) releaseInflight(n int64) {
	if p.MaxInflightBytes <= 0 {
		return
	}
	p.inflightBytes.Add(-n)
}

// DetectMemoryLimit reads the current container's memory limit from the
// cgroup filesystem. Returns the limit in bytes on success, or an error if
// the container has no limit configured or the cgroup info is unreadable.
// Reads cgroup v2 first, falls back to cgroup v1.
func DetectMemoryLimit() (int64, error) {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(b))
		if s == "max" {
			return 0, errors.New("cgroup v2: no memory limit set")
		}
		return strconv.ParseInt(s, 10, 64)
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return 0, err
		}
		// cgroup v1 uses a very large sentinel to mean "no limit".
		if v >= 1<<60 {
			return 0, errors.New("cgroup v1: no memory limit set")
		}
		return v, nil
	}
	return 0, errors.New("no cgroup memory info found")
}

// forward sends a request to a backend. If body is non-nil, it's used as the
// request body; otherwise the request is sent with no body (for GET).
func (p *Proxy) forward(orig *http.Request, b *Backend, path string, body []byte) (*http.Response, error) {
	url := b.URL + path
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(orig.Context(), orig.Method, url, reqBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}

	for _, h := range []string{"Authorization", "Want-Object", "Accept-Encoding", "Content-Type"} {
		if v := orig.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}

	return p.Client.Do(req)
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *Proxy) handleHealth(w http.ResponseWriter, _ *http.Request) {
	for _, b := range p.Backends {
		if b.healthy.Load() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "ok\n")
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, "no healthy backends\n")
}

// HealthCheck pings each backend every 10s. Blocks forever.
func (p *Proxy) HealthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	check := func() {
		var wg sync.WaitGroup
		for _, b := range p.Backends {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Get(b.URL + "/")
				healthy := err == nil
				if resp != nil {
					resp.Body.Close()
				}
				was := b.healthy.Swap(healthy)
				if was != healthy {
					log.Printf("backend %s health: %v -> %v", b.URL, was, healthy)
				}
			}()
		}
		wg.Wait()
	}

	check()
	for range ticker.C {
		check()
	}
}
