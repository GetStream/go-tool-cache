// Package gocacheproxy is an HTTP reverse proxy that routes cache requests
// to backend gocached servers using consistent hashing on actionID.
package gocacheproxy

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
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
type Proxy struct {
	Backends []*Backend
	Client   *http.Client
	// Retries is the number of retries after the initial attempt on PUT.
	// Total attempts = 1 + Retries. Set to 0 to disable retries.
	Retries int
	Verbose bool
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

// proxyGet forwards a GET /action/<actionID> to the primary backend,
// falling back to other backends on failure.
func (p *Proxy) proxyGet(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.Backends)
	primary := ServerIndex(actionID, n)

	for i := 0; i < n; i++ {
		idx := (primary + i) % n
		b := p.Backends[idx]
		if i > 0 && !b.healthy.Load() {
			continue
		}

		resp, err := p.forward(r, b, r.URL.RequestURI(), nil)
		if err != nil {
			log.Printf("GET %s backend %d (%s) error: %v", r.URL.Path, idx, b.URL, err)
			continue
		}

		copyResponse(w, resp)
		resp.Body.Close()
		return
	}

	http.Error(w, "all backends failed", http.StatusBadGateway)
}

// proxyPut forwards a PUT to the primary backend with retries. The body is
// buffered so it can be resent; on persistent failure, returns 502.
func (p *Proxy) proxyPut(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.Backends)
	primary := ServerIndex(actionID, n)
	b := p.Backends[primary]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("PUT %s read body error: %v", r.URL.Path, err)
		http.Error(w, "read body", http.StatusBadRequest)
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
	log.Printf("PUT %s backend %d (%s) failed after %d attempts (%d bytes); last error: %v",
		r.URL.Path, primary, b.URL, attempts, len(body), lastErr)
	http.Error(w, "backend error", http.StatusBadGateway)
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
