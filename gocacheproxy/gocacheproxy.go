// Package gocacheproxy is an HTTP reverse proxy that routes cache requests
// to backend gocached servers using consistent hashing on actionID.
package gocacheproxy

import (
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
	Verbose  bool
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

		resp, err := p.forward(r, b, r.URL.RequestURI())
		if err != nil {
			if p.Verbose {
				log.Printf("GET %s backend %d (%s) error: %v", r.URL.Path, idx, b.URL, err)
			}
			continue
		}

		copyResponse(w, resp)
		resp.Body.Close()
		return
	}

	http.Error(w, "all backends failed", http.StatusBadGateway)
}

// proxyPut forwards a PUT to the primary backend only (no fallback).
func (p *Proxy) proxyPut(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.Backends)
	primary := ServerIndex(actionID, n)
	b := p.Backends[primary]

	resp, err := p.forward(r, b, r.URL.RequestURI())
	if err != nil {
		if p.Verbose {
			log.Printf("PUT %s backend %d (%s) error: %v", r.URL.Path, primary, b.URL, err)
		}
		http.Error(w, "backend error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyResponse(w, resp)
}

func (p *Proxy) forward(orig *http.Request, b *Backend, path string) (*http.Response, error) {
	url := b.URL + path
	req, err := http.NewRequestWithContext(orig.Context(), orig.Method, url, orig.Body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = orig.ContentLength

	for _, h := range []string{"Authorization", "Want-Object", "Accept-Encoding", "Content-Type", "Content-Length"} {
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
