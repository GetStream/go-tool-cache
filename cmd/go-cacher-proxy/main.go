// The go-cacher-proxy is an HTTP reverse proxy that routes cache requests
// to backend gocached servers using consistent hashing on actionID.
//
// It sits between go-cacher clients and gocached backends, owning the
// sharding logic so clients can connect to any proxy instance.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	listenAddr   = flag.String("listen", ":31365", "listen address")
	backendsFlag = flag.String("backends", "", "comma-separated backend addresses (host:port)")
	verbose      = flag.Bool("verbose", false, "verbose logging")
	healthPath   = "/health"
)

func main() {
	flag.Parse()

	if *backendsFlag == "" {
		log.Fatal("--backends is required")
	}

	addrs := strings.Split(*backendsFlag, ",")
	backends := make([]*backend, len(addrs))
	for i, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if !strings.HasPrefix(addr, "http") {
			addr = "http://" + addr
		}
		backends[i] = &backend{url: addr}
		backends[i].healthy.Store(true)
	}

	p := &proxy{
		backends: backends,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		verbose: *verbose,
	}

	go p.healthCheck()

	log.Printf("go-cacher-proxy listening on %s with %d backends", *listenAddr, len(backends))
	log.Fatal(http.ListenAndServe(*listenAddr, p))
}

type backend struct {
	url     string
	healthy atomic.Bool
}

type proxy struct {
	backends []*backend
	client   *http.Client
	verbose  bool
}

// serverIndex hashes the actionID to pick a primary backend index.
// Duplicated from cachers/multi_http.go to avoid importing that package.
func serverIndex(actionID string, n int) int {
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

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
func (p *proxy) proxyGet(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.backends)
	primary := serverIndex(actionID, n)

	for i := 0; i < n; i++ {
		idx := (primary + i) % n
		b := p.backends[idx]
		if i > 0 && !b.healthy.Load() {
			continue
		}

		resp, err := p.forward(r, b, r.URL.RequestURI())
		if err != nil {
			if p.verbose {
				log.Printf("GET %s backend %d (%s) error: %v", r.URL.Path, idx, b.url, err)
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
func (p *proxy) proxyPut(w http.ResponseWriter, r *http.Request, actionID string) {
	n := len(p.backends)
	primary := serverIndex(actionID, n)
	b := p.backends[primary]

	resp, err := p.forward(r, b, r.URL.RequestURI())
	if err != nil {
		if p.verbose {
			log.Printf("PUT %s backend %d (%s) error: %v", r.URL.Path, primary, b.url, err)
		}
		http.Error(w, "backend error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyResponse(w, resp)
}

// forward creates a new request to the backend and returns the response.
func (p *proxy) forward(orig *http.Request, b *backend, path string) (*http.Response, error) {
	url := b.url + path
	req, err := http.NewRequestWithContext(orig.Context(), orig.Method, url, orig.Body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = orig.ContentLength

	// Pass through relevant headers.
	for _, h := range []string{"Authorization", "Want-Object", "Accept-Encoding", "Content-Type", "Content-Length"} {
		if v := orig.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}

	return p.client.Do(req)
}

// copyResponse writes the backend response to the client.
func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *proxy) handleHealth(w http.ResponseWriter, _ *http.Request) {
	for _, b := range p.backends {
		if b.healthy.Load() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "ok\n")
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, "no healthy backends\n")
}

// healthCheck pings each backend every 10s.
func (p *proxy) healthCheck() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	check := func() {
		var wg sync.WaitGroup
		for _, b := range p.backends {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Get(b.url + "/")
				healthy := err == nil
				if resp != nil {
					resp.Body.Close()
				}
				was := b.healthy.Swap(healthy)
				if was != healthy {
					log.Printf("backend %s health: %v -> %v", b.url, was, healthy)
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
