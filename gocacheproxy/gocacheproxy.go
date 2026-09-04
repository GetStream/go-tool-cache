// Package gocacheproxy is an HTTP reverse proxy that routes cache requests
// to backend gocached servers using rendezvous (HRW) hashing on actionID.
package gocacheproxy

import (
	"bytes"
	"cmp"
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

	"github.com/GetStream/go-tool-cache/wire"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	healthPath           = "/health"
	defaultReplication   = 2
	defaultHealthEvery   = 2 * time.Second
	healthProbeTimeout   = 5 * time.Second
	unhealthyAfterFails  = 2
	defaultClientTimeout = 30 * time.Second
)

// Backend is a single gocached server.
type Backend struct {
	URL     string
	healthy atomic.Bool
	fails   atomic.Int32
}

// Proxy routes cache requests to backends using rendezvous hashing.
//
// The proxy is best-effort: GET returns 200 (hit) or 404 (miss/error);
// PUT always returns 204 even if the upload ultimately failed, because a
// cache failure should never break the build — the compiler will just
// re-cache next time. GET never returns 5xx to the caller.
type Proxy struct {
	Backends []*Backend
	Client   *http.Client
	// Retries is the number of retries after the initial attempt on PUT
	// to a given backend. Total attempts per backend = 1 + Retries.
	Retries int
	// Replication is the number of healthy HRW candidates to PUT in
	// parallel. Zero means 2.
	Replication int
	// MaxInflightBytes is the soft cap on total buffered PUT body bytes.
	// A PUT whose Content-Length would push us over is dropped silently
	// (logged and counted). 0 disables the check.
	MaxInflightBytes int64
	// HealthInterval is the backend probe period. Zero means 2s.
	HealthInterval time.Duration
	Verbose        bool

	inflightBytes atomic.Int64

	initOnce        sync.Once
	metricsHandler  http.Handler
	getTotal        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	backendUp       *prometheus.GaugeVec
	inflight        prometheus.Gauge
	putDropped      *prometheus.CounterVec
	putOK           *prometheus.CounterVec
	putErr          *prometheus.CounterVec
	bytesIn         *prometheus.CounterVec
	bytesOut        *prometheus.CounterVec
	backendBytesIn  prometheus.Counter
	backendBytesOut prometheus.Counter
}

func (p *Proxy) ensure() {
	p.initOnce.Do(p.init)
}

func (p *Proxy) init() {
	if p.Client == nil {
		p.Client = &http.Client{Timeout: defaultClientTimeout}
	} else if p.Client.Timeout == 0 {
		p.Client.Timeout = defaultClientTimeout
	}
	if p.Client.Transport == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.MaxIdleConnsPerHost = 64
		tr.MaxConnsPerHost = 64
		tr.DisableCompression = true
		p.Client.Transport = tr
	}
	p.initMetrics()
}

func (p *Proxy) replication() int {
	return cmp.Or(p.Replication, defaultReplication)
}

func healthyBackends(cands []*Backend) []*Backend {
	out := make([]*Backend, 0, len(cands))
	for _, b := range cands {
		if b.healthy.Load() {
			out = append(out, b)
		}
	}
	return out
}

func (p *Proxy) actionKind(r *http.Request) string {
	return wire.SanitizeActionKind(r.Header.Get(wire.ActionKindHeader))
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.ensure()
	switch {
	case r.URL.Path == healthPath:
		p.handleHealth(w, r)
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/action/"):
		actionID := strings.TrimPrefix(r.URL.Path, "/action/")
		p.proxyGet(w, r, actionID)
	case r.Method == "PUT":
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

// proxyGet walks the first R healthy HRW candidates — the same set
// proxyPut writes to. Transport errors and 5xx fail over within that
// set; a backend 404 is a replica miss. The client never sees 5xx.
func (p *Proxy) proxyGet(w http.ResponseWriter, r *http.Request, actionID string) {
	kind := p.actionKind(r)
	cands, err := p.Candidates(actionID)
	if err != nil {
		p.getTotal.WithLabelValues("error", kind).Inc()
		log.Printf("GET %s: %v", r.URL.Path, err)
		http.NotFound(w, r)
		return
	}
	healthy := healthyBackends(cands)
	if len(healthy) == 0 {
		p.getTotal.WithLabelValues("no_healthy_backend", kind).Inc()
		log.Printf("GET %s: no healthy backends", r.URL.Path)
		http.NotFound(w, r)
		return
	}

	n := min(p.replication(), len(healthy))
	targets := healthy[:n]

	sawMiss := false
	sawErr := false
	for _, b := range targets {
		start := time.Now()
		resp, err := p.forward(r, b, r.URL.RequestURI(), nil, 0)
		p.duration.WithLabelValues(r.Method, b.URL, kind).Observe(time.Since(start).Seconds())
		if err != nil {
			sawErr = true
			log.Printf("GET %s backend %s error: %v", r.URL.Path, b.URL, err)
			continue
		}
		n := resp.ContentLength
		switch resp.StatusCode {
		case http.StatusOK:
			p.getTotal.WithLabelValues("hit", kind).Inc()
			written := copyResponse(w, resp)
			resp.Body.Close()
			if n < 0 {
				n = written
			}
			p.backendBytesOut.Add(float64(n))
			p.bytesOut.WithLabelValues(kind).Add(float64(written))
			return
		case http.StatusNotFound:
			sawMiss = true
			if n > 0 {
				p.backendBytesOut.Add(float64(n))
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		default:
			sawErr = true
			log.Printf("GET %s backend %s unexpected status %d", r.URL.Path, b.URL, resp.StatusCode)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
	if sawMiss || !sawErr {
		p.getTotal.WithLabelValues("miss", kind).Inc()
	} else {
		p.getTotal.WithLabelValues("error", kind).Inc()
	}
	http.NotFound(w, r)
}

// proxyPut replicates to the first R healthy HRW candidates. Known-length
// bodies are buffered so they can be retried; chunked bodies are streamed
// with no retries. The caller always receives 204.
func (p *Proxy) proxyPut(w http.ResponseWriter, r *http.Request, actionID string) {
	defer func() { w.WriteHeader(http.StatusNoContent) }()
	kind := p.actionKind(r)

	cands, err := p.Candidates(actionID)
	if err != nil {
		p.putErr.WithLabelValues(kind).Inc()
		log.Printf("PUT %s: %v", r.URL.Path, err)
		io.Copy(io.Discard, r.Body)
		return
	}
	healthy := healthyBackends(cands)
	if len(healthy) == 0 {
		p.putErr.WithLabelValues(kind).Inc()
		log.Printf("PUT %s: no healthy backends", r.URL.Path)
		io.Copy(io.Discard, r.Body)
		return
	}

	if r.ContentLength < 0 {
		p.streamPut(r, healthy, kind)
		return
	}

	if !p.reserveInflight(r.ContentLength) {
		p.putDropped.WithLabelValues(kind).Inc()
		log.Printf("PUT %s dropped: inflight %d+%d > %d (bytes)",
			r.URL.Path, p.inflightBytes.Load(), r.ContentLength, p.MaxInflightBytes)
		io.Copy(io.Discard, r.Body)
		return
	}
	defer p.releaseInflight(r.ContentLength)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.putErr.WithLabelValues(kind).Inc()
		log.Printf("PUT %s read body error: %v", r.URL.Path, err)
		return
	}
	p.bytesIn.WithLabelValues(kind).Add(float64(len(body)))

	n := min(p.replication(), len(healthy))
	targets := healthy[:n]
	if p.putBuffered(r, targets, body, kind) {
		p.putOK.WithLabelValues(kind).Inc()
		return
	}
	p.putErr.WithLabelValues(kind).Inc()
	log.Printf("PUT %s failed on all %d replica targets (%d bytes)", r.URL.Path, n, len(body))
}

func (p *Proxy) putBuffered(orig *http.Request, targets []*Backend, body []byte, kind string) bool {
	var ok atomic.Bool
	var wg sync.WaitGroup
	for _, b := range targets {
		wg.Go(func() {
			if p.putOneBuffered(orig, b, body, kind) {
				ok.Store(true)
			}
		})
	}
	wg.Wait()
	return ok.Load()
}

func (p *Proxy) putOneBuffered(orig *http.Request, b *Backend, body []byte, kind string) bool {
	attempts := 1 + p.Retries
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		start := time.Now()
		resp, err := p.forward(orig, b, orig.URL.RequestURI(), bytes.NewReader(body), int64(len(body)))
		p.duration.WithLabelValues(orig.Method, b.URL, kind).Observe(time.Since(start).Seconds())
		p.backendBytesIn.Add(float64(len(body)))
		if err == nil {
			if resp.ContentLength > 0 {
				p.backendBytesOut.Add(float64(resp.ContentLength))
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if attempt > 1 {
				log.Printf("PUT %s backend %s ok on attempt %d/%d (%d bytes)",
					orig.URL.Path, b.URL, attempt, attempts, len(body))
			}
			return true
		}
		lastErr = err
		log.Printf("PUT %s backend %s attempt %d/%d error (%d bytes): %v",
			orig.URL.Path, b.URL, attempt, attempts, len(body), err)
		if attempt < attempts {
			time.Sleep(time.Duration(attempt) * 50 * time.Millisecond)
		}
	}
	log.Printf("PUT %s backend %s failed after %d attempts (%d bytes); last error: %v",
		orig.URL.Path, b.URL, attempts, len(body), lastErr)
	return false
}

func (p *Proxy) streamPut(orig *http.Request, healthy []*Backend, kind string) {
	n := min(p.replication(), len(healthy))
	targets := healthy[:n]
	writers := make([]io.Writer, n)
	readers := make([]*io.PipeReader, n)
	for i := range n {
		pr, pw := io.Pipe()
		readers[i] = pr
		writers[i] = pw
	}

	var copied atomic.Int64
	go func() {
		nw, err := io.Copy(io.MultiWriter(writers...), orig.Body)
		copied.Store(nw)
		for _, w := range writers {
			w.(*io.PipeWriter).CloseWithError(err)
		}
	}()

	var ok atomic.Bool
	var wg sync.WaitGroup
	for i, b := range targets {
		wg.Go(func() {
			start := time.Now()
			resp, err := p.forward(orig, b, orig.URL.RequestURI(), readers[i], -1)
			p.duration.WithLabelValues(orig.Method, b.URL, kind).Observe(time.Since(start).Seconds())
			if err != nil {
				log.Printf("PUT %s streamed backend %s error: %v", orig.URL.Path, b.URL, err)
				return
			}
			if resp.ContentLength > 0 {
				p.backendBytesOut.Add(float64(resp.ContentLength))
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ok.Store(true)
		})
	}
	wg.Wait()
	p.bytesIn.WithLabelValues(kind).Add(float64(copied.Load()))
	p.backendBytesIn.Add(float64(copied.Load()) * float64(n))
	if ok.Load() {
		p.putOK.WithLabelValues(kind).Inc()
		return
	}
	p.putErr.WithLabelValues(kind).Inc()
	log.Printf("PUT %s streamed put failed on all %d targets", orig.URL.Path, n)
}

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
			p.inflight.Set(float64(cur + n))
			return true
		}
	}
}

func (p *Proxy) releaseInflight(n int64) {
	if p.MaxInflightBytes <= 0 {
		return
	}
	v := p.inflightBytes.Add(-n)
	p.inflight.Set(float64(v))
}

// DetectMemoryLimit reads the current container's memory limit from the
// cgroup filesystem. Returns the limit in bytes on success, or an error if
// the container has no limit configured or the cgroup info is unreadable.
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
		if v >= 1<<60 {
			return 0, errors.New("cgroup v1: no memory limit set")
		}
		return v, nil
	}
	return 0, errors.New("no cgroup memory info found")
}

func (p *Proxy) forward(orig *http.Request, b *Backend, path string, body io.Reader, contentLength int64) (*http.Response, error) {
	url := b.URL + path
	req, err := http.NewRequestWithContext(orig.Context(), orig.Method, url, body)
	if err != nil {
		return nil, err
	}
	if body != nil && contentLength >= 0 {
		req.ContentLength = contentLength
	}
	for _, h := range []string{"Authorization", "Want-Object", "Accept-Encoding", "Content-Type", wire.ActionKindHeader} {
		if v := orig.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}

	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 500 {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("backend status %d", resp.StatusCode)
	}
	return resp, nil
}

func copyResponse(w http.ResponseWriter, resp *http.Response) int64 {
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	n, _ := io.Copy(w, resp.Body)
	return n
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

// HealthCheck pings each backend on HealthInterval. Blocks forever.
func (p *Proxy) HealthCheck() {
	p.ensure()
	interval := p.HealthInterval
	if interval <= 0 {
		interval = defaultHealthEvery
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	check := func() {
		var wg sync.WaitGroup
		for _, b := range p.Backends {
			wg.Go(func() {
				p.probe(b)
			})
		}
		wg.Wait()
	}

	check()
	for range ticker.C {
		check()
	}
}

func (p *Proxy) probe(b *Backend) {
	client := &http.Client{Timeout: healthProbeTimeout}
	resp, err := client.Get(b.URL + healthPath)
	ok := err == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300
	if resp != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if ok {
		b.fails.Store(0)
		was := b.healthy.Swap(true)
		p.backendUp.WithLabelValues(b.URL).Set(1)
		if !was {
			log.Printf("backend %s health: false -> true", b.URL)
		}
		return
	}
	n := b.fails.Add(1)
	if n >= unhealthyAfterFails {
		was := b.healthy.Swap(false)
		p.backendUp.WithLabelValues(b.URL).Set(0)
		if was {
			log.Printf("backend %s health: true -> false", b.URL)
		}
	}
}
