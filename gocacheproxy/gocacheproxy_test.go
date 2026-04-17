package gocacheproxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestServerIndex(t *testing.T) {
	if got := ServerIndex("abcdef01", 3); got != ServerIndex("abcdef01", 3) {
		t.Fatal("ServerIndex not deterministic")
	}
	if got := ServerIndex("abcdef01", 1); got != 0 {
		t.Fatalf("ServerIndex with 1 backend = %d, want 0", got)
	}

	// Distribution test: hash 1000 IDs across 3 backends, each should get some.
	counts := [3]int{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%08x", i)
		counts[ServerIndex(id, 3)]++
	}
	for i, c := range counts {
		if c == 0 {
			t.Errorf("backend %d got 0 requests", i)
		}
	}
}

func TestProxyGetRouting(t *testing.T) {
	backends := make([]*httptest.Server, 3)
	for i := range backends {
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actionID := strings.TrimPrefix(r.URL.Path, "/action/")
			primary := ServerIndex(actionID, 3)
			if primary == i {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"outputID":"beef0001","size":4}`))
			} else {
				http.NotFound(w, r)
			}
		}))
		defer backends[i].Close()
	}

	p := &Proxy{
		Backends: make([]*Backend, 3),
		Client:   &http.Client{Timeout: 5 * time.Second},
	}
	for i, s := range backends {
		p.Backends[i] = &Backend{URL: s.URL}
		p.Backends[i].healthy.Store(true)
	}

	actionID := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	req := httptest.NewRequest("GET", "/action/"+actionID, nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /action status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "beef0001") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestProxyGetFallback(t *testing.T) {
	called := [3]int{}
	backends := make([]*httptest.Server, 3)
	for i := range backends {
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called[i]++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"outputID":"cafe0001","size":4}`))
		}))
		defer backends[i].Close()
	}

	actionID := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	primary := ServerIndex(actionID, 3)

	// Close the primary backend to force fallback.
	backends[primary].Close()

	p := &Proxy{
		Backends: make([]*Backend, 3),
		Client:   &http.Client{Timeout: 2 * time.Second},
	}
	for i, s := range backends {
		p.Backends[i] = &Backend{URL: s.URL}
		p.Backends[i].healthy.Store(true)
	}

	req := httptest.NewRequest("GET", "/action/"+actionID, nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET with fallback status = %d, want 200", rec.Code)
	}
}

func TestProxyPut(t *testing.T) {
	var gotBody string
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := &Proxy{
		Backends: []*Backend{{URL: srv.URL}},
		Client:   &http.Client{Timeout: 5 * time.Second},
	}
	p.Backends[0].healthy.Store(true)

	body := strings.NewReader("hello")
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", body)
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", rec.Code)
	}
	if gotPath != "/abcdef01/beef0001" {
		t.Fatalf("backend got path %q, want /abcdef01/beef0001", gotPath)
	}
	if gotBody != "hello" {
		t.Fatalf("backend got body %q, want hello", gotBody)
	}
}

func TestProxyHealth(t *testing.T) {
	p := &Proxy{
		Backends: []*Backend{
			{URL: "http://localhost:1"},
			{URL: "http://localhost:2"},
		},
		Client: &http.Client{},
	}

	p.Backends[0].healthy.Store(true)
	p.Backends[1].healthy.Store(true)
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rec.Code)
	}

	p.Backends[0].healthy.Store(false)
	p.Backends[1].healthy.Store(false)
	rec = httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, want 503", rec.Code)
	}
}

func TestProxyHeaderPassthrough(t *testing.T) {
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Go-Output-Id", "cafe0001")
		w.Write([]byte("data"))
	}))
	defer srv.Close()

	p := &Proxy{
		Backends: []*Backend{{URL: srv.URL}},
		Client:   &http.Client{Timeout: 5 * time.Second},
	}
	p.Backends[0].healthy.Store(true)

	req := httptest.NewRequest("GET", "/action/abcdef01", nil)
	req.Header.Set("Want-Object", "1")
	req.Header.Set("Accept-Encoding", "lz4")
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotHeaders.Get("Want-Object") != "1" {
		t.Error("Want-Object header not forwarded")
	}
	if gotHeaders.Get("Accept-Encoding") != "lz4" {
		t.Error("Accept-Encoding header not forwarded")
	}
	if gotHeaders.Get("Authorization") != "Bearer test-token" {
		t.Error("Authorization header not forwarded")
	}
	if rec.Header().Get("Go-Output-Id") != "cafe0001" {
		t.Error("Go-Output-Id response header not passed through")
	}
}

func TestAllBackendsFail(t *testing.T) {
	p := &Proxy{
		Backends: []*Backend{
			{URL: "http://localhost:1"},
			{URL: "http://localhost:2"},
		},
		Client: &http.Client{Timeout: 1 * time.Second},
	}
	p.Backends[0].healthy.Store(true)
	p.Backends[1].healthy.Store(true)

	req := httptest.NewRequest("GET", "/action/abcdef01", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestProxyPutRetrySucceeds(t *testing.T) {
	var attempts atomic.Int32
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			// Simulate backend stall; hijack to drop the connection so the
			// proxy's http.Client returns an error rather than an HTTP status.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("hijacker not supported")
				return
			}
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := &Proxy{
		Backends: []*Backend{{URL: srv.URL}},
		Client:   &http.Client{Timeout: 2 * time.Second},
		Retries:  2,
	}
	p.Backends[0].healthy.Store(true)

	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT after retry status = %d, want 204", rec.Code)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if gotBody != "hello" {
		t.Fatalf("body after retry = %q, want hello", gotBody)
	}
}

func TestProxyPutRetryExhausted(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("hijacker not supported")
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	p := &Proxy{
		Backends: []*Backend{{URL: srv.URL}},
		Client:   &http.Client{Timeout: 2 * time.Second},
		Retries:  2,
	}
	p.Backends[0].healthy.Store(true)

	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("PUT status after exhausted retries = %d, want 502", rec.Code)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (1 + 2 retries)", got)
	}
}

func TestUnhealthyBackendSkipped(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"outputID":"beef0001","size":4}`))
	}))
	defer srv.Close()

	p := &Proxy{
		Backends: []*Backend{
			{URL: "http://localhost:1"}, // unhealthy
			{URL: srv.URL},             // healthy
		},
		Client: &http.Client{Timeout: 2 * time.Second},
	}
	p.Backends[0].healthy.Store(false)
	p.Backends[1].healthy.Store(true)

	var actionID string
	for i := 0; i < 256; i++ {
		candidate := strings.Repeat("0", 6) + string([]byte{
			"0123456789abcdef"[i/16],
			"0123456789abcdef"[i%16],
		})
		if ServerIndex(candidate, 2) == 0 {
			actionID = candidate
			break
		}
	}
	if actionID == "" {
		t.Skip("couldn't find actionID that hashes to backend 0")
	}

	req := httptest.NewRequest("GET", "/action/"+actionID, nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
