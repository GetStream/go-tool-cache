package main

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
	// Same tests as cachers/multi_http_test.go to ensure identical behavior.
	if got := serverIndex("abcdef01", 3); got != serverIndex("abcdef01", 3) {
		t.Fatal("serverIndex not deterministic")
	}
	if got := serverIndex("abcdef01", 1); got != 0 {
		t.Fatalf("serverIndex with 1 backend = %d, want 0", got)
	}

	// Distribution test: hash 1000 IDs across 3 backends, each should get some.
	counts := [3]int{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("%08x", i)
		counts[serverIndex(id, 3)]++
	}
	for i, c := range counts {
		if c == 0 {
			t.Errorf("backend %d got 0 requests", i)
		}
	}
}

func TestProxyGetRouting(t *testing.T) {
	// Create 3 fake backends. Only backend that matches the hash has the data.
	backends := make([]*httptest.Server, 3)
	for i := range backends {
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actionID := strings.TrimPrefix(r.URL.Path, "/action/")
			primary := serverIndex(actionID, 3)
			if primary == i {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"outputID":"beef0001","size":4}`))
			} else {
				http.NotFound(w, r)
			}
		}))
		defer backends[i].Close()
	}

	p := &proxy{
		backends: make([]*backend, 3),
		client:   &http.Client{Timeout: 5 * time.Second},
	}
	for i, s := range backends {
		p.backends[i] = &backend{url: s.URL}
		p.backends[i].healthy.Store(true)
	}

	// Test that any actionID gets routed to the correct backend.
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
	// Primary backend is down, fallback should find the data.
	called := [3]int{}
	backends := make([]*httptest.Server, 3)
	for i := range backends {
		backends[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called[i]++
			// All backends return data if asked.
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"outputID":"cafe0001","size":4}`))
		}))
		defer backends[i].Close()
	}

	actionID := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	primary := serverIndex(actionID, 3)

	// Close the primary backend to force fallback.
	backends[primary].Close()

	p := &proxy{
		backends: make([]*backend, 3),
		client:   &http.Client{Timeout: 2 * time.Second},
	}
	for i, s := range backends {
		p.backends[i] = &backend{url: s.URL}
		p.backends[i].healthy.Store(true)
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

	p := &proxy{
		backends: []*backend{{url: srv.URL}},
		client:   &http.Client{Timeout: 5 * time.Second},
	}
	p.backends[0].healthy.Store(true)

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
	p := &proxy{
		backends: []*backend{
			{url: "http://localhost:1"},
			{url: "http://localhost:2"},
		},
		client: &http.Client{},
	}

	// All healthy.
	p.backends[0].healthy.Store(true)
	p.backends[1].healthy.Store(true)
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rec.Code)
	}

	// All unhealthy.
	p.backends[0].healthy.Store(false)
	p.backends[1].healthy.Store(false)
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

	p := &proxy{
		backends: []*backend{{url: srv.URL}},
		client:   &http.Client{Timeout: 5 * time.Second},
	}
	p.backends[0].healthy.Store(true)

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

	// Check response headers passed back.
	if rec.Header().Get("Go-Output-Id") != "cafe0001" {
		t.Error("Go-Output-Id response header not passed through")
	}
}

func TestAllBackendsFail(t *testing.T) {
	p := &proxy{
		backends: []*backend{
			{url: "http://localhost:1"},
			{url: "http://localhost:2"},
		},
		client: &http.Client{Timeout: 1 * time.Second},
	}
	p.backends[0].healthy.Store(true)
	p.backends[1].healthy.Store(true)

	req := httptest.NewRequest("GET", "/action/abcdef01", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
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

	p := &proxy{
		backends: []*backend{
			{url: "http://localhost:1"}, // unhealthy
			{url: srv.URL},             // healthy
		},
		client: &http.Client{Timeout: 2 * time.Second},
	}
	p.backends[0].healthy.Store(false)
	p.backends[1].healthy.Store(true)

	// Use actionID that hashes to backend 0 (unhealthy).
	// The proxy should skip it and try backend 1.
	// We need to find an actionID whose primary is 0.
	var actionID string
	for i := 0; i < 256; i++ {
		candidate := strings.Repeat("0", 6) + string([]byte{
			"0123456789abcdef"[i/16],
			"0123456789abcdef"[i%16],
		})
		if serverIndex(candidate, 2) == 0 {
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
