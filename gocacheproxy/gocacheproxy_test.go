package gocacheproxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/GetStream/go-tool-cache/wire"
)

func testActionID() string {
	return "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
}

func newTestProxy(t *testing.T, backends []*Backend) *Proxy {
	t.Helper()
	p := &Proxy{
		Backends: backends,
		Client:   &http.Client{Timeout: 2 * time.Second},
		Retries:  2,
	}
	p.ensure()
	for _, b := range backends {
		b.healthy.Store(true)
	}
	return p
}

func backendsFromServers(svcs []*httptest.Server) []*Backend {
	out := make([]*Backend, len(svcs))
	for i, s := range svcs {
		out[i] = &Backend{URL: s.URL}
	}
	return out
}

func counterVal(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.Counter != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

func TestHRWDeterministicAndOrdered(t *testing.T) {
	p := &Proxy{Backends: []*Backend{
		{URL: "http://b0"},
		{URL: "http://b1"},
		{URL: "http://b2"},
	}}
	id := testActionID()
	a, err := p.Candidates(id)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Candidates(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 3 || a[0].URL != b[0].URL || a[1].URL != b[1].URL || a[2].URL != b[2].URL {
		t.Fatalf("HRW order not deterministic: %v vs %v", urls(a), urls(b))
	}
	if _, err := p.Candidates("xyz"); err == nil {
		t.Fatal("expected error for malformed actionID")
	}
	if _, err := p.Candidates("abc"); err == nil { // odd length
		t.Fatal("expected error for odd-length actionID")
	}
}

func urls(cands []*Backend) []string {
	s := make([]string, len(cands))
	for i, b := range cands {
		s[i] = b.URL
	}
	return s
}

func TestHRWResizeRemapFraction(t *testing.T) {
	const n0, n1, samples = 20, 25, 5000
	old := make([]*Backend, n0)
	newer := make([]*Backend, n1)
	for i := range n1 {
		b := &Backend{URL: fmt.Sprintf("http://b%d", i)}
		if i < n0 {
			old[i] = b
		}
		newer[i] = b
	}
	p0 := &Proxy{Backends: old}
	p1 := &Proxy{Backends: newer}
	changed := 0
	for i := range samples {
		id := fmt.Sprintf("%064x", i)
		c0, err := p0.Candidates(id)
		if err != nil {
			t.Fatal(err)
		}
		c1, err := p1.Candidates(id)
		if err != nil {
			t.Fatal(err)
		}
		if c0[0].URL != c1[0].URL {
			changed++
		}
	}
	frac := float64(changed) / float64(samples)
	// HRW remaps k/(N+k) = 5/25 = 0.20. Modulo would remap ~96%.
	if frac < 0.10 || frac > 0.30 {
		t.Fatalf("HRW remap fraction 20→25 = %.3f, want ~0.20", frac)
	}
}

func TestMalformedActionIDGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("backend should not be called")
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/zzzz", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := counterVal(t, p.getTotal.WithLabelValues("error", "")); got != 1 {
		t.Fatalf("get_total error = %v, want 1", got)
	}
}

func TestProxyGetRouting(t *testing.T) {
	const n = 3
	var hits [n]atomic.Int32
	svcs := make([]*httptest.Server, n)
	for i := range n {
		i := i
		svcs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"outputID":"beef0001","size":4}`))
		}))
		defer svcs[i].Close()
	}
	p := newTestProxy(t, backendsFromServers(svcs))
	id := testActionID()
	cands, err := p.Candidates(id)
	if err != nil {
		t.Fatal(err)
	}
	primary := cands[0].URL

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+id, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var primaryHits, others int32
	for i, s := range svcs {
		if s.URL == primary {
			primaryHits = hits[i].Load()
		} else {
			others += hits[i].Load()
		}
	}
	if primaryHits != 1 || others != 0 {
		t.Fatalf("primary hits=%d others=%d, want 1 and 0", primaryHits, others)
	}
}

func TestProxyGetFailoverOnDown(t *testing.T) {
	id := testActionID()
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"outputID":"cafe0001","size":4}`))
	}))
	defer alive.Close()

	p := &Proxy{Client: &http.Client{Timeout: 500 * time.Millisecond}}
	p.Backends = []*Backend{
		{URL: "http://127.0.0.1:1"},
		{URL: alive.URL},
	}
	p.ensure()
	for _, b := range p.Backends {
		b.healthy.Store(true)
	}
	// With R=2 and two healthy backends, GET tries both regardless of HRW order.
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+id, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET with one backend down status = %d, want 200", rec.Code)
	}
}

func TestProxyGet5xxConvertedTo404AfterFailover(t *testing.T) {
	id := testActionID()
	svcs := make([]*httptest.Server, 2)
	svcs[0] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer svcs[0].Close()
	svcs[1] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer svcs[1].Close()
	p := newTestProxy(t, backendsFromServers(svcs))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+id, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (never 5xx)", rec.Code)
	}
}

func TestProxyGetSkipUnhealthy(t *testing.T) {
	var called atomic.Int32
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		w.Write([]byte("should-not-run"))
	}))
	defer dead.Close()
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer live.Close()
	p := newTestProxy(t, []*Backend{{URL: dead.URL}, {URL: live.URL}})
	p.Backends[0].healthy.Store(false)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+testActionID(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if called.Load() != 0 {
		t.Fatal("unhealthy backend was called")
	}
}

func TestProxyGetNoHealthy(t *testing.T) {
	p := newTestProxy(t, []*Backend{{URL: "http://localhost:1"}})
	p.Backends[0].healthy.Store(false)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+testActionID(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := counterVal(t, p.getTotal.WithLabelValues("no_healthy_backend", "")); got != 1 {
		t.Fatalf("no_healthy_backend = %v, want 1", got)
	}
}

func TestProxyGetCapsFanOutToReplication(t *testing.T) {
	const nBackends = 6
	var hits atomic.Int32
	svcs := make([]*httptest.Server, nBackends)
	for i := range nBackends {
		svcs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			http.NotFound(w, r)
		}))
		defer svcs[i].Close()
	}
	p := newTestProxy(t, backendsFromServers(svcs))
	p.Replication = 2
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+testActionID(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := hits.Load(); got != int32(p.Replication) {
		t.Fatalf("backend GETs = %d, want %d (replication factor)", got, p.Replication)
	}
	if got := counterVal(t, p.getTotal.WithLabelValues("miss", "")); got != 1 {
		t.Fatalf("get_total miss = %v, want 1", got)
	}
}

func TestProxyGetHitsSecondReplica(t *testing.T) {
	id := testActionID()
	const n = 3
	var hits [n]atomic.Int32
	svcs := make([]*httptest.Server, n)
	p := &Proxy{Client: &http.Client{Timeout: 2 * time.Second}, Retries: 2}
	for i := range n {
		i := i
		svcs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			cands, err := p.Candidates(id)
			if err != nil {
				t.Error(err)
				http.NotFound(w, r)
				return
			}
			switch svcs[i].URL {
			case cands[0].URL:
				http.NotFound(w, r)
			case cands[1].URL:
				w.Write([]byte(`{"outputID":"cafe0001","size":4}`))
			default:
				t.Errorf("GET probed backend %s beyond replication", svcs[i].URL)
				http.NotFound(w, r)
			}
		}))
		defer svcs[i].Close()
	}
	p.Backends = backendsFromServers(svcs)
	p.ensure()
	for _, b := range p.Backends {
		b.healthy.Store(true)
	}

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest("GET", "/action/"+id, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	cands, err := p.Candidates(id)
	if err != nil {
		t.Fatal(err)
	}
	var first, second, extra int32
	for i, s := range svcs {
		switch s.URL {
		case cands[0].URL:
			first = hits[i].Load()
		case cands[1].URL:
			second = hits[i].Load()
		default:
			extra += hits[i].Load()
		}
	}
	if first != 1 || second != 1 || extra != 0 {
		t.Fatalf("first=%d second=%d extra=%d, want 1, 1, 0", first, second, extra)
	}
}

func TestProxyPut(t *testing.T) {
	var gotBody, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", rec.Code)
	}
	if gotPath != "/abcdef01/beef0001" {
		t.Fatalf("backend path %q", gotPath)
	}
	if gotBody != "hello" {
		t.Fatalf("body %q", gotBody)
	}
}

func TestProxyPutReplication(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			got[r.Host] = string(b)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
	}
	s1, s2, s3 := mk(), mk(), mk()
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()
	p := newTestProxy(t, backendsFromServers([]*httptest.Server{s1, s2, s3}))
	req := httptest.NewRequest("PUT", "/"+testActionID()+"/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("replicas = %d, want 2 (got %#v)", len(got), got)
	}
	for _, v := range got {
		if v != "hello" {
			t.Fatalf("body %q", v)
		}
	}
}

func TestProxyPutChunkedTee(t *testing.T) {
	var mu sync.Mutex
	got := []string{}
	mk := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			got = append(got, string(b))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
	}
	s1, s2 := mk(), mk()
	defer s1.Close()
	defer s2.Close()
	p := newTestProxy(t, backendsFromServers([]*httptest.Server{s1, s2}))
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("streamed to %d backends, want 2", len(got))
	}
	for _, v := range got {
		if v != "hello" {
			t.Fatalf("body %q", v)
		}
	}
}

func TestProxyHealth(t *testing.T) {
	p := newTestProxy(t, []*Backend{
		{URL: "http://localhost:1"},
		{URL: "http://localhost:2"},
	})
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

func TestHealthCheckRequires2xxAndThreshold(t *testing.T) {
	var status atomic.Int32
	status.Store(500)
	var sawPath atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawPath.Store(r.URL.Path)
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	b := &Backend{URL: srv.URL}
	b.healthy.Store(true)
	p := &Proxy{Backends: []*Backend{b}, HealthInterval: time.Hour}
	p.ensure()

	p.probe(b)
	if !b.healthy.Load() {
		t.Fatal("one 500 should not mark unhealthy")
	}
	p.probe(b)
	if b.healthy.Load() {
		t.Fatal("two 500s should mark unhealthy")
	}
	if sawPath.Load() != "/health" {
		t.Fatalf("probed %v, want /health", sawPath.Load())
	}

	status.Store(200)
	p.probe(b)
	if !b.healthy.Load() {
		t.Fatal("2xx should mark healthy")
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
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	req := httptest.NewRequest("GET", "/action/abcdef01", nil)
	req.Header.Set("Want-Object", "1")
	req.Header.Set("Accept-Encoding", "lz4")
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set(wire.ActionKindHeader, "compile")
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
	if gotHeaders.Get(wire.ActionKindHeader) != "compile" {
		t.Error("Go-Action-Kind header not forwarded")
	}
	if rec.Header().Get("Go-Output-Id") != "cafe0001" {
		t.Error("Go-Output-Id response header not passed through")
	}
}

func TestProxyPutRetrySucceeds(t *testing.T) {
	var attempts atomic.Int32
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
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
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
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

func TestProxyPutRetry5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
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
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status after exhausted retries = %d, want 204", rec.Code)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	if got := counterVal(t, p.putErr.WithLabelValues("")); got != 1 {
		t.Fatalf("putErr = %v, want 1", got)
	}
}

func TestProxyPutDropped(t *testing.T) {
	var backendCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	p.MaxInflightBytes = 1
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
	req.ContentLength = 5
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("dropped PUT status = %d, want 204", rec.Code)
	}
	if got := counterVal(t, p.putDropped.WithLabelValues("")); got != 1 {
		t.Fatalf("putDropped = %v, want 1", got)
	}
	if got := backendCalls.Load(); got != 0 {
		t.Fatalf("backend was called %d times", got)
	}
}

func TestProxyInflightReleasesOnSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	p.MaxInflightBytes = 1024
	for i := range 10 {
		req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("hello"))
		req.ContentLength = 5
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		if got := p.inflightBytes.Load(); got != 0 {
			t.Fatalf("iter %d: inflightBytes = %d after PUT, want 0", i, got)
		}
	}
	if got := counterVal(t, p.putOK.WithLabelValues("")); got != 10 {
		t.Fatalf("putOK = %v, want 10", got)
	}
}

func TestProxyStatsAndMetricsEndpoint(t *testing.T) {
	var nextStatus atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch nextStatus.Load() {
		case 200:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"outputID":"a","size":1}`))
		case 404:
			http.NotFound(w, r)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})
	for _, status := range []int32{200, 404} {
		nextStatus.Store(status)
		p.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/action/abcdef01", nil))
	}
	nextStatus.Store(204)
	req := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("x"))
	req.ContentLength = 1
	p.ServeHTTP(httptest.NewRecorder(), req)

	if got := counterVal(t, p.getTotal.WithLabelValues("hit", "")); got != 1 {
		t.Errorf("hit = %v, want 1", got)
	}
	if got := counterVal(t, p.getTotal.WithLabelValues("miss", "")); got != 1 {
		t.Errorf("miss = %v, want 1", got)
	}
	if got := counterVal(t, p.putOK.WithLabelValues("")); got != 1 {
		t.Errorf("putOK = %v, want 1", got)
	}

	rec := httptest.NewRecorder()
	p.ServeHTTPDebug(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`gocacheproxy_get_total{action_kind="",result="hit"}`,
		`gocacheproxy_get_total{action_kind="",result="miss"}`,
		"gocacheproxy_request_duration_seconds",
		"gocacheproxy_backend_up",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestProxyActionKindMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Go-Output-Id", "cafe0001")
		w.Write([]byte("data"))
	}))
	defer srv.Close()
	p := newTestProxy(t, []*Backend{{URL: srv.URL}})

	get := httptest.NewRequest("GET", "/action/abcdef01", nil)
	get.Header.Set(wire.ActionKindHeader, "compile")
	p.ServeHTTP(httptest.NewRecorder(), get)

	put := httptest.NewRequest("PUT", "/abcdef01/beef0001", strings.NewReader("x"))
	put.ContentLength = 1
	put.Header.Set(wire.ActionKindHeader, "test")
	p.ServeHTTP(httptest.NewRecorder(), put)

	if got := counterVal(t, p.getTotal.WithLabelValues("hit", "compile")); got != 1 {
		t.Errorf("hit compile = %v, want 1", got)
	}
	if got := counterVal(t, p.putOK.WithLabelValues("test")); got != 1 {
		t.Errorf("putOK test = %v, want 1", got)
	}

	bad := httptest.NewRequest("GET", "/action/abcdef01", nil)
	bad.Header.Set(wire.ActionKindHeader, "build fmt")
	p.ServeHTTP(httptest.NewRecorder(), bad)
	if got := counterVal(t, p.getTotal.WithLabelValues("hit", "")); got != 1 {
		t.Errorf("invalid ActionKind should count as empty, got %v", got)
	}
}
