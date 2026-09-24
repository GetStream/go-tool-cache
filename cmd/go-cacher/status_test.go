package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GetStream/go-tool-cache/cacheproc"
	"github.com/GetStream/go-tool-cache/cachers"
	"github.com/GetStream/go-tool-cache/gocacheproxy"
)

func TestStatusReporter(t *testing.T) {
	statusCh := make(chan gocacheproxy.ClientStatus, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != gocacheproxy.ClientStatusPath {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var status gocacheproxy.ClientStatus
		if err := json.NewDecoder(r.Body).Decode(&status); err != nil {
			t.Errorf("decode status: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		statusCh <- status
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cpuStat := filepath.Join(t.TempDir(), "cpu.stat")
	if err := os.WriteFile(cpuStat, []byte("usage_usec 1250000\nuser_usec 1000000\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	process := &cacheproc.Process{}
	progress := time.Unix(1_700_000_000, 500_000_000)
	process.LastProgressUnixNano.Store(progress.UnixNano())
	reporter := statusReporter{
		URL:         server.URL + gocacheproxy.ClientStatusPath,
		ClientID:    "runner-abc123",
		Disk:        &cachers.DiskCache{},
		Process:     process,
		Client:      server.Client(),
		cpuStatPath: cpuStat,
	}
	if err := reporter.report(t.Context()); err != nil {
		t.Fatal(err)
	}

	status := <-statusCh
	if status.ClientID != "runner-abc123" {
		t.Fatalf("ClientID = %q", status.ClientID)
	}
	if status.LastProgressUnixSeconds != 1_700_000_000.5 {
		t.Fatalf("LastProgressUnixSeconds = %f", status.LastProgressUnixSeconds)
	}
	if status.RunnerCPUUsageSecondsTotal != 1.25 {
		t.Fatalf("RunnerCPUUsageSecondsTotal = %f", status.RunnerCPUUsageSecondsTotal)
	}
	if status.HeldLocks != 0 || status.LockWaiters != 0 {
		t.Fatalf("unexpected lock status: %+v", status)
	}
}

func TestReadCPUUsageSecondsRequiresUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.stat")
	if err := os.WriteFile(path, []byte("user_usec 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readCPUUsageSeconds(path); err == nil {
		t.Fatal("readCPUUsageSeconds succeeded without usage_usec")
	}
}
