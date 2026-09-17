//go:build unix

package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testActionA = "aabbccdd"
	testActionB = "bbccddee"
	testOutputA = "00112233"
	testOutputB = "11223344"
)

func testSweeper(dir string, maxBytes int64) *Sweeper {
	s := NewSweeper(SweepConfig{Dir: dir, MaxBytes: maxBytes})
	s.randN = func(int64) int64 { return 0 }
	s.fsUsage = func(string) (uint64, uint64, error) { return 0, 100, nil }
	return s
}

func writeTestIndex(t *testing.T, dc *DiskCache, actionID, outputID string, size int64) {
	t.Helper()
	path := dc.ActionFilename(actionID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(indexEntry{Version: 1, OutputID: outputID, Size: size})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func putAndClose(t *testing.T, dir, actionID, outputID string, data []byte) string {
	t.Helper()
	dc := &DiskCache{Dir: dir}
	path, err := dc.Put(context.Background(), actionID, outputID, int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestHTTPClientGetDanglingIndexRefillsFromRemote(t *testing.T) {
	dir := t.TempDir()
	dc := &DiskCache{Dir: dir}
	t.Cleanup(func() { dc.Close() })
	writeTestIndex(t, dc, testActionA, testOutputA, 6)
	if outputID, path, err := dc.Get(context.Background(), testActionA); err != nil || outputID != "" || path != "" {
		t.Fatalf("dangling index was not a clean miss: output=%q path=%q err=%v", outputID, path, err)
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Go-Output-Id", testOutputA)
		w.Header().Set("Content-Length", "6")
		_, _ = w.Write([]byte("remote"))
	}))
	defer server.Close()

	client := &HTTPClient{BaseURL: server.URL, Disk: dc}
	outputID, path, err := client.Get(context.Background(), testActionA)
	if err != nil {
		t.Fatal(err)
	}
	if outputID != testOutputA {
		t.Fatalf("output ID = %q, want %q", outputID, testOutputA)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "remote" {
		t.Fatalf("refilled object = %q, %v", got, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("remote requests = %d, want 1", requests.Load())
	}
}

func TestDiskCacheGetRejectsBadObject(t *testing.T) {
	tests := []struct {
		name       string
		makeObject func(*testing.T, string)
	}{
		{"size mismatch", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte("wrong"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"not regular", func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", func(t *testing.T, path string) {
			target := path + ".target"
			if err := os.WriteFile(target, []byte("remote"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc := &DiskCache{Dir: t.TempDir()}
			t.Cleanup(func() { dc.Close() })
			writeTestIndex(t, dc, testActionA, testOutputA, 6)
			path := dc.OutputFilename(testOutputA)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			tt.makeObject(t, path)
			outputID, diskPath, err := dc.Get(context.Background(), testActionA)
			if err != nil {
				t.Fatal(err)
			}
			if outputID != "" || diskPath != "" {
				t.Fatalf("bad object was a hit: %q, %q", outputID, diskPath)
			}
		})
	}
}

func TestDiskCacheDefaultDoesNotRetain(t *testing.T) {
	dir := t.TempDir()
	dc := &DiskCache{Dir: dir}
	data := []byte("server object")
	path, err := dc.Put(context.Background(), testActionA, testOutputA, int64(len(data)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	for range 1000 {
		outputID, gotPath, err := dc.Get(context.Background(), testActionA)
		if err != nil {
			t.Fatal(err)
		}
		if outputID != testOutputA || gotPath != path {
			t.Fatalf("unexpected hit: output=%q path=%q", outputID, gotPath)
		}
	}
	dc.mu.Lock()
	retained := len(dc.held)
	dc.mu.Unlock()
	if retained != 0 {
		t.Fatalf("default DiskCache retained %d objects", retained)
	}
	if _, err := os.Stat(dc.ActionFilename(testActionA) + ".lock"); !os.IsNotExist(err) {
		t.Fatalf("action sidecar lock was created: %v", err)
	}

	result, err := testSweeper(dir, 1).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 1 {
		t.Fatalf("non-retained object remained locked: %+v", result)
	}
}

func TestPutFastPathDoesNotBlockOnRetainedSharedLock(t *testing.T) {
	dir := t.TempDir()
	data := []byte("immutable object")
	holder := &DiskCache{Dir: dir, HoldOpen: true}
	defer holder.Close()
	if _, err := holder.Put(context.Background(), testActionA, testOutputA, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	other := &DiskCache{Dir: dir, HoldOpen: true}
	done := make(chan error, 1)
	go func() {
		_, err := other.Put(context.Background(), testActionB, testOutputA, int64(len(data)), bytes.NewReader(data))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Put blocked behind another helper's retained shared lock")
	}

	checker := &DiskCache{Dir: dir}
	outputID, _, err := checker.Get(context.Background(), testActionB)
	if err != nil {
		t.Fatal(err)
	}
	if outputID != testOutputA {
		t.Fatalf("fast-path action index output = %q, want %q", outputID, testOutputA)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := testSweeper(dir, 1).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 0 {
		t.Fatal("fast-path Put did not retain its shared lock")
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskCacheGetRefreshesMtime(t *testing.T) {
	dir := t.TempDir()
	path := putAndClose(t, dir, testActionA, testOutputA, []byte("object"))
	old := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	dc := &DiskCache{Dir: dir}
	defer dc.Close()
	if _, _, err := dc.Get(context.Background(), testActionA); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().After(old) {
		t.Fatalf("mtime = %v, want after %v", info.ModTime(), old)
	}
}

func TestDiskCacheSharedLockPreventsSweepUntilClose(t *testing.T) {
	dir := t.TempDir()
	path := putAndClose(t, dir, testActionA, testOutputA, []byte("object"))
	reader := &DiskCache{Dir: dir, HoldOpen: true}
	if _, _, err := reader.Get(context.Background(), testActionA); err != nil {
		t.Fatal(err)
	}

	sweeper := testSweeper(dir, 1)
	result, err := sweeper.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 0 {
		t.Fatalf("deleted %d locked objects", result.ObjectsDeleted)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locked object disappeared: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	result, err = sweeper.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 1 {
		t.Fatalf("deleted %d objects after Close, want 1", result.ObjectsDeleted)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("object still exists after sweep: %v", err)
	}
	if _, err := os.Stat((&DiskCache{Dir: dir}).ActionFilename(testActionA)); !os.IsNotExist(err) {
		t.Fatalf("action index still exists after object deletion: %v", err)
	}
}

func TestSweepSizeEvictionUsesLRU(t *testing.T) {
	dir := t.TempDir()
	oldPath := putAndClose(t, dir, testActionA, testOutputA, []byte("oldest"))
	newPath := putAndClose(t, dir, testActionB, testOutputB, []byte("newest"))
	base := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(oldPath, base, base); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newPath, base.Add(time.Hour), base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	result, err := testSweeper(dir, int64(len("newest"))).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 1 || result.BytesDeleted != int64(len("oldest")) {
		t.Fatalf("unexpected result: %+v", result)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("oldest object was not evicted: %v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("newest object was evicted: %v", err)
	}
}

func TestSweepFilesystemHighToLowWatermark(t *testing.T) {
	dir := t.TempDir()
	putAndClose(t, dir, testActionA, testOutputA, bytes.Repeat([]byte("a"), 10))
	putAndClose(t, dir, testActionB, testOutputB, bytes.Repeat([]byte("b"), 10))
	sweeper := testSweeper(dir, 0)
	sweeper.Config.FilesystemHighPct = 90
	sweeper.Config.FilesystemLowPct = 80
	sweeper.fsUsage = func(string) (uint64, uint64, error) { return 95, 100, nil }

	result, err := sweeper.SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 2 || result.BytesDeleted != 20 {
		t.Fatalf("unexpected watermark result: %+v", result)
	}
}

func TestSweepExcludesLockAndTempFilesFromAccounting(t *testing.T) {
	dir := t.TempDir()
	path := putAndClose(t, dir, testActionA, testOutputA, []byte("object"))
	if err := os.WriteFile(path+".lock", bytes.Repeat([]byte("l"), 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".temporary", bytes.Repeat([]byte("t"), 100), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := testSweeper(dir, int64(len("object"))).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesBefore != int64(len("object")) || result.ObjectsDeleted != 0 {
		t.Fatalf("lock or temp file was accounted: %+v", result)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("object was unexpectedly deleted: %v", err)
	}
}

func TestSweepSkipsLockedVictim(t *testing.T) {
	dir := t.TempDir()
	oldPath := putAndClose(t, dir, testActionA, testOutputA, []byte("oldest"))
	newPath := putAndClose(t, dir, testActionB, testOutputB, []byte("newest"))
	reader := &DiskCache{Dir: dir, HoldOpen: true}
	defer reader.Close()
	if _, _, err := reader.Get(context.Background(), testActionA); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(oldPath, base, base); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newPath, base.Add(time.Hour), base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	result, err := testSweeper(dir, 1).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectsDeleted != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("locked oldest object was evicted: %v", err)
	}
	if _, err := os.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf("unlocked object was not evicted: %v", err)
	}
}

func TestSweepLockSerialization(t *testing.T) {
	dir := t.TempDir()
	first := testSweeper(dir, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	first.locked = func() {
		close(entered)
		<-release
	}
	done := make(chan error, 1)
	go func() {
		_, err := first.SweepOnce(context.Background())
		done <- err
	}()
	<-entered

	result, err := testSweeper(dir, 1).SweepOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Skipped {
		t.Fatal("second sweep did not skip while global lock was held")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentGetPutSweep(t *testing.T) {
	dir := t.TempDir()
	dc := &DiskCache{Dir: dir, HoldOpen: true}
	defer dc.Close()
	data := []byte("concurrent object")
	if _, err := dc.Put(context.Background(), testActionA, testOutputA, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	sweeper := testSweeper(dir, 1)
	start := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for range 25 {
			outputID, _, err := dc.Get(context.Background(), testActionA)
			if err != nil || outputID != testOutputA {
				errs <- fmt.Errorf("get: output=%q err=%w", outputID, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 25 {
			if _, err := dc.Put(context.Background(), testActionA, testOutputA, int64(len(data)), bytes.NewReader(data)); err != nil {
				errs <- fmt.Errorf("put: %w", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 25 {
			if _, err := sweeper.SweepOnce(context.Background()); err != nil {
				errs <- fmt.Errorf("sweep: %w", err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
