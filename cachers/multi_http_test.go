package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestServerIndex(t *testing.T) {
	// Same actionID should always route to the same server
	idx1 := serverIndex("aabbccdd", 3)
	idx2 := serverIndex("aabbccdd", 3)
	if idx1 != idx2 {
		t.Errorf("same actionID gave different indices: %d vs %d", idx1, idx2)
	}

	// Different actionIDs should (likely) route to different servers
	// with enough servers. Not guaranteed, but good sanity check.
	indices := map[int]bool{}
	for _, id := range []string{"aabbccdd", "11223344", "ffeeddcc", "99887766"} {
		indices[serverIndex(id, 100)] = true
	}
	if len(indices) < 2 {
		t.Errorf("expected at least 2 distinct indices from 4 different actionIDs, got %d", len(indices))
	}

	// Single server always returns 0
	if idx := serverIndex("aabbccdd", 1); idx != 0 {
		t.Errorf("single server: got index %d, want 0", idx)
	}
}

func TestMultiHTTPClientGetFallback(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("hello from secondary server")

	// Primary server returns 404 (miss)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer primary.Close()

	// Secondary server has the data
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/action/"+testActionID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ActionValue{
				OutputID: testOutputID,
				Size:     int64(len(testData)),
			})
			return
		}
		if r.URL.Path == "/output/"+testOutputID {
			w.Header().Set("Content-Length", "27")
			w.Write(testData)
			return
		}
		http.NotFound(w, r)
	}))
	defer secondary.Close()

	disk := &DiskCache{Dir: t.TempDir()}

	// We need to ensure primary is tried first. Since serverIndex is deterministic,
	// we arrange the clients so index 0 is primary (miss) and index 1 is secondary (hit).
	// We use serverIndex to figure out which order to place them.
	idx := serverIndex(testActionID, 2)
	clients := make([]*HTTPClient, 2)
	if idx == 0 {
		clients[0] = &HTTPClient{BaseURL: primary.URL, Disk: disk, BestEffortHTTP: true}
		clients[1] = &HTTPClient{BaseURL: secondary.URL, Disk: disk, BestEffortHTTP: true}
	} else {
		clients[0] = &HTTPClient{BaseURL: secondary.URL, Disk: disk, BestEffortHTTP: true}
		clients[1] = &HTTPClient{BaseURL: primary.URL, Disk: disk, BestEffortHTTP: true}
	}

	mc := &MultiHTTPClient{
		Clients: clients,
		Disk:    disk,
	}

	outputID, diskPath, err := mc.Get(context.Background(), testActionID)
	if err != nil {
		t.Fatal(err)
	}
	if outputID != testOutputID {
		t.Errorf("outputID = %q, want %q", outputID, testOutputID)
	}
	if diskPath == "" {
		t.Fatal("diskPath is empty")
	}

	got, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testData) {
		t.Errorf("disk content = %q, want %q", got, testData)
	}
}

func TestMultiHTTPClientGetFallbackUnreachable(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("hello from reachable server")

	// Unreachable server (closed port)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachableURL := "http://" + ln.Addr().String()
	ln.Close()

	// Working server
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/action/"+testActionID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ActionValue{
				OutputID: testOutputID,
				Size:     int64(len(testData)),
			})
			return
		}
		if r.URL.Path == "/output/"+testOutputID {
			w.Header().Set("Content-Length", "27")
			w.Write(testData)
			return
		}
		http.NotFound(w, r)
	}))
	defer working.Close()

	disk := &DiskCache{Dir: t.TempDir()}

	idx := serverIndex(testActionID, 2)
	clients := make([]*HTTPClient, 2)
	if idx == 0 {
		clients[0] = &HTTPClient{BaseURL: unreachableURL, Disk: disk, BestEffortHTTP: true}
		clients[1] = &HTTPClient{BaseURL: working.URL, Disk: disk, BestEffortHTTP: true}
	} else {
		clients[0] = &HTTPClient{BaseURL: working.URL, Disk: disk, BestEffortHTTP: true}
		clients[1] = &HTTPClient{BaseURL: unreachableURL, Disk: disk, BestEffortHTTP: true}
	}

	mc := &MultiHTTPClient{
		Clients: clients,
		Disk:    disk,
	}

	outputID, diskPath, err := mc.Get(context.Background(), testActionID)
	if err != nil {
		t.Fatal(err)
	}
	if outputID != testOutputID {
		t.Errorf("outputID = %q, want %q", outputID, testOutputID)
	}
	if diskPath == "" {
		t.Fatal("diskPath is empty")
	}
}

func TestMultiHTTPClientPutDiskSucceedsWhenPrimaryDown(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("data to put")

	// Unreachable server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachableURL := "http://" + ln.Addr().String()
	ln.Close()

	disk := &DiskCache{Dir: t.TempDir()}

	mc := &MultiHTTPClient{
		Clients: []*HTTPClient{
			{BaseURL: unreachableURL, Disk: disk, BestEffortHTTP: true},
		},
		Disk: disk,
	}

	diskPath, err := mc.Put(context.Background(), testActionID, testOutputID, int64(len(testData)), bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if diskPath == "" {
		t.Fatal("diskPath is empty")
	}

	got, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testData) {
		t.Errorf("disk content = %q, want %q", got, testData)
	}
}

func TestMultiHTTPClientSingleServer(t *testing.T) {
	const (
		testActionID = "aabbccdd"
		testOutputID = "eeff0011"
	)
	testData := []byte("single server data")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/action/"+testActionID {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ActionValue{
				OutputID: testOutputID,
				Size:     int64(len(testData)),
			})
			return
		}
		if r.Method == "GET" && r.URL.Path == "/output/"+testOutputID {
			w.Header().Set("Content-Length", "18")
			w.Write(testData)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	disk := &DiskCache{Dir: t.TempDir()}

	mc := &MultiHTTPClient{
		Clients: []*HTTPClient{
			{BaseURL: ts.URL, Disk: disk, BestEffortHTTP: true},
		},
		Disk: disk,
	}

	outputID, diskPath, err := mc.Get(context.Background(), testActionID)
	if err != nil {
		t.Fatal(err)
	}
	if outputID != testOutputID {
		t.Errorf("outputID = %q, want %q", outputID, testOutputID)
	}
	if diskPath == "" {
		t.Fatal("diskPath is empty")
	}

	got, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testData) {
		t.Errorf("disk content = %q, want %q", got, testData)
	}
}
