//go:build unix

package cachers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"
)

// A helper that lost the SH-to-EX race must use the object another helper
// published, not wait for that helper to exit and release its retained SH.
func TestPutUsesObjectPublishedWhileSwitchingLockModes(t *testing.T) {
	dir := t.TempDir()
	data := []byte("published")
	a := &DiskCache{Dir: dir, HoldOpen: true, LockTimeout: 5 * time.Second}
	b := &DiskCache{Dir: dir, HoldOpen: true, LockTimeout: 5 * time.Second}
	defer a.Close()
	defer b.Close()

	if _, err := a.Put(context.Background(), testActionA, testOutputA, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// b reaches the EX step after a published and retained SH on the object.
	start := time.Now()
	lock, f, err := b.acquireForPublish(context.Background(), b.OutputFilename(testOutputA), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.close()
	defer f.Close()
	if f == nil {
		t.Fatal("expected the published object")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %v for a lock retained by another helper", elapsed)
	}
}

// Helpers sharing one dir and publishing the same new objects concurrently
// must never wait for each other's retained locks.
func TestConcurrentHelpersPublishSameObjects(t *testing.T) {
	dir := t.TempDir()
	helpers := []*DiskCache{
		{Dir: dir, HoldOpen: true, LockTimeout: 5 * time.Second},
		{Dir: dir, HoldOpen: true, LockTimeout: 5 * time.Second},
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2*8*200)
	for _, dc := range helpers {
		defer dc.Close()
		for g := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 200 {
					body := fmt.Appendf(nil, "obj-%d", i)
					outputID := fmt.Sprintf("%x", sha256.Sum256(body))
					actionID := fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "act-%d-%d", i, g)))
					if _, err := dc.Put(context.Background(), actionID, outputID, int64(len(body)), bytes.NewReader(body)); err != nil {
						errs <- err
					}
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
