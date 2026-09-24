package cachers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// indexEntry is the metadata that DiskCache stores on disk for an ActionID.
type indexEntry struct {
	Version   int    `json:"v"`
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeNanos int64  `json:"t"`
}

type heldObject struct {
	lock *fileLock
	file *os.File
}

const diskLockStripes = 256

const lockRetryInterval = 100 * time.Millisecond

type DiskCache struct {
	Dir     string
	Verbose bool
	Logf    func(format string, args ...any) // optional alt logger

	// HoldOpen keeps shared object locks and descriptors open until Close.
	// Enable this only for GOCACHEPROG helpers, whose returned DiskPaths must
	// remain protected for the lifetime of the helper process.
	HoldOpen bool

	// LockTimeout bounds how long Get or Put waits to acquire an object lock.
	// A zero value preserves the historical unbounded wait.
	LockTimeout time.Duration

	gates [diskLockStripes]sync.Mutex

	mu     sync.Mutex
	held   map[string]heldObject
	closed bool

	lockWaiters atomic.Int64
}

func (dc *DiskCache) logf(format string, args ...any) {
	if dc.Logf != nil {
		dc.Logf(format, args...)
	} else if dc.Verbose {
		log.Printf(format, args...)
	}
}

func (dc *DiskCache) outputGate(outputID string) *sync.Mutex {
	return &dc.gates[hexNibble(outputID[0])*16+hexNibble(outputID[1])]
}

func hexNibble(b byte) int {
	if b <= '9' {
		return int(b - '0')
	}
	return int(b-'a') + 10
}

func (dc *DiskCache) checkOpen() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.closed {
		return errors.New("disk cache is closed")
	}
	return nil
}

func (dc *DiskCache) heldObject(path string) (heldObject, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	h, ok := dc.held[path]
	return h, ok
}

func (dc *DiskCache) retain(path string, h heldObject) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.closed {
		return errors.New("disk cache is closed")
	}
	if dc.held == nil {
		dc.held = make(map[string]heldObject)
	}
	dc.held[path] = h
	return nil
}

// HeldCount reports the number of object locks retained for cmd/go.
func (dc *DiskCache) HeldCount() int {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return len(dc.held)
}

// LockWaiters reports the number of requests waiting for an object lock.
func (dc *DiskCache) LockWaiters() int64 {
	return dc.lockWaiters.Load()
}

func (dc *DiskCache) acquireLock(ctx context.Context, path string, exclusive bool) (*fileLock, error) {
	if dc.LockTimeout <= 0 {
		lock, _, err := acquireFileLock(path, exclusive, false)
		return lock, err
	}

	ctx, cancel := context.WithTimeout(ctx, dc.LockTimeout)
	defer cancel()

	waiting := false
	defer func() {
		if waiting {
			dc.lockWaiters.Add(-1)
		}
	}()

	for {
		lock, acquired, err := acquireFileLock(path, exclusive, true)
		if err != nil {
			return nil, err
		}
		if acquired {
			return lock, nil
		}
		if !waiting {
			waiting = true
			dc.lockWaiters.Add(1)
		}

		timer := time.NewTimer(lockRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, fmt.Errorf("acquiring cache object lock %s: %w", path, ctx.Err())
		case <-timer.C:
		}
	}
}

// Close releases all object locks and descriptors retained for paths returned
// to cmd/go. It is safe to call more than once.
func (dc *DiskCache) Close() error {
	dc.mu.Lock()
	if dc.closed {
		dc.mu.Unlock()
		return nil
	}
	dc.closed = true
	held := dc.held
	dc.held = nil
	dc.mu.Unlock()

	var errs []error
	for _, h := range held {
		errs = append(errs, h.file.Close(), h.lock.close())
	}
	return errors.Join(errs...)
}

func (dc *DiskCache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	if !validHex(actionID) {
		return "", "", errors.New("actionID must be valid hex strings")
	}
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if err := dc.checkOpen(); err != nil {
		return "", "", err
	}

	actionFile := dc.ActionFilename(actionID)
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			dc.logf("disk miss: %v", actionID)
			return "", "", nil
		}
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		dc.logf("Warning: JSON error for action %q: %v", actionID, err)
		return "", "", nil
	}
	if !validHex(ie.OutputID) || ie.Size < 0 {
		return "", "", nil
	}

	outputFile := dc.OutputFilename(ie.OutputID)
	gate := dc.outputGate(ie.OutputID)
	gate.Lock()
	defer gate.Unlock()

	if dc.HoldOpen {
		if h, ok := dc.heldObject(outputFile); ok {
			if !validHeldObject(h.file, outputFile, ie.Size) {
				return "", "", nil
			}
			if err := touchFile(outputFile); err != nil {
				return "", "", err
			}
			return ie.OutputID, outputFile, nil
		}
	}

	if err := os.MkdirAll(filepath.Dir(outputFile), 0o755); err != nil {
		return "", "", err
	}
	lock, err := dc.acquireLock(ctx, outputFile+".lock", false)
	if err != nil {
		return "", "", err
	}
	f, valid, err := openValidObject(outputFile, ie.Size)
	if err != nil {
		lock.close()
		return "", "", err
	}
	if !valid {
		lock.close()
		return "", "", nil
	}
	if err := touchFile(outputFile); err != nil {
		f.Close()
		lock.close()
		return "", "", err
	}
	if dc.HoldOpen {
		if err := dc.retain(outputFile, heldObject{lock: lock, file: f}); err != nil {
			f.Close()
			lock.close()
			return "", "", err
		}
	} else if err := errors.Join(f.Close(), lock.close()); err != nil {
		return "", "", err
	}
	return ie.OutputID, outputFile, nil
}

func openValidObject(path string, size int64) (*os.File, bool, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !validHeldObject(f, path, size) {
		if err := f.Close(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	return f, true, nil
}

func validHeldObject(f *os.File, path string, size int64) bool {
	fileInfo, err := f.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Size() != size {
		return false
	}
	pathInfo, err := os.Lstat(path)
	return err == nil && pathInfo.Mode().IsRegular() && os.SameFile(fileInfo, pathInfo)
}

func touchFile(path string) error {
	now := time.Now()
	return os.Chtimes(path, now, now)
}

func (dc *DiskCache) OutputFilename(outputID string) string {
	if !validHex(outputID) {
		return ""
	}
	return filepath.Join(dc.Dir, outputID[:2], fmt.Sprintf("o-%s", outputID))
}

func (dc *DiskCache) ActionFilename(actionID string) string {
	if !validHex(actionID) {
		return ""
	}
	return filepath.Join(dc.Dir, actionID[:2], fmt.Sprintf("a-%s", actionID))
}

func validHex(x string) bool {
	if len(x) < 4 || len(x) > 100 {
		return false
	}
	for _, b := range x {
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return false
	}
	return true
}

func (dc *DiskCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	if !validHex(actionID) || !validHex(outputID) {
		return "", errors.New("actionID and outputID must be valid hex strings")
	}
	if size < 0 {
		return "", errors.New("size must not be negative")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := dc.checkOpen(); err != nil {
		return "", err
	}

	actionFile := dc.ActionFilename(actionID)
	outputFile := dc.OutputFilename(outputID)
	if err := os.MkdirAll(filepath.Dir(actionFile), 0o755); err != nil {
		return "", fmt.Errorf("failed to create action directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outputFile), 0o755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	gate := dc.outputGate(outputID)
	gate.Lock()
	defer gate.Unlock()

	if dc.HoldOpen {
		if h, ok := dc.heldObject(outputFile); ok {
			if !validHeldObject(h.file, outputFile, size) {
				return "", errors.New("retained output has unexpected size or type")
			}
			if err := drainBody(body, size); err != nil {
				return "", err
			}
			if err := dc.writeIndex(actionFile, outputID, size); err != nil {
				return "", err
			}
			if err := touchFile(outputFile); err != nil {
				return "", err
			}
			return outputFile, nil
		}
	}

	// Immutable objects normally already exist. Join current readers with a
	// shared lock so this fast path never waits for GOCACHEPROG helpers to exit.
	lock, err := dc.acquireLock(ctx, outputFile+".lock", false)
	if err != nil {
		return "", err
	}
	f, valid, err := openValidObject(outputFile, size)
	if err != nil {
		lock.close()
		return "", err
	}
	if valid {
		if err := drainBody(body, size); err != nil {
			f.Close()
			lock.close()
			return "", err
		}
		return dc.finishPut(actionFile, outputFile, outputID, size, f, lock)
	}
	if err := lock.close(); err != nil {
		return "", err
	}

	// Release SH before taking EX, then recheck because another process may
	// have published the object while this process was switching lock modes.
	lock, err = dc.acquireLock(ctx, outputFile+".lock", true)
	if err != nil {
		return "", err
	}
	f, valid, err = openValidObject(outputFile, size)
	if err != nil {
		lock.close()
		return "", err
	}
	if valid {
		if err := drainBody(body, size); err != nil {
			f.Close()
			lock.close()
			return "", err
		}
	} else {
		wrote, err := writeOutputFile(outputFile, body, size, outputID)
		if err != nil {
			lock.close()
			return "", err
		}
		if wrote != size {
			lock.close()
			return "", fmt.Errorf("wrote %d bytes, expected %d", wrote, size)
		}
		f, valid, err = openValidObject(outputFile, size)
		if err != nil {
			lock.close()
			return "", err
		}
		if !valid {
			lock.close()
			return "", errors.New("published output has unexpected size or type")
		}
	}
	return dc.finishPut(actionFile, outputFile, outputID, size, f, lock)
}

func drainBody(body io.Reader, size int64) error {
	n, err := io.Copy(io.Discard, body)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("read %d bytes, expected %d", n, size)
	}
	return nil
}

func (dc *DiskCache) finishPut(actionFile, outputFile, outputID string, size int64, f *os.File, lock *fileLock) (string, error) {
	if err := dc.writeIndex(actionFile, outputID, size); err != nil {
		f.Close()
		lock.close()
		return "", err
	}
	if err := touchFile(outputFile); err != nil {
		f.Close()
		lock.close()
		return "", err
	}
	if dc.HoldOpen {
		if err := lock.downgrade(); err != nil {
			f.Close()
			lock.close()
			return "", err
		}
		if err := dc.retain(outputFile, heldObject{lock: lock, file: f}); err != nil {
			f.Close()
			lock.close()
			return "", err
		}
	} else if err := errors.Join(f.Close(), lock.close()); err != nil {
		return "", err
	}
	return outputFile, nil
}

func (dc *DiskCache) writeIndex(actionFile, outputID string, size int64) error {
	ij, err := json.Marshal(indexEntry{
		Version:   1,
		OutputID:  outputID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	return writeActionFile(actionFile, ij)
}
