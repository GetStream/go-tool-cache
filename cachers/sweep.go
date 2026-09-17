package cachers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// SweepConfig controls cache eviction. A zero MaxBytes disables the tree-size
// limit. Filesystem watermarks are disabled when both percentages are zero.
type SweepConfig struct {
	Dir               string
	MaxBytes          int64
	FilesystemHighPct float64
	FilesystemLowPct  float64
	BaseInterval      time.Duration
	Jitter            time.Duration
	InitialDelay      time.Duration
	Logf              func(format string, args ...any)
}

// SweepResult describes one attempted sweep.
type SweepResult struct {
	Skipped        bool
	ObjectsScanned int
	ObjectsDeleted int
	BytesBefore    int64
	BytesDeleted   int64
}

type cacheObject struct {
	id   string
	path string
	info os.FileInfo
}

// Sweeper evicts least-recently-used objects from a shared disk cache.
type Sweeper struct {
	Config SweepConfig

	randN   func(int64) int64
	fsUsage func(string) (used, total uint64, err error)
	locked  func() // test hook, called while holding .sweep.lock
}

func NewSweeper(cfg SweepConfig) *Sweeper {
	return &Sweeper{Config: cfg, randN: rand.Int64N, fsUsage: filesystemUsage}
}

func (s *Sweeper) validate() error {
	cfg := s.Config
	if cfg.Dir == "" {
		return errors.New("cache directory is required")
	}
	if cfg.MaxBytes < 0 {
		return errors.New("max bytes must not be negative")
	}
	if cfg.FilesystemHighPct == 0 && cfg.FilesystemLowPct == 0 {
		return nil
	}
	if cfg.FilesystemLowPct < 0 || cfg.FilesystemHighPct > 100 || cfg.FilesystemHighPct <= cfg.FilesystemLowPct {
		return errors.New("filesystem percentages must satisfy 0 <= low < high <= 100")
	}
	return nil
}

func (s *Sweeper) randomDuration(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	return time.Duration(s.randN(int64(limit)))
}

func waitFor(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run waits for a randomized initial delay and then sweeps until ctx is
// canceled. Errors are logged and retried only after the next full interval.
func (s *Sweeper) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	if s.Config.BaseInterval <= 0 {
		return errors.New("base interval must be positive")
	}
	if s.Config.Jitter < 0 || s.Config.InitialDelay < 0 {
		return errors.New("jitter and initial delay must not be negative")
	}
	if err := waitFor(ctx, s.randomDuration(s.Config.InitialDelay)); err != nil {
		return err
	}
	for {
		if _, err := s.SweepOnce(ctx); err != nil && s.Config.Logf != nil {
			s.Config.Logf("cache sweep failed: %v", err)
		}
		delay := s.Config.BaseInterval + s.randomDuration(s.Config.Jitter)
		if err := waitFor(ctx, delay); err != nil {
			return err
		}
	}
}

// SweepOnce performs one nonblocking sweep attempt.
func (s *Sweeper) SweepOnce(ctx context.Context) (SweepResult, error) {
	var result SweepResult
	if err := s.validate(); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := os.MkdirAll(s.Config.Dir, 0o755); err != nil {
		return result, err
	}

	used, total, err := s.fsUsage(s.Config.Dir)
	if err != nil {
		return result, fmt.Errorf("checking filesystem usage: %w", err)
	}
	sweepLock, acquired, err := acquireFileLock(filepath.Join(s.Config.Dir, ".sweep.lock"), true, true)
	if err != nil {
		return result, fmt.Errorf("locking sweep: %w", err)
	}
	if !acquired {
		result.Skipped = true
		return result, nil
	}
	defer sweepLock.close()
	if s.locked != nil {
		s.locked()
	}

	objects, actions, err := s.scan(ctx)
	if err != nil {
		return result, err
	}
	result.ObjectsScanned = len(objects)
	for _, object := range objects {
		result.BytesBefore += object.info.Size()
	}

	bytesNeeded := int64(0)
	if s.Config.MaxBytes > 0 {
		bytesNeeded = max(int64(0), result.BytesBefore-s.Config.MaxBytes)
	}
	if total > 0 && s.Config.FilesystemHighPct > 0 && float64(used)*100 >= s.Config.FilesystemHighPct*float64(total) {
		toLow := int64(float64(used) - s.Config.FilesystemLowPct*float64(total)/100)
		bytesNeeded = max(bytesNeeded, toLow)
	}
	if bytesNeeded <= 0 {
		return result, nil
	}

	slices.SortStableFunc(objects, func(a, b cacheObject) int {
		return a.info.ModTime().Compare(b.info.ModTime())
	})
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if result.BytesDeleted >= bytesNeeded {
			break
		}
		deleted, err := s.deleteObject(object, actions[object.id])
		if err != nil {
			return result, err
		}
		if deleted {
			result.ObjectsDeleted++
			result.BytesDeleted += object.info.Size()
		}
	}
	return result, nil
}

func (s *Sweeper) scan(ctx context.Context) ([]cacheObject, map[string][]string, error) {
	var objects []cacheObject
	actions := make(map[string][]string)
	start := int(s.randN(256))
	for offset := range 256 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		prefix := fmt.Sprintf("%02x", (start+offset)%256)
		dir := filepath.Join(s.Config.Dir, prefix)
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading cache prefix %s: %w", prefix, err)
		}
		for _, entry := range entries {
			if err := s.scanEntry(dir, prefix, entry, &objects, actions); err != nil {
				return nil, nil, err
			}
		}
	}
	return objects, actions, nil
}

func (s *Sweeper) scanEntry(dir, prefix string, entry os.DirEntry, objects *[]cacheObject, actions map[string][]string) error {
	name := entry.Name()
	path := filepath.Join(dir, name)
	if strings.HasPrefix(name, "o-") {
		id := strings.TrimPrefix(name, "o-")
		if !validHex(id) || !strings.HasPrefix(id, prefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			*objects = append(*objects, cacheObject{id: id, path: path, info: info})
		}
		return nil
	}
	if !strings.HasPrefix(name, "a-") || !validHex(strings.TrimPrefix(name, "a-")) {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var index indexEntry
	if json.Unmarshal(data, &index) == nil && validHex(index.OutputID) {
		actions[index.OutputID] = append(actions[index.OutputID], path)
	}
	return nil
}

func (s *Sweeper) deleteObject(object cacheObject, actionPaths []string) (bool, error) {
	lock, acquired, err := acquireFileLock(object.path+".lock", true, true)
	if err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	defer lock.close()

	info, err := os.Stat(object.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(object.info, info) || info.Size() != object.info.Size() || !info.ModTime().Equal(object.info.ModTime()) {
		return false, nil
	}

	for _, actionPath := range actionPaths {
		data, err := os.ReadFile(actionPath)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		var index indexEntry
		if json.Unmarshal(data, &index) == nil && index.OutputID == object.id {
			if err := os.Remove(actionPath); err != nil && !os.IsNotExist(err) {
				return false, err
			}
		}
	}

	if err := os.Remove(object.path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
