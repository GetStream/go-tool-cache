package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/GetStream/go-tool-cache/cacheproc"
	"github.com/GetStream/go-tool-cache/cachers"
	"github.com/GetStream/go-tool-cache/gocacheproxy"
)

const runnerCPUStatPath = "/sys/fs/cgroup/cpu.stat"

type statusReporter struct {
	URL      string
	ClientID string
	Interval time.Duration
	Disk     *cachers.DiskCache
	Process  *cacheproc.Process
	Client   *http.Client
	Verbose  bool

	cpuStatPath string
}

func (r *statusReporter) Run(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := r.report(ctx); err != nil && r.Verbose && ctx.Err() == nil {
			log.Printf("go-cacher: reporting client status: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *statusReporter) report(ctx context.Context) error {
	cpuStatPath := r.cpuStatPath
	if cpuStatPath == "" {
		cpuStatPath = runnerCPUStatPath
	}
	cpuSeconds, err := readCPUUsageSeconds(cpuStatPath)
	if err != nil {
		return err
	}

	status := gocacheproxy.ClientStatus{
		ClientID:                   r.ClientID,
		HeldLocks:                  r.Disk.HeldCount(),
		LockWaiters:                r.Disk.LockWaiters(),
		LastProgressUnixSeconds:    float64(r.Process.LastProgressUnixNano.Load()) / float64(time.Second),
		RunnerCPUUsageSecondsTotal: cpuSeconds,
	}
	body, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("marshal status: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create status request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := r.Client.Do(req)
	if err != nil {
		return fmt.Errorf("post status: %w", err)
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4<<10))
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("post status: %s", res.Status)
	}
	return nil
}

func readCPUUsageSeconds(path string) (float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open cgroup cpu.stat: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), " ")
		if !ok || key != "usage_usec" {
			continue
		}
		usec, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse cgroup usage_usec: %w", err)
		}
		return float64(usec) / 1e6, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read cgroup cpu.stat: %w", err)
	}
	return 0, errors.New("cgroup cpu.stat has no usage_usec")
}
