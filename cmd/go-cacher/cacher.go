// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/GetStream/go-tool-cache/cacheproc"
	"github.com/GetStream/go-tool-cache/cachers"
)

// shutdownDrainTimeout bounds how long Close waits for background PUTs to drain
// before abandoning them.
const shutdownDrainTimeout = 5 * time.Second

var (
	dir         = flag.String("cache-dir", "", "cache directory; empty means automatic")
	serverBase  = flag.String("cache-server", "", "optional cache server HTTP prefix(es), comma-separated (scheme and authority only); should be low latency. empty means to not use one.")
	verbose     = flag.Bool("verbose", false, "be verbose")
	gwPort      = flag.Int("gateway-addr-port", 0, "if non-zero, try to use an HTTP server on this port on our machine's gateway IP. If that fails, use local disk instead.")
	token       = flag.String("access-token", "", "optional access token to use with the cache server")
	httpTimeout = flag.Duration("http-timeout", 2*time.Minute, "timeout for each request to the remote cache")
	lockTimeout = flag.Duration("lock-timeout", 10*time.Minute, "maximum time to wait for a cache object lock; 0 means no timeout")
	statusURL   = flag.String("status-url", "", "optional gocacheproxy client-status endpoint")
	clientID    = flag.String("client-id", "", "stable client identifier included in status heartbeats")
	statusEvery = flag.Duration("status-interval", 15*time.Second, "interval between client-status heartbeats")
	sweepMode   = flag.Bool("sweep", false, "run the standalone cache sweeper instead of GOCACHEPROG")
	maxSizeGB   = flag.Int("max-size-gb", 100, "maximum cached object size in GiB; 0 means no limit")
	fsHighPct   = flag.Float64("filesystem-high-percent", 90, "filesystem usage percentage that starts eviction")
	fsLowPct    = flag.Float64("filesystem-low-percent", 80, "filesystem target percentage after eviction starts")
	interval    = flag.Duration("interval", 10*time.Minute, "base interval between sweep attempts")
	jitter      = flag.Duration("jitter", 5*time.Minute, "maximum random delay added to each interval")
	firstDelay  = flag.Duration("initial-delay", 5*time.Minute, "maximum randomized delay before the first sweep")
)

func main() {
	flag.Parse()
	if *httpTimeout <= 0 {
		log.Fatal("-http-timeout must be positive")
	}
	if (*statusURL == "") != (*clientID == "") {
		log.Fatal("-status-url and -client-id must be set together")
	}
	if *statusEvery <= 0 {
		log.Fatal("-status-interval must be positive")
	}
	if *verbose {
		log.Printf("go-cacher: verbose mode enabled")
	}
	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		log.Printf("Defaulting to cache dir %v ...", d)
		*dir = d
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}
	if *sweepMode {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		sweeper := cachers.NewSweeper(cachers.SweepConfig{
			Dir:               *dir,
			MaxBytes:          int64(*maxSizeGB) << 30,
			FilesystemHighPct: *fsHighPct,
			FilesystemLowPct:  *fsLowPct,
			BaseInterval:      *interval,
			Jitter:            *jitter,
			InitialDelay:      *firstDelay,
			Logf:              log.Printf,
		})
		if err := sweeper.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		return
	}

	dc := &cachers.DiskCache{Dir: *dir, HoldOpen: true, LockTimeout: *lockTimeout}
	defer dc.Close()

	var p *cacheproc.Process
	p = &cacheproc.Process{
		Get: dc.Get,
		Put: dc.Put,
	}
	p.LastProgressUnixNano.Store(time.Now().UnixNano())
	var hc *cachers.HTTPClient
	statsFunc := func() error {
		if *verbose {
			putDetail := fmt.Sprintf("%d errors", p.PutErrors.Load())
			if hc != nil {
				putDetail += fmt.Sprintf(", %d timed out, %d canceled", hc.PutsTimedOut.Load(), hc.PutsCanceled.Load())
			}
			log.Printf("cacher: closing; %d gets (%d hits, %d misses, %d errors); %d puts (%s)",
				p.Gets.Load(), p.GetHits.Load(), p.GetMisses.Load(), p.GetErrors.Load(), p.Puts.Load(), putDetail)
		}
		return dc.Close()
	}
	p.Close = statsFunc

	if *gwPort != 0 {
		if gw, ok := getGatewayIP(); ok {
			probe := net.JoinHostPort(gw, fmt.Sprint(*gwPort))
			log.Printf("go-cacher: probing gateway IP %v", gw)
			var d net.Dialer
			d.Timeout = time.Second / 2
			c, err := d.Dial("tcp", probe)
			if err != nil {
				log.Printf("go-cacher: failed to probe %v: %v", probe, err)
			} else {
				c.Close()
				*serverBase = "http://" + probe
			}
		} else {
			log.Printf("go-cacher: failed to get gateway IP; using local disk cache instead")
		}
	}

	if *serverBase != "" {
		remoteClient := &http.Client{Timeout: *httpTimeout}
		urls := strings.Split(*serverBase, ",")
		for i, u := range urls {
			u = strings.TrimSpace(u)
			if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
				u = "http://" + u
			}
			urls[i] = u
		}
		if len(urls) == 1 {
			hc = &cachers.HTTPClient{
				BaseURL:     urls[0],
				Disk:        dc,
				HTTPClient:  remoteClient,
				Verbose:     *verbose,
				AccessToken: *token,
			}
			p.Get = hc.Get
			p.Put = hc.Put
			p.Close = shutdownHTTP(hc, statsFunc)
		} else {
			clients := make([]*cachers.HTTPClient, len(urls))
			for i, u := range urls {
				clients[i] = &cachers.HTTPClient{
					BaseURL:        u,
					Disk:           dc,
					HTTPClient:     remoteClient,
					Verbose:        *verbose,
					AccessToken:    *token,
					BestEffortHTTP: true,
				}
			}
			mc := &cachers.MultiHTTPClient{
				Clients: clients,
				Disk:    dc,
				Verbose: *verbose,
			}
			p.Get = mc.Get
			p.Put = mc.Put
			p.Close = func() error {
				ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
				defer cancel()
				for _, c := range clients {
					if !c.Shutdown(ctx) {
						log.Printf("go-cacher: timed out waiting for background PUTs to drain (%s)", c.BaseURL)
					}
				}
				return statsFunc()
			}
		}
	}

	statusCtx, stopStatus := context.WithCancel(context.Background())
	var statusWG sync.WaitGroup
	if *statusURL != "" && *clientID != "" {
		reporter := statusReporter{
			URL:      *statusURL,
			ClientID: *clientID,
			Interval: *statusEvery,
			Disk:     dc,
			Process:  p,
			Client:   &http.Client{Timeout: 5 * time.Second},
			Verbose:  *verbose,
		}
		statusWG.Go(func() {
			reporter.Run(statusCtx)
		})
	}
	defer func() {
		stopStatus()
		statusWG.Wait()
	}()

	if err := p.Run(); err != nil {
		log.Fatal(err)
	}
}

func shutdownHTTP(hc *cachers.HTTPClient, statsFunc func() error) func() error {
	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
		defer cancel()
		if !hc.Shutdown(ctx) {
			log.Printf("go-cacher: timed out waiting for background PUTs to drain")
		}
		if timedOut, canceled := hc.PutsTimedOut.Load(), hc.PutsCanceled.Load(); timedOut+canceled > 0 {
			log.Printf("go-cacher: %d background PUTs timed out, %d canceled", timedOut, canceled)
		}
		return statsFunc()
	}
}

func getGatewayIP() (ip string, ok bool) {
	switch runtime.GOOS {
	case "darwin":
		return getDarwinGatewayIP()
	case "linux":
		return getLinuxGatewayIP()
	}
	return "", false
}

func getDarwinGatewayIP() (ip string, ok bool) {
	out, err := exec.Command("route", "-n", "get", "default").CombinedOutput()
	if err != nil {
		log.Printf("getGatewayIP: %v, %s", err, out)
		return "", false
	}
	rx := regexp.MustCompile(`(?m)^\s*gateway: (\S+)`)
	if m := rx.FindSubmatch(out); len(m) == 2 {
		return string(m[1]), true
	}
	return "", false
}

func getLinuxGatewayIP() (ip string, ok bool) {
	out, err := exec.Command("ip", "route", "show", "default").CombinedOutput()
	if err != nil {
		log.Printf("getGatewayIP: %v, %s", err, out)
		return "", false
	}
	rx := regexp.MustCompile(`^default via (\d+\.\d+\.\d+\.\d+)`) // 'default via 192.168.0.1 dev eth0... '
	if m := rx.FindSubmatch(out); len(m) == 2 {
		return string(m[1]), true
	}
	return "", false
}
