// The gocacheproxy command runs an HTTP reverse proxy that routes cache
// requests to backend gocached servers using consistent hashing.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/GetStream/go-tool-cache/gocacheproxy"
)

var (
	listenAddr       = flag.String("listen", ":31365", "listen address")
	backendsFlag     = flag.String("backends", "", "comma-separated backend addresses (host:port)")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	timeout          = flag.Duration("timeout", 3*time.Second, "per-attempt timeout for proxied PUT/GET to a backend")
	retries          = flag.Int("retries", 2, "number of PUT retries after the initial attempt (total attempts = 1 + retries)")
	maxInflightBytes = flag.Int64("max-inflight-bytes", 0, "soft cap on buffered PUT body bytes; PUTs above this are dropped silently. 0 = auto-detect 80% of container memory limit")
)

// normalizeURL ensures addr has an http:// scheme.
func normalizeURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return addr
}

func main() {
	flag.Parse()

	if *backendsFlag == "" {
		log.Fatal("--backends is required")
	}

	addrs := strings.Split(*backendsFlag, ",")
	backends := make([]*gocacheproxy.Backend, len(addrs))
	for i, addr := range addrs {
		backends[i] = &gocacheproxy.Backend{URL: normalizeURL(addr)}
	}

	maxInflight := *maxInflightBytes
	switch {
	case maxInflight > 0:
		log.Printf("inflight cap: %d MiB (flag)", maxInflight>>20)
	default:
		if lim, err := gocacheproxy.DetectMemoryLimit(); err == nil {
			maxInflight = lim * 80 / 100
			log.Printf("inflight cap: 80%% of %d MiB container limit = %d MiB",
				lim>>20, maxInflight>>20)
		} else {
			maxInflight = 512 << 20
			log.Printf("inflight cap: %d MiB (no cgroup info: %v)", maxInflight>>20, err)
		}
	}

	p := &gocacheproxy.Proxy{
		Backends:         backends,
		Client:           &http.Client{Timeout: *timeout},
		Retries:          *retries,
		MaxInflightBytes: maxInflight,
		Verbose:          *verbose,
	}

	go p.HealthCheck()
	go p.LogStats(60 * time.Second)

	log.Printf("gocacheproxy listening on %s with %d backends", *listenAddr, len(backends))
	log.Fatal(http.ListenAndServe(*listenAddr, p))
}
