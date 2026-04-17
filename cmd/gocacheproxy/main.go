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
	listenAddr   = flag.String("listen", ":31365", "listen address")
	backendsFlag = flag.String("backends", "", "comma-separated backend addresses (host:port)")
	verbose      = flag.Bool("verbose", false, "verbose logging")
	timeout      = flag.Duration("timeout", 3*time.Second, "per-attempt timeout for proxied PUT/GET to a backend")
	retries      = flag.Int("retries", 2, "number of PUT retries after the initial attempt (total attempts = 1 + retries)")
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

	p := &gocacheproxy.Proxy{
		Backends: backends,
		Client:   &http.Client{Timeout: *timeout},
		Retries:  *retries,
		Verbose:  *verbose,
	}

	go p.HealthCheck()

	log.Printf("gocacheproxy listening on %s with %d backends", *listenAddr, len(backends))
	log.Fatal(http.ListenAndServe(*listenAddr, p))
}
