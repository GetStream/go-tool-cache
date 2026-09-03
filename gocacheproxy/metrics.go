package gocacheproxy

import (
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

func (p *Proxy) initMetrics() {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	p.getTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_get_total",
		Help: "GET /action results: hit, miss, error, or no_healthy_backend; action_kind is empty when the client omitted Go-Action-Kind",
	}, []string{"result", "action_kind"})
	p.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gocacheproxy_request_duration_seconds",
		Help:    "backend request duration labeled by method, backend URL, and action_kind",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "backend", "action_kind"})
	p.backendUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gocacheproxy_backend_up",
		Help: "1 if the backend is considered healthy",
	}, []string{"backend"})
	p.inflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gocacheproxy_inflight_bytes",
		Help: "buffered PUT body bytes currently reserved against MaxInflightBytes",
	})
	p.putDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_put_dropped_total",
		Help: "PUTs shed because they would exceed MaxInflightBytes",
	}, []string{"action_kind"})
	p.putOK = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_put_ok_total",
		Help: "PUTs that landed on at least one backend",
	}, []string{"action_kind"})
	p.putErr = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_put_error_total",
		Help: "PUTs that did not land on any backend",
	}, []string{"action_kind"})
	p.bytesIn = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_bytes_in",
		Help: "client PUT body bytes accepted by the proxy",
	}, []string{"action_kind"})
	p.bytesOut = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gocacheproxy_bytes_out",
		Help: "client GET response body bytes on cache hits",
	}, []string{"action_kind"})
	p.backendBytesIn = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gocacheproxy_backend_bytes_in",
		Help: "request body bytes sent to backends (replicas and retries count)",
	})
	p.backendBytesOut = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gocacheproxy_backend_bytes_out",
		Help: "response body bytes received from backends",
	})
	reg.MustRegister(
		p.getTotal, p.duration, p.backendUp, p.inflight, p.putDropped,
		p.putOK, p.putErr, p.bytesIn, p.bytesOut, p.backendBytesIn, p.backendBytesOut,
	)
	p.metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorLog: log.Default()})
	for _, b := range p.Backends {
		up := 0.0
		if b.healthy.Load() {
			up = 1
		}
		p.backendUp.WithLabelValues(b.URL).Set(up)
	}
}

// ServeHTTPDebug serves unauthenticated debug endpoints (metrics, pprof).
func (p *Proxy) ServeHTTPDebug(w http.ResponseWriter, r *http.Request) {
	p.ensure()
	switch {
	case r.URL.Path == "/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<h1>gocacheproxy</h1>")
		io.WriteString(w, "<p>See <a href='/metrics'>/metrics</a> for Prometheus metrics.</p>")
		io.WriteString(w, "<p>See <a href='/debug/pprof/'>/debug/pprof/</a> for pprof</p>")
	case r.URL.Path == "/metrics":
		p.metricsHandler.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/profile"):
		pprof.Profile(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/cmdline"):
		pprof.Cmdline(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/symbol"):
		pprof.Symbol(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/trace"):
		pprof.Trace(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/"):
		pprof.Index(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// LogStats periodically logs aggregate counters. Blocks forever; run in a
// goroutine.
func (p *Proxy) LogStats(interval time.Duration) {
	p.ensure()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		h := counterVecSum(p.getTotal, "result", "hit")
		m := counterVecSum(p.getTotal, "result", "miss")
		e := counterVecSum(p.getTotal, "result", "error")
		nh := counterVecSum(p.getTotal, "result", "no_healthy_backend")
		po := counterVecSum(p.putOK, "", "")
		pe := counterVecSum(p.putErr, "", "")
		pd := counterVecSum(p.putDropped, "", "")
		var hitRate float64
		if hm := h + m; hm > 0 {
			hitRate = float64(h) * 100 / float64(hm)
		}
		log.Printf("stats: gets=%d hits=%d misses=%d errors=%d no_healthy=%d hit_rate=%.1f%% | puts ok=%d errors=%d dropped=%d inflight_bytes=%d",
			h+m+e+nh, h, m, e, nh, hitRate, po, pe, pd, p.inflightBytes.Load())
	}
}

func counterValue(c prometheus.Counter) int64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		return 0
	}
	return int64(m.GetCounter().GetValue())
}

// counterVecSum totals CounterVec series. If name is non-empty, only series
// whose label name equals value are included.
func counterVecSum(c prometheus.Collector, name, value string) int64 {
	ch := make(chan prometheus.Metric, 16)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var sum float64
	for met := range ch {
		var m dto.Metric
		if err := met.Write(&m); err != nil || m.Counter == nil {
			continue
		}
		if name != "" {
			match := false
			for _, lp := range m.Label {
				if lp.GetName() == name && lp.GetValue() == value {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		sum += m.GetCounter().GetValue()
	}
	return int64(sum)
}
