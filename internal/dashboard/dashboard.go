package dashboard

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

//go:embed web/index.html
var indexHTML []byte

// DefaultStreamInterval adalah interval pengiriman event JSON SSE ke browser (1 detik).
const DefaultStreamInterval = 1 * time.Second

// Snapshot merepresentasikan payload JSON real-time yang dikirim ke client SSE tiap 1 detik.
type Snapshot struct {
	Timestamp string            `json:"timestamp"`
	Strategy  string            `json:"strategy"`
	UptimeSec float64           `json:"uptime_sec"`
	Totals    ClusterTotals     `json:"totals"`
	Backends  []BackendSnapshot `json:"backends"`
}

// ClusterTotals merangkum metrik gabungan seluruh backend.
type ClusterTotals struct {
	HealthyBackends   int     `json:"healthy_backends"`
	TotalBackends     int     `json:"total_backends"`
	ActiveConnections int64   `json:"active_connections"`
	TotalRequests     uint64  `json:"total_requests"`
	TotalErrors       uint64  `json:"total_errors"`
	RequestsPerSec    float64 `json:"requests_per_sec"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	RecentLatencyMs   float64 `json:"recent_latency_ms"`
}

// BackendSnapshot merepresentasikan status dan metrik satu backend di dalam JSON SSE.
type BackendSnapshot struct {
	URL               string  `json:"url"`
	Weight            int     `json:"weight"`
	Alive             bool    `json:"alive"`
	Status            string  `json:"status"`
	ActiveConnections int64   `json:"active_connections"`
	TotalRequests     uint64  `json:"total_requests"`
	TotalErrors       uint64  `json:"total_errors"`
	RequestsPerSec    float64 `json:"requests_per_sec"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	RecentLatencyMs   float64 `json:"recent_latency_ms"`
}

type samplePoint struct {
	requests  uint64
	latencyNs uint64
}

// Server menyediakan halaman dashboard embed, stream SSE (/events), JSON (/api/stats),
// serta eksposisi Prometheus sederhana (/metrics).
type Server struct {
	backends     []*backend.Backend
	strategyName string
	interval     time.Duration
	startedAt    time.Time
	logger       *slog.Logger
	mux          *http.ServeMux

	sampleMu   sync.Mutex
	lastSample time.Time
	prevStats  map[string]samplePoint
	latest     atomic.Pointer[Snapshot]

	stopOnce sync.Once
	stopCh   chan struct{}
}

// New membuat instance Server dashboard baru.
func New(backends []*backend.Backend, strategyName string, interval time.Duration, logger *slog.Logger) *Server {
	if interval <= 0 {
		interval = DefaultStreamInterval
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	now := time.Now()
	copied := make([]*backend.Backend, len(backends))
	copy(copied, backends)

	prev := make(map[string]samplePoint, len(copied))
	for _, b := range copied {
		st := b.Stats()
		prev[st.URL] = samplePoint{
			requests:  st.TotalRequests,
			latencyNs: st.TotalLatencyNs,
		}
	}

	s := &Server{
		backends:     copied,
		strategyName: strategyName,
		interval:     interval,
		startedAt:    now,
		logger:       logger,
		mux:          http.NewServeMux(),
		lastSample:   now,
		prevStats:    prev,
		stopCh:       make(chan struct{}),
	}

	initial := s.computeSnapshot(now)
	s.latest.Store(&initial)

	s.mux.HandleFunc("GET /", s.handleIndex)
	s.mux.HandleFunc("GET /events", s.handleSSE)
	s.mux.HandleFunc("GET /api/stats", s.handleStatsJSON)
	s.mux.HandleFunc("GET /metrics", s.handlePrometheusMetrics)

	return s
}

// Start menjalankan kalkulasi laju RPS & latensi tiap interval hingga ctx dibatalkan,
// serta menutup channel stopCh agar koneksi SSE aktif langsung keluar saat graceful shutdown.
func (s *Server) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.Stop()
			return
		case now := <-ticker.C:
			snap := s.computeSnapshot(now)
			s.latest.Store(&snap)
		}
	}
}

// Stop memberi sinyal penghentian ke seluruh koneksi SSE agar http.Server.Shutdown tidak tertahan.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// ServeHTTP meneruskan request ke router internal dashboard.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// CurrentSnapshot mengembalikan snapshot terbaru secara lock-free.
func (s *Server) CurrentSnapshot() Snapshot {
	if snap := s.latest.Load(); snap != nil {
		return *snap
	}
	return s.computeSnapshot(time.Now())
}

func (s *Server) computeSnapshot(now time.Time) Snapshot {
	s.sampleMu.Lock()
	defer s.sampleMu.Unlock()

	dt := now.Sub(s.lastSample).Seconds()
	s.lastSample = now

	backendSnaps := make([]BackendSnapshot, 0, len(s.backends))
	var totals ClusterTotals
	totals.TotalBackends = len(s.backends)

	var clusterLatencyNs uint64
	var windowDeltaReqs uint64
	var windowDeltaLatencyNs uint64

	for _, b := range s.backends {
		st := b.Stats()
		prev := s.prevStats[st.URL]

		var deltaReq uint64
		if st.TotalRequests >= prev.requests {
			deltaReq = st.TotalRequests - prev.requests
		}
		var deltaLatNs uint64
		if st.TotalLatencyNs >= prev.latencyNs {
			deltaLatNs = st.TotalLatencyNs - prev.latencyNs
		}

		s.prevStats[st.URL] = samplePoint{
			requests:  st.TotalRequests,
			latencyNs: st.TotalLatencyNs,
		}

		var rps float64
		if dt > 0 {
			rps = round2(float64(deltaReq) / dt)
		}

		var avgLatencyMs float64
		if st.TotalRequests > 0 {
			avgLatencyMs = round2(float64(st.TotalLatencyNs) / float64(st.TotalRequests) / 1e6)
		}

		var recentLatencyMs float64
		if deltaReq > 0 {
			recentLatencyMs = round2(float64(deltaLatNs) / float64(deltaReq) / 1e6)
		}

		statusStr := "unhealthy"
		if st.Alive {
			statusStr = "healthy"
			totals.HealthyBackends++
		}

		totals.ActiveConnections += st.ActiveConnections
		totals.TotalRequests += st.TotalRequests
		totals.TotalErrors += st.TotalErrors
		totals.RequestsPerSec += rps
		clusterLatencyNs += st.TotalLatencyNs
		windowDeltaReqs += deltaReq
		windowDeltaLatencyNs += deltaLatNs

		backendSnaps = append(backendSnaps, BackendSnapshot{
			URL:               st.URL,
			Weight:            st.Weight,
			Alive:             st.Alive,
			Status:            statusStr,
			ActiveConnections: st.ActiveConnections,
			TotalRequests:     st.TotalRequests,
			TotalErrors:       st.TotalErrors,
			RequestsPerSec:    rps,
			AvgLatencyMs:      avgLatencyMs,
			RecentLatencyMs:   recentLatencyMs,
		})
	}

	totals.RequestsPerSec = round2(totals.RequestsPerSec)
	if totals.TotalRequests > 0 {
		totals.AvgLatencyMs = round2(float64(clusterLatencyNs) / float64(totals.TotalRequests) / 1e6)
	}
	if windowDeltaReqs > 0 {
		totals.RecentLatencyMs = round2(float64(windowDeltaLatencyNs) / float64(windowDeltaReqs) / 1e6)
	}

	return Snapshot{
		Timestamp: now.UTC().Format(time.RFC3339),
		Strategy:  s.strategyName,
		UptimeSec: round2(now.Sub(s.startedAt).Seconds()),
		Totals:    totals,
		Backends:  backendSnaps,
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(indexHTML)
}

func (s *Server) handleStatsJSON(w http.ResponseWriter, _ *http.Request) {
	snap := s.CurrentSnapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(snap)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	if err := s.writeSSEFrame(w, flusher, s.CurrentSnapshot()); err != nil {
		return
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.writeSSEFrame(w, flusher, s.CurrentSnapshot()); err != nil {
				return
			}
		}
	}
}

func (s *Server) writeSSEFrame(w http.ResponseWriter, flusher http.Flusher, snap Snapshot) error {
	payload, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, _ *http.Request) {
	var buf bytes.Buffer
	uptime := time.Since(s.startedAt).Seconds()

	buf.WriteString("# HELP lb_uptime_seconds Uptime of the HTTP load balancer in seconds.\n")
	buf.WriteString("# TYPE lb_uptime_seconds gauge\n")
	fmt.Fprintf(&buf, "lb_uptime_seconds %.3f\n\n", uptime)

	buf.WriteString("# HELP lb_backend_up Health status of the backend (1 = healthy, 0 = unhealthy).\n")
	buf.WriteString("# TYPE lb_backend_up gauge\n")
	for _, b := range s.backends {
		st := b.Stats()
		val := 0
		if st.Alive {
			val = 1
		}
		fmt.Fprintf(&buf, "lb_backend_up{backend=%q} %d\n", st.URL, val)
	}
	buf.WriteByte('\n')

	buf.WriteString("# HELP lb_backend_weight Configured routing weight of the backend.\n")
	buf.WriteString("# TYPE lb_backend_weight gauge\n")
	for _, b := range s.backends {
		st := b.Stats()
		fmt.Fprintf(&buf, "lb_backend_weight{backend=%q} %d\n", st.URL, st.Weight)
	}
	buf.WriteByte('\n')

	buf.WriteString("# HELP lb_backend_active_connections Current number of in-flight HTTP requests to the backend.\n")
	buf.WriteString("# TYPE lb_backend_active_connections gauge\n")
	for _, b := range s.backends {
		st := b.Stats()
		fmt.Fprintf(&buf, "lb_backend_active_connections{backend=%q} %d\n", st.URL, st.ActiveConnections)
	}
	buf.WriteByte('\n')

	buf.WriteString("# HELP lb_backend_requests_total Total number of HTTP requests forwarded to the backend.\n")
	buf.WriteString("# TYPE lb_backend_requests_total counter\n")
	for _, b := range s.backends {
		st := b.Stats()
		fmt.Fprintf(&buf, "lb_backend_requests_total{backend=%q} %d\n", st.URL, st.TotalRequests)
	}
	buf.WriteByte('\n')

	buf.WriteString("# HELP lb_backend_errors_total Total number of failed or 5xx HTTP requests on the backend.\n")
	buf.WriteString("# TYPE lb_backend_errors_total counter\n")
	for _, b := range s.backends {
		st := b.Stats()
		fmt.Fprintf(&buf, "lb_backend_errors_total{backend=%q} %d\n", st.URL, st.TotalErrors)
	}
	buf.WriteByte('\n')

	buf.WriteString("# HELP lb_backend_request_duration_seconds_sum Cumulative upstream request latency in seconds.\n")
	buf.WriteString("# TYPE lb_backend_request_duration_seconds_sum counter\n")
	for _, b := range s.backends {
		st := b.Stats()
		sec := float64(st.TotalLatencyNs) / 1e9
		fmt.Fprintf(&buf, "lb_backend_request_duration_seconds_sum{backend=%q} %.6f\n", st.URL, sec)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
