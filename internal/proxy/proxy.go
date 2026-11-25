package proxy

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

// LoadBalancer menerima trafik HTTP masuk dan meneruskannya ke backend sehat sesuai Strategy.
type LoadBalancer struct {
	backends []*backend.Backend
	strategy strategy.Strategy
	logger   *slog.Logger
}

// New membuat instance LoadBalancer baru dengan validasi dependensi eksplisit.
func New(backends []*backend.Backend, strat strategy.Strategy, logger *slog.Logger) (*LoadBalancer, error) {
	if strat == nil {
		return nil, errors.New("strategy cannot be nil")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	copied := make([]*backend.Backend, len(backends))
	copy(copied, backends)

	return &LoadBalancer{
		backends: copied,
		strategy: strat,
		logger:   logger,
	}, nil
}

// ServeHTTP memilih backend menggunakan Strategy lalu meneruskan request via ReverseProxy.
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	target := lb.strategy.Next(lb.backends, r)
	if target == nil {
		lb.logger.Warn("no healthy backends available",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
		)
		http.Error(w, "Service Unavailable: no healthy backends", http.StatusServiceUnavailable)
		return
	}

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	target.ServeHTTP(rec, r)

	lb.logger.Info("proxied request",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("remote_addr", r.RemoteAddr),
		slog.String("backend", target.URLString()),
		slog.Int("status", rec.status),
		slog.Duration("duration", time.Since(start)),
	)
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
