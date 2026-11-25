package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

const (
	// DefaultMaxRetries adalah batas maksimal retry pasif ke backend lain saat terjadi error proxy.
	DefaultMaxRetries = 2
	// maxRetryBodyBytes membatasi ukuran body request yang di-buffer di memori agar aman di-replay saat retry.
	maxRetryBodyBytes = 1 << 20 // 1 MiB
)

type retryContextKey struct{}

type requestAttemptState struct {
	origReq              *http.Request
	origCtx              context.Context
	recorder             *statusRecorder
	bodyBytes            []byte
	canRetryBody         bool
	retries              int
	finalBackend         string
	finishCurrentAttempt func(recordMetrics bool, transportErr bool)
}

// LoadBalancer menerima trafik HTTP masuk dan meneruskannya ke backend sehat sesuai Strategy.
type LoadBalancer struct {
	backends   []*backend.Backend
	proxies    map[*backend.Backend]*httputil.ReverseProxy
	strategy   strategy.Strategy
	maxRetries int
	logger     *slog.Logger
}

// New membuat instance LoadBalancer baru dengan validasi dependensi eksplisit serta ErrorHandler pasif.
func New(backends []*backend.Backend, strat strategy.Strategy, logger *slog.Logger) (*LoadBalancer, error) {
	if strat == nil {
		return nil, errors.New("strategy cannot be nil")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	copied := make([]*backend.Backend, len(backends))
	copy(copied, backends)

	lb := &LoadBalancer{
		backends:   copied,
		proxies:    make(map[*backend.Backend]*httputil.ReverseProxy, len(copied)),
		strategy:   strat,
		maxRetries: DefaultMaxRetries,
		logger:     logger,
	}

	for _, b := range copied {
		lb.proxies[b] = b.NewReverseProxy(lb.makeErrorHandler(b))
	}

	return lb, nil
}

// ServeHTTP memilih backend menggunakan Strategy lalu meneruskan request via ReverseProxy dengan retry pasif.
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	bodyBytes, canRetryBody, err := prepareRequestBody(r)
	if err != nil {
		lb.logger.Warn("failed reading client request body",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("error", err.Error()),
		)
		http.Error(w, "Bad Request: failed reading request body", http.StatusBadRequest)
		return
	}

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	state := &requestAttemptState{
		origReq:      r,
		origCtx:      r.Context(),
		recorder:     rec,
		bodyBytes:    bodyBytes,
		canRetryBody: canRetryBody,
	}

	lb.dispatchAttempt(state)

	if state.finalBackend != "" {
		lb.logger.Info("proxied request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote_addr", r.RemoteAddr),
			slog.String("backend", state.finalBackend),
			slog.Int("retries", state.retries),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	}
}

func (lb *LoadBalancer) dispatchAttempt(state *requestAttemptState) {
	ctx := context.WithValue(state.origCtx, retryContextKey{}, state)
	attemptReq := state.origReq.Clone(ctx)

	if state.canRetryBody {
		if len(state.bodyBytes) > 0 {
			attemptReq.Body = io.NopCloser(bytes.NewReader(state.bodyBytes))
			attemptReq.ContentLength = int64(len(state.bodyBytes))
		} else {
			attemptReq.Body = http.NoBody
		}
	} else {
		attemptReq.Body = state.origReq.Body
	}

	target := lb.strategy.Next(lb.backends, attemptReq)
	if target == nil {
		if state.retries == 0 {
			lb.logger.Warn("no healthy backends available",
				slog.String("method", attemptReq.Method),
				slog.String("path", attemptReq.URL.Path),
				slog.String("remote_addr", attemptReq.RemoteAddr),
			)
			http.Error(state.recorder, "Service Unavailable: no healthy backends", http.StatusServiceUnavailable)
			return
		}

		lb.logger.Error("no healthy backends remaining for retry",
			slog.String("method", attemptReq.Method),
			slog.String("path", attemptReq.URL.Path),
			slog.Int("retries", state.retries),
		)
		http.Error(state.recorder, "Bad Gateway: no healthy backends remaining", http.StatusBadGateway)
		return
	}

	state.finalBackend = target.URLString()
	attemptStart := time.Now()
	target.IncConnections()

	var finishOnce sync.Once
	finish := func(recordMetrics bool, transportErr bool) {
		finishOnce.Do(func() {
			target.DecConnections()
			if recordMetrics {
				isErr := transportErr || state.recorder.status >= http.StatusInternalServerError
				target.RecordRequest(time.Since(attemptStart), isErr)
			}
		})
	}
	state.finishCurrentAttempt = finish
	defer finish(true, false)

	if rp, ok := lb.proxies[target]; ok {
		rp.ServeHTTP(state.recorder, attemptReq)
		return
	}
	target.ServeHTTP(state.recorder, attemptReq)
}

func (lb *LoadBalancer) makeErrorHandler(b *backend.Backend) func(http.ResponseWriter, *http.Request, error) {
	return func(rw http.ResponseWriter, req *http.Request, err error) {
		state, _ := req.Context().Value(retryContextKey{}).(*requestAttemptState)

		// Jika client sendiri membatalkan request, jangan tandai backend bermasalah maupun error upstream.
		if state != nil && state.origCtx.Err() != nil {
			if state.finishCurrentAttempt != nil {
				state.finishCurrentAttempt(false, false)
			}
			lb.logger.Debug("client canceled proxied request",
				slog.String("backend", b.URLString()),
				slog.String("error", err.Error()),
			)
			return
		}

		if state != nil && state.finishCurrentAttempt != nil {
			state.finishCurrentAttempt(true, true)
		}

		if b.MarkUnhealthy() {
			lb.logger.Warn("passive health check marked backend unhealthy",
				slog.String("backend", b.URLString()),
				slog.String("error", err.Error()),
			)
		}

		// Jika header response sudah terkirim ke client, tidak bisa me-retry.
		if state == nil || state.recorder.wroteHeader {
			return
		}

		if !state.canRetryBody || state.retries >= lb.maxRetries {
			lb.logger.Error("upstream proxy failed; retries exhausted",
				slog.String("failed_backend", b.URLString()),
				slog.Int("retries", state.retries),
				slog.String("error", err.Error()),
			)
			http.Error(rw, "Bad Gateway: upstream unavailable", http.StatusBadGateway)
			return
		}

		state.retries++
		lb.logger.Warn("retrying request on another backend",
			slog.String("failed_backend", b.URLString()),
			slog.Int("retry", state.retries),
			slog.Int("max_retries", lb.maxRetries),
			slog.String("error", err.Error()),
		)
		lb.dispatchAttempt(state)
	}
}

func prepareRequestBody(r *http.Request) (bodyBytes []byte, canRetry bool, err error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, true, nil
	}
	if r.ContentLength > maxRetryBodyBytes {
		return nil, false, nil
	}

	buf, err := io.ReadAll(io.LimitReader(r.Body, maxRetryBodyBytes+1))
	if err != nil {
		_ = r.Body.Close()
		return nil, false, err
	}

	if len(buf) > maxRetryBodyBytes {
		r.Body = &combinedReadCloser{
			Reader: io.MultiReader(bytes.NewReader(buf), r.Body),
			Closer: r.Body,
		}
		return nil, false, nil
	}

	_ = r.Body.Close()
	return buf, true, nil
}

type combinedReadCloser struct {
	io.Reader
	io.Closer
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
