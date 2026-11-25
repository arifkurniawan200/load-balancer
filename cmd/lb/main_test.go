package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/config"
)

func TestGracefulShutdownCompletesInFlightRequestAndClosesSSE(t *testing.T) {
	inFlightEntered := make(chan struct{})
	allowFinish := make(chan struct{})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		close(inFlightEntered)
		<-allowFinish
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("completed-before-exit"))
	}))
	defer upstream.Close()

	cfg := &config.Config{
		Listen:          "127.0.0.1:0",
		Dashboard:       "127.0.0.1:0",
		Strategy:        "round_robin",
		ShutdownTimeout: 3 * time.Second,
		HealthCheck: config.HealthCheckConfig{
			Path:          "/health",
			Interval:      1 * time.Second,
			Timeout:       500 * time.Millisecond,
			FailThreshold: 3,
			RiseThreshold: 2,
		},
		Backends: []config.BackendConfig{
			{URL: upstream.URL, Weight: 1},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, triggerShutdown := context.WithCancel(context.Background())
	defer triggerShutdown()

	readyCh := make(chan boundAddresses, 1)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- run(ctx, cfg, logger, readyCh)
	}()

	addrs := <-readyCh

	// Buka koneksi SSE aktif di dashboard untuk memastikan koneksi SSE tidak menahan Shutdown.
	sseResp, err := http.Get("http://" + addrs.dashboardAddr + "/events")
	if err != nil {
		t.Fatalf("GET dashboard /events failed: %v", err)
	}
	defer sseResp.Body.Close()

	sseScan := bufio.NewScanner(sseResp.Body)
	for sseScan.Scan() {
		if strings.HasPrefix(sseScan.Text(), "data: ") {
			break
		}
	}

	type clientResult struct {
		status int
		body   string
		err    error
	}
	resCh := make(chan clientResult, 1)
	go func() {
		resp, err := http.Get("http://" + addrs.proxyAddr + "/slow-job")
		if err != nil {
			resCh <- clientResult{err: err}
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		resCh <- clientResult{status: resp.StatusCode, body: string(b)}
	}()

	// Tunggu hingga request benar-benar sedang diproses di backend
	<-inFlightEntered

	// Picu graceful shutdown saat request proxy in-flight DAN koneksi SSE dashboard terbuka
	triggerShutdown()

	time.Sleep(40 * time.Millisecond)
	close(allowFinish)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("expected in-flight request to succeed during graceful shutdown, got error: %v", res.err)
	}
	if res.status != http.StatusOK || res.body != "completed-before-exit" {
		t.Fatalf("unexpected response status=%d body=%q", res.status, res.body)
	}

	if err := <-runErrCh; err != nil {
		t.Fatalf("expected clean graceful shutdown of proxy and dashboard, got error: %v", err)
	}
}

func TestParseFlagsAndConfigWithOverrides(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "lb.yaml")
	yamlContent := `
listen: ":8080"
dashboard: ":8081"
strategy: round_robin
backends:
  - url: http://localhost:9001
    weight: 1
`
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := parseFlagsAndConfig([]string{"-config", cfgPath, "-listen", ":19080", "-dashboard", ":19081", "-strategy", "least_conn"})
	if err != nil {
		t.Fatalf("parseFlagsAndConfig failed: %v", err)
	}
	if cfg.Listen != ":19080" || cfg.Dashboard != ":19081" || cfg.Strategy != "least_conn" {
		t.Fatalf("expected overrides applied, got listen=%q dashboard=%q strategy=%q", cfg.Listen, cfg.Dashboard, cfg.Strategy)
	}
}
