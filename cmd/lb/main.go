package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/health"
	"github.com/arifkurniawan200/load-balancer/internal/proxy"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	listenAddr := flag.String("listen", ":8080", "HTTP listen address for load balancer")
	backendsCSV := flag.String("backends", "http://localhost:9001,http://localhost:9002,http://localhost:9003", "comma-separated list of backend URLs")
	healthPath := flag.String("health-path", "/health", "HTTP path for active health checks")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "interval between active health checks")
	healthTimeout := flag.Duration("health-timeout", 2*time.Second, "timeout per active health check request")
	flag.Parse()

	backends, err := parseBackends(*backendsCSV)
	if err != nil {
		logger.Error("failed to initialize backends", slog.String("error", err.Error()))
		os.Exit(1)
	}

	hcCfg := health.DefaultConfig()
	hcCfg.Path = *healthPath
	hcCfg.Interval = *healthInterval
	hcCfg.Timeout = *healthTimeout

	checker, err := health.New(backends, hcCfg, logger)
	if err != nil {
		logger.Error("failed to initialize health checker", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go checker.Start(ctx)

	strat := strategy.NewRoundRobin()
	lb, err := proxy.New(backends, strat, logger)
	if err != nil {
		logger.Error("failed to create load balancer", slog.String("error", err.Error()))
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           lb,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("starting HTTP load balancer",
		slog.String("listen", *listenAddr),
		slog.String("strategy", "round_robin"),
		slog.Int("backends", len(backends)),
	)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("load balancer stopped with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func parseBackends(rawCSV string) ([]*backend.Backend, error) {
	parts := strings.Split(rawCSV, ",")
	backends := make([]*backend.Backend, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		b, err := backend.New(trimmed, 1)
		if err != nil {
			return nil, fmt.Errorf("invalid backend %q: %w", trimmed, err)
		}
		backends = append(backends, b)
	}

	if len(backends) == 0 {
		return nil, errors.New("at least one backend URL is required")
	}

	return backends, nil
}
