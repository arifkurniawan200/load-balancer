package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/config"
	"github.com/arifkurniawan200/load-balancer/internal/health"
	"github.com/arifkurniawan200/load-balancer/internal/proxy"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := parseFlagsAndConfig(os.Args[1:])
	if err != nil {
		logger.Error("failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, nil); err != nil {
		logger.Error("load balancer exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func parseFlagsAndConfig(args []string) (*config.Config, error) {
	fs := flag.NewFlagSet("lb", flag.ContinueOnError)
	configPath := fs.String("config", "config.yaml", "path to YAML configuration file")
	listenOverride := fs.String("listen", "", "optional override for listen address")
	strategyOverride := fs.String("strategy", "", "optional override for load balancing strategy")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return nil, err
	}

	overridden := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "listen":
			cfg.Listen = *listenOverride
			overridden = true
		case "strategy":
			cfg.Strategy = *strategyOverride
			overridden = true
		}
	})

	if overridden {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, readyCh chan<- string) error {
	backends := make([]*backend.Backend, 0, len(cfg.Backends))
	for _, bCfg := range cfg.Backends {
		b, err := backend.New(bCfg.URL, bCfg.Weight)
		if err != nil {
			return fmt.Errorf("init backend %q: %w", bCfg.URL, err)
		}
		backends = append(backends, b)
	}

	strat, err := strategy.New(cfg.Strategy)
	if err != nil {
		return err
	}

	checker, err := health.New(backends, health.Config{
		Path:          cfg.HealthCheck.Path,
		Interval:      cfg.HealthCheck.Interval,
		Timeout:       cfg.HealthCheck.Timeout,
		FailThreshold: cfg.HealthCheck.FailThreshold,
		RiseThreshold: cfg.HealthCheck.RiseThreshold,
	}, logger)
	if err != nil {
		return fmt.Errorf("init health checker: %w", err)
	}

	lb, err := proxy.New(backends, strat, logger)
	if err != nil {
		return fmt.Errorf("init proxy: %w", err)
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Listen, err)
	}

	srv := &http.Server{
		Handler:           lb,
		ReadHeaderTimeout: 5 * time.Second,
	}

	checkerCtx, cancelChecker := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		checker.Start(checkerCtx)
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP load balancer",
			slog.String("listen", ln.Addr().String()),
			slog.String("strategy", cfg.Strategy),
			slog.Int("backends", len(backends)),
		)
		if readyCh != nil {
			readyCh <- ln.Addr().String()
		}
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
		close(errCh)
	}()

	select {
	case serveErr := <-errCh:
		cancelChecker()
		wg.Wait()
		return serveErr

	case <-ctx.Done():
		logger.Info("shutdown signal received; draining in-flight requests",
			slog.Duration("timeout", cfg.ShutdownTimeout),
		)
		cancelChecker()

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancelShutdown()

		shutdownErr := srv.Shutdown(shutdownCtx)
		wg.Wait()

		if shutdownErr != nil {
			return fmt.Errorf("graceful shutdown failed: %w", shutdownErr)
		}
		logger.Info("load balancer shut down gracefully")
		return nil
	}
}
