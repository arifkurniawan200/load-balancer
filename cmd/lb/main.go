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
	"github.com/arifkurniawan200/load-balancer/internal/dashboard"
	"github.com/arifkurniawan200/load-balancer/internal/health"
	"github.com/arifkurniawan200/load-balancer/internal/proxy"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

type boundAddresses struct {
	proxyAddr     string
	dashboardAddr string
}

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
	dashboardOverride := fs.String("dashboard", "", "optional override for dashboard address")
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
		case "dashboard":
			cfg.Dashboard = *dashboardOverride
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

func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, readyCh chan<- boundAddresses) error {
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

	dashHandler := dashboard.New(backends, cfg.Strategy, dashboard.DefaultStreamInterval, logger)

	proxyLn, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen proxy on %s: %w", cfg.Listen, err)
	}

	dashLn, err := net.Listen("tcp", cfg.Dashboard)
	if err != nil {
		_ = proxyLn.Close()
		return fmt.Errorf("listen dashboard on %s: %w", cfg.Dashboard, err)
	}

	proxySrv := &http.Server{
		Handler:           lb,
		ReadHeaderTimeout: 5 * time.Second,
	}

	dashSrv := &http.Server{
		Handler:           dashHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	bgCtx, cancelBg := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		checker.Start(bgCtx)
	}()
	go func() {
		defer wg.Done()
		dashHandler.Start(bgCtx)
	}()

	errCh := make(chan error, 2)

	go func() {
		logger.Info("starting dashboard server",
			slog.String("dashboard", dashLn.Addr().String()),
		)
		if serveErr := dashSrv.Serve(dashLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("dashboard server: %w", serveErr)
		}
	}()

	go func() {
		logger.Info("starting HTTP load balancer",
			slog.String("listen", proxyLn.Addr().String()),
			slog.String("dashboard", dashLn.Addr().String()),
			slog.String("strategy", cfg.Strategy),
			slog.Int("backends", len(backends)),
		)
		if readyCh != nil {
			readyCh <- boundAddresses{
				proxyAddr:     proxyLn.Addr().String(),
				dashboardAddr: dashLn.Addr().String(),
			}
		}
		if serveErr := proxySrv.Serve(proxyLn); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- fmt.Errorf("proxy server: %w", serveErr)
		}
	}()

	select {
	case serveErr := <-errCh:
		cancelBg()
		dashHandler.Stop()
		_ = dashSrv.Close()
		_ = proxySrv.Close()
		wg.Wait()
		return serveErr

	case <-ctx.Done():
		logger.Info("shutdown signal received; draining in-flight requests",
			slog.Duration("timeout", cfg.ShutdownTimeout),
		)
		cancelBg()
		dashHandler.Stop()

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancelShutdown()

		proxyErr := proxySrv.Shutdown(shutdownCtx)
		dashErr := dashSrv.Shutdown(shutdownCtx)
		wg.Wait()

		if err := errors.Join(proxyErr, dashErr); err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}
		logger.Info("load balancer and dashboard shut down gracefully")
		return nil
	}
}
