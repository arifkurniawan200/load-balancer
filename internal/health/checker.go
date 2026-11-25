package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// Config menyimpan pengaturan active health check.
type Config struct {
	Path          string
	Interval      time.Duration
	Timeout       time.Duration
	FailThreshold int
	RiseThreshold int
}

// DefaultConfig mengembalikan konfigurasi bawaan active health check.
func DefaultConfig() Config {
	return Config{
		Path:          "/health",
		Interval:      5 * time.Second,
		Timeout:       2 * time.Second,
		FailThreshold: backend.DefaultFailThreshold,
		RiseThreshold: backend.DefaultRiseThreshold,
	}
}

// Checker menjalankan active health check periodik ke seluruh backend menggunakan time.Ticker.
type Checker struct {
	backends []*backend.Backend
	cfg      Config
	client   *http.Client
	logger   *slog.Logger
}

// New memvalidasi konfigurasi dan membuat instance Checker baru.
func New(backends []*backend.Backend, cfg Config, logger *slog.Logger) (*Checker, error) {
	if cfg.Path == "" {
		cfg.Path = "/health"
	}
	if !strings.HasPrefix(cfg.Path, "/") {
		cfg.Path = "/" + cfg.Path
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("health check interval must be greater than 0")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("health check timeout must be greater than 0")
	}
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = backend.DefaultFailThreshold
	}
	if cfg.RiseThreshold <= 0 {
		cfg.RiseThreshold = backend.DefaultRiseThreshold
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	copied := make([]*backend.Backend, len(backends))
	copy(copied, backends)

	return &Checker{
		backends: copied,
		cfg:      cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		logger: logger,
	}, nil
}

// Start menjalankan loop pemeriksaan kesehatan periodik hingga ctx dibatalkan.
func (c *Checker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	c.logger.Info("active health checker started",
		slog.String("path", c.cfg.Path),
		slog.Duration("interval", c.cfg.Interval),
		slog.Duration("timeout", c.cfg.Timeout),
		slog.Int("fail_threshold", c.cfg.FailThreshold),
		slog.Int("rise_threshold", c.cfg.RiseThreshold),
	)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("active health checker stopped")
			return
		case <-ticker.C:
			c.CheckAll(ctx)
		}
	}
}

// CheckAll memeriksa seluruh backend secara paralel satu kali putaran.
func (c *Checker) CheckAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range c.backends {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		go func(target *backend.Backend) {
			defer wg.Done()
			c.checkOne(ctx, target)
		}(b)
	}
	wg.Wait()
}

func (c *Checker) checkOne(ctx context.Context, b *backend.Backend) {
	targetURL, err := buildHealthURL(b.URL(), c.cfg.Path)
	if err != nil {
		c.logger.Error("failed to build health check URL",
			slog.String("backend", b.URLString()),
			slog.String("error", err.Error()),
		)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		c.logger.Error("failed to create health check request",
			slog.String("backend", b.URLString()),
			slog.String("error", err.Error()),
		)
		return
	}

	resp, err := c.client.Do(req)
	if err != nil {
		// Jika context utama dibatalkan (misal saat shutdown), jangan hitung sebagai kegagalan backend.
		if ctx.Err() != nil {
			return
		}
		changed, _ := b.RecordHealthCheck(false, c.cfg.FailThreshold, c.cfg.RiseThreshold)
		if changed {
			c.logger.Warn("active health check marked backend unhealthy",
				slog.String("backend", b.URLString()),
				slog.String("error", err.Error()),
			)
		} else {
			c.logger.Debug("active health check failed",
				slog.String("backend", b.URLString()),
				slog.String("error", err.Error()),
			)
		}
		return
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()

	healthy := resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	changed, nowAlive := b.RecordHealthCheck(healthy, c.cfg.FailThreshold, c.cfg.RiseThreshold)
	if !changed {
		if !healthy {
			c.logger.Debug("active health check non-2xx status",
				slog.String("backend", b.URLString()),
				slog.Int("status", resp.StatusCode),
			)
		}
		return
	}

	if nowAlive {
		c.logger.Info("active health check recovered backend to healthy",
			slog.String("backend", b.URLString()),
			slog.Int("status", resp.StatusCode),
		)
	} else {
		c.logger.Warn("active health check marked backend unhealthy",
			slog.String("backend", b.URLString()),
			slog.Int("status", resp.StatusCode),
		)
	}
}

func buildHealthURL(base *url.URL, checkPath string) (string, error) {
	ref, err := url.Parse(checkPath)
	if err != nil {
		return "", fmt.Errorf("parse health path %q: %w", checkPath, err)
	}
	baseCopy := *base
	if baseCopy.Path != "" && baseCopy.Path != "/" {
		baseCopy.Path = strings.TrimRight(baseCopy.Path, "/") + "/" + strings.TrimLeft(ref.Path, "/")
	} else {
		baseCopy.Path = ref.Path
	}
	baseCopy.RawQuery = ref.RawQuery
	return baseCopy.String(), nil
}
