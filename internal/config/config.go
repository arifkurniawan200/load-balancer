package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

const (
	DefaultListenAddr      = ":8080"
	DefaultDashboardAddr   = ":8081"
	DefaultHealthPath      = "/health"
	DefaultHealthInterval  = 5 * time.Second
	DefaultHealthTimeout   = 2 * time.Second
	DefaultShutdownTimeout = 10 * time.Second
)

// Config merepresentasikan konfigurasi utama HTTP Load Balancer dari file YAML.
type Config struct {
	Listen          string            `yaml:"listen"`
	Dashboard       string            `yaml:"dashboard"`
	Strategy        string            `yaml:"strategy"`
	ShutdownTimeout time.Duration     `yaml:"shutdown_timeout"`
	HealthCheck     HealthCheckConfig `yaml:"health_check"`
	Backends        []BackendConfig   `yaml:"backends"`
}

// HealthCheckConfig menyimpan konfigurasi pemeriksaan kesehatan aktif.
type HealthCheckConfig struct {
	Path          string        `yaml:"path"`
	Interval      time.Duration `yaml:"interval"`
	Timeout       time.Duration `yaml:"timeout"`
	FailThreshold int           `yaml:"fail_threshold"`
	RiseThreshold int           `yaml:"rise_threshold"`
}

// BackendConfig menyimpan URL dan bobot satu server backend di dalam file YAML.
type BackendConfig struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

// Load membaca, mem-parse, mengisi nilai default, dan memvalidasi file konfigurasi YAML.
func Load(path string) (*Config, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("config file path cannot be empty")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	return Parse(raw)
}

// Parse mem-parse byte YAML secara ketat (menolak field asing), mengisi default, dan memvalidasi isi.
func Parse(data []byte) (*Config, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("config content cannot be empty")
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode yaml config: %w", err)
	}

	if err := cfg.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) applyDefaultsAndValidate() error {
	// Periksa nilai negatif sebelum menerapkan nilai default pada field kosong/nol.
	if c.ShutdownTimeout < 0 {
		return errors.New("shutdown_timeout cannot be negative")
	}
	if c.HealthCheck.Interval < 0 {
		return errors.New("health_check.interval cannot be negative")
	}
	if c.HealthCheck.Timeout < 0 {
		return errors.New("health_check.timeout cannot be negative")
	}
	if c.HealthCheck.FailThreshold < 0 {
		return errors.New("health_check.fail_threshold cannot be negative")
	}
	if c.HealthCheck.RiseThreshold < 0 {
		return errors.New("health_check.rise_threshold cannot be negative")
	}

	c.Listen = strings.TrimSpace(c.Listen)
	if c.Listen == "" {
		c.Listen = DefaultListenAddr
	}

	c.Dashboard = strings.TrimSpace(c.Dashboard)
	if c.Dashboard == "" {
		c.Dashboard = DefaultDashboardAddr
	}

	if c.Listen == c.Dashboard {
		return fmt.Errorf("listen (%s) and dashboard (%s) addresses must be different", c.Listen, c.Dashboard)
	}

	c.Strategy = strings.ToLower(strings.TrimSpace(c.Strategy))
	if c.Strategy == "" {
		c.Strategy = strategy.NameRoundRobin
	}
	if _, err := strategy.New(c.Strategy); err != nil {
		return err
	}

	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}

	c.HealthCheck.Path = strings.TrimSpace(c.HealthCheck.Path)
	if c.HealthCheck.Path == "" {
		c.HealthCheck.Path = DefaultHealthPath
	}
	if !strings.HasPrefix(c.HealthCheck.Path, "/") {
		c.HealthCheck.Path = "/" + c.HealthCheck.Path
	}
	if c.HealthCheck.Interval == 0 {
		c.HealthCheck.Interval = DefaultHealthInterval
	}
	if c.HealthCheck.Timeout == 0 {
		c.HealthCheck.Timeout = DefaultHealthTimeout
	}
	if c.HealthCheck.FailThreshold == 0 {
		c.HealthCheck.FailThreshold = backend.DefaultFailThreshold
	}
	if c.HealthCheck.RiseThreshold == 0 {
		c.HealthCheck.RiseThreshold = backend.DefaultRiseThreshold
	}

	if len(c.Backends) == 0 {
		return errors.New("at least one backend must be configured in backends")
	}

	seen := make(map[string]struct{}, len(c.Backends))
	for i := range c.Backends {
		b := &c.Backends[i]
		b.URL = strings.TrimSpace(b.URL)
		if b.URL == "" {
			return fmt.Errorf("backends[%d].url cannot be empty", i)
		}
		if b.Weight < 0 {
			return fmt.Errorf("backends[%d].weight cannot be negative", i)
		}
		if b.Weight == 0 {
			b.Weight = 1
		}

		parsed, err := url.Parse(b.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("backends[%d].url %q must be a valid http/https URL", i, b.URL)
		}

		normalizedURL := parsed.String()
		if _, exists := seen[normalizedURL]; exists {
			return fmt.Errorf("duplicate backend url %q", normalizedURL)
		}
		seen[normalizedURL] = struct{}{}
	}

	return nil
}

// Validate memeriksa kembali konsistensi struct Config apabila dimodifikasi lewat flag CLI.
func (c *Config) Validate() error {
	return c.applyDefaultsAndValidate()
}
