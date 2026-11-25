package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseValidAndDefaults(t *testing.T) {
	raw := []byte(`
listen: ":8080"
dashboard: ":8081"
strategy: least_conn
health_check:
  path: /health
  interval: 5s
  timeout: 2s
backends:
  - url: http://localhost:9001
    weight: 1
  - url: http://localhost:9002
`)

	cfg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if cfg.Listen != ":8080" || cfg.Dashboard != ":8081" || cfg.Strategy != "least_conn" {
		t.Fatalf("unexpected top-level config: %+v", cfg)
	}
	if cfg.HealthCheck.Path != "/health" || cfg.HealthCheck.Interval != 5*time.Second || cfg.HealthCheck.Timeout != 2*time.Second {
		t.Fatalf("unexpected health_check config: %+v", cfg.HealthCheck)
	}
	if len(cfg.Backends) != 2 || cfg.Backends[1].Weight != 1 {
		t.Fatalf("expected default weight=1 on 2nd backend, got %+v", cfg.Backends)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Fatalf("expected default shutdown timeout %v, got %v", DefaultShutdownTimeout, cfg.ShutdownTimeout)
	}
}

func TestParseValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "empty file",
			yaml: "",
		},
		{
			name: "unknown field typo",
			yaml: `
listen: ":8080"
strategyy: round_robin
backends:
  - url: http://localhost:9001
`,
		},
		{
			name: "unknown strategy",
			yaml: `
strategy: magic_hash
backends:
  - url: http://localhost:9001
`,
		},
		{
			name: "same listen and dashboard port",
			yaml: `
listen: ":8080"
dashboard: ":8080"
backends:
  - url: http://localhost:9001
`,
		},
		{
			name: "no backends",
			yaml: `
listen: ":8080"
backends: []
`,
		},
		{
			name: "duplicate backend",
			yaml: `
backends:
  - url: http://localhost:9001
  - url: http://localhost:9001
`,
		},
		{
			name: "negative weight",
			yaml: `
backends:
  - url: http://localhost:9001
    weight: -2
`,
		},
		{
			name: "negative interval",
			yaml: `
health_check:
  interval: -1s
backends:
  - url: http://localhost:9001
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected validation error for case %q, got nil", tc.name)
			}
		})
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
listen: ":18080"
dashboard: ":18081"
strategy: consistent_hash
backends:
  - url: http://localhost:9001
    weight: 2
`)
	if err := os.WriteFile(cfgPath, content, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Strategy != "consistent_hash" || cfg.Backends[0].Weight != 2 {
		t.Fatalf("unexpected loaded config: %+v", cfg)
	}
}
