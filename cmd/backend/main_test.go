package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseArgsFlexibleOrder(t *testing.T) {
	opts, err := parseArgs([]string{"9001", "-delay", "50ms", "-fail"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if opts.port != "9001" || opts.name != "backend-9001" || opts.delay != 50*time.Millisecond || !opts.fail {
		t.Fatalf("unexpected opts: %+v", opts)
	}

	opts2, err := parseArgs([]string{"-delay", "10ms", "-name", "api-1", "9002"})
	if err != nil {
		t.Fatalf("parseArgs flags-first failed: %v", err)
	}
	if opts2.port != "9002" || opts2.name != "api-1" || opts2.delay != 10*time.Millisecond || opts2.fail {
		t.Fatalf("unexpected opts2: %+v", opts2)
	}
}

func TestBackendHandlerHealthyAndFailing(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	healthySrv := httptest.NewServer(newBackendHandler(backendOptions{
		port:  "9001",
		name:  "backend-9001",
		delay: 10 * time.Millisecond,
		fail:  false,
	}, discardLogger))
	defer healthySrv.Close()

	resp, err := http.Get(healthySrv.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "backend-9001") {
		t.Fatalf("expected 200 with instance name, got %d %q", resp.StatusCode, string(body))
	}

	hResp, err := http.Get(healthySrv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	hResp.Body.Close()
	if hResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on /health, got %d", hResp.StatusCode)
	}

	failingSrv := httptest.NewServer(newBackendHandler(backendOptions{
		port: "9002",
		name: "backend-9002",
		fail: true,
	}, discardLogger))
	defer failingSrv.Close()

	fResp, err := http.Get(failingSrv.URL + "/health")
	if err != nil {
		t.Fatalf("GET failing /health error: %v", err)
	}
	fResp.Body.Close()
	if fResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 on failing /health, got %d", fResp.StatusCode)
	}
}
