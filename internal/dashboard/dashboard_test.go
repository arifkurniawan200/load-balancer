package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

func TestDashboardIndexStatsSSEAndPrometheus(t *testing.T) {
	b1, err := backend.New("http://localhost:9001", 2)
	if err != nil {
		t.Fatalf("backend.New b1: %v", err)
	}
	b2, err := backend.New("http://localhost:9002", 1)
	if err != nil {
		t.Fatalf("backend.New b2: %v", err)
	}

	b1.IncConnections()
	b1.RecordRequest(15*time.Millisecond, false)
	b1.RecordRequest(25*time.Millisecond, false)
	b2.SetAlive(false)
	b2.RecordRequest(5*time.Millisecond, true)

	dash := New([]*backend.Backend{b1, b2}, "least_conn", 25*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dash.Start(ctx)

	srv := httptest.NewServer(dash)
	defer srv.Close()

	// 1. Halaman utama embed HTML + Chart.js
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	htmlBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	htmlStr := string(htmlBytes)
	if resp.StatusCode != http.StatusOK || !strings.Contains(htmlStr, "chart.umd.min.js") || !strings.Contains(htmlStr, "EventSource('/events')") {
		t.Fatalf("unexpected index HTML: status=%d len=%d", resp.StatusCode, len(htmlStr))
	}

	// 2. Endpoint SSE /events mengirim frame JSON valid secara berkala dan berhenti saat Stop()
	sseResp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatalf("GET /events failed: %v", err)
	}
	defer sseResp.Body.Close()
	if ct := sseResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}

	// Tambah trafik baru agar tick berikutnya menghitung RPS > 0
	b1.RecordRequest(10*time.Millisecond, false)

	scanner := bufio.NewScanner(sseResp.Body)
	var frames []Snapshot
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var snap Snapshot
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snap); err != nil {
			t.Fatalf("invalid JSON in SSE frame: %v", err)
		}
		frames = append(frames, snap)
		if len(frames) >= 2 {
			break
		}
	}

	if len(frames) < 2 {
		t.Fatalf("expected at least 2 SSE frames, got %d", len(frames))
	}
	latest := frames[len(frames)-1]
	if latest.Strategy != "least_conn" || latest.Totals.HealthyBackends != 1 || latest.Totals.TotalBackends != 2 {
		t.Fatalf("unexpected totals in SSE snapshot: %+v", latest)
	}
	if latest.Totals.ActiveConnections != 1 || latest.Totals.TotalRequests != 4 || latest.Totals.TotalErrors != 1 {
		t.Fatalf("unexpected counters in SSE snapshot: %+v", latest.Totals)
	}

	// 3. Endpoint Prometheus /metrics
	mResp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	metricsBytes, _ := io.ReadAll(mResp.Body)
	mResp.Body.Close()
	metricsStr := string(metricsBytes)

	expectedSnippets := []string{
		`lb_backend_up{backend="http://localhost:9001"} 1`,
		`lb_backend_up{backend="http://localhost:9002"} 0`,
		`lb_backend_active_connections{backend="http://localhost:9001"} 1`,
		`lb_backend_requests_total{backend="http://localhost:9001"} 3`,
		`lb_backend_errors_total{backend="http://localhost:9002"} 1`,
		`lb_backend_request_duration_seconds_sum{backend="http://localhost:9001"}`,
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(metricsStr, snippet) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", snippet, metricsStr)
		}
	}
}
