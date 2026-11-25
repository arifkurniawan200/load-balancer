package proxy_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/dashboard"
	"github.com/arifkurniawan200/load-balancer/internal/health"
	"github.com/arifkurniawan200/load-balancer/internal/proxy"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

func TestEndToEndPassiveFailoverAndActiveRecovery(t *testing.T) {
	var crashB1 atomic.Bool

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if crashB1.Load() {
			// Putus koneksi TCP mentah agar ReverseProxy memicu ErrorHandler (transport EOF)
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("b1"))
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("b2"))
	}))
	defer srv2.Close()

	b1, _ := backend.New(srv1.URL, 1)
	b2, _ := backend.New(srv2.URL, 1)
	backends := []*backend.Backend{b1, b2}

	checker, err := health.New(backends, health.Config{
		Path:          "/health",
		Interval:      50 * time.Millisecond,
		Timeout:       100 * time.Millisecond,
		FailThreshold: 3,
		RiseThreshold: 2,
	}, nil)
	if err != nil {
		t.Fatalf("health.New: %v", err)
	}

	lb, err := proxy.New(backends, strategy.NewRoundRobin(), nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	dash := dashboard.New(backends, strategy.NameRoundRobin, 50*time.Millisecond, nil)

	lbSrv := httptest.NewServer(lb)
	defer lbSrv.Close()

	// Simulasi b1 crash mendadak -> passive ErrorHandler menandai b1 mati & retry ke b2
	crashB1.Store(true)
	resp, err := http.Get(lbSrv.URL + "/items")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK || string(body) != "b2" {
		t.Fatalf("expected transparent retry to b2, got status=%d body=%q", resp.StatusCode, string(body))
	}
	if b1.IsAlive() {
		t.Fatalf("expected b1 marked dead passively")
	}

	// Pulihkan b1 -> active health check butuh 2x sukses berturut-turut untuk menghidupkan b1 kembali
	crashB1.Store(false)
	ctx := context.Background()
	checker.CheckAll(ctx)
	if b1.IsAlive() {
		t.Fatalf("expected b1 still dead after 1st health success")
	}
	checker.CheckAll(ctx)
	if !b1.IsAlive() {
		t.Fatalf("expected b1 revived after 2nd consecutive health success")
	}

	// Kirim 2 request lagi: b1 kini kembali melayani trafik
	seenB1 := false
	for i := 0; i < 2; i++ {
		r, err := http.Get(lbSrv.URL + "/items")
		if err != nil {
			t.Fatalf("GET after recovery failed: %v", err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if string(b) == "b1" {
			seenB1 = true
		}
	}
	if !seenB1 {
		t.Fatalf("expected revived b1 to serve traffic again")
	}

	// Verifikasi telemetri dashboard
	dashCtx, cancel := context.WithCancel(context.Background())
	go dash.Start(dashCtx)
	time.Sleep(70 * time.Millisecond)
	cancel()

	snap := dash.CurrentSnapshot()
	if snap.Totals.HealthyBackends != 2 || snap.Totals.TotalErrors != 1 {
		t.Fatalf("unexpected dashboard totals: %+v", snap.Totals)
	}
}

func TestRoundRobinVsLeastConnectionsSlowBackendComparison(t *testing.T) {
	type workloadResult struct {
		wallDuration time.Duration
		avgLatency   time.Duration
		slowRequests uint64
	}

	runWorkload := func(stratName string) workloadResult {
		// Simulasikan backend lambat yang jenuh (antrean kapasitas 1 worker @ 30ms/request)
		var slowMu sync.Mutex
		slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			slowMu.Lock()
			time.Sleep(30 * time.Millisecond)
			slowMu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer slowSrv.Close()

		fastSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(2 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer fastSrv1.Close()

		fastSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(2 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer fastSrv2.Close()

		bSlow, _ := backend.New(slowSrv.URL, 1)
		bFast1, _ := backend.New(fastSrv1.URL, 1)
		bFast2, _ := backend.New(fastSrv2.URL, 1)

		strat, err := strategy.New(stratName)
		if err != nil {
			t.Fatalf("strategy.New(%q): %v", stratName, err)
		}
		lb, err := proxy.New([]*backend.Backend{bSlow, bFast1, bFast2}, strat, nil)
		if err != nil {
			t.Fatalf("proxy.New: %v", err)
		}
		lbSrv := httptest.NewServer(lb)
		defer lbSrv.Close()

		const workers = 9
		const reqPerWorker = 5
		var totalReqLatencyNs atomic.Int64

		start := time.Now()
		var wg sync.WaitGroup
		for c := 0; c < workers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < reqPerWorker; i++ {
					reqStart := time.Now()
					resp, err := http.Get(lbSrv.URL + "/")
					if err == nil {
						resp.Body.Close()
					}
					totalReqLatencyNs.Add(time.Since(reqStart).Nanoseconds())
				}
			}()
		}
		wg.Wait()

		const totalReqs = workers * reqPerWorker
		return workloadResult{
			wallDuration: time.Since(start),
			avgLatency:   time.Duration(totalReqLatencyNs.Load() / totalReqs),
			slowRequests: bSlow.Stats().TotalRequests,
		}
	}

	rr := runWorkload(strategy.NameRoundRobin)
	lc := runWorkload(strategy.NameLeastConn)

	if lc.slowRequests >= rr.slowRequests {
		t.Fatalf("expected least_conn to send fewer requests to slow backend (%d) than round_robin (%d)", lc.slowRequests, rr.slowRequests)
	}
	if lc.avgLatency >= rr.avgLatency {
		t.Fatalf("expected least_conn avg latency (%v) < round_robin avg latency (%v)", lc.avgLatency, rr.avgLatency)
	}
	if lc.wallDuration >= rr.wallDuration {
		t.Fatalf("expected least_conn wall time (%v) < round_robin wall time (%v)", lc.wallDuration, rr.wallDuration)
	}
}

func TestConsistentHashAndWeightedRoundRobinEndToEnd(t *testing.T) {
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("node-1"))
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("node-2"))
	}))
	defer srv2.Close()

	// 1. Consistent Hashing via LoadBalancer
	b1, _ := backend.New(srv1.URL, 3)
	b2, _ := backend.New(srv2.URL, 1)

	chLB, _ := proxy.New([]*backend.Backend{b1, b2}, strategy.NewConsistentHash(100), nil)
	chSrv := httptest.NewServer(chLB)
	defer chSrv.Close()

	var targetNode string
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, chSrv.URL+"/profile", nil)
		req.Header.Set("X-Forwarded-For", "198.51.100.77")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		got := strings.TrimSpace(string(body))
		if i == 0 {
			targetNode = got
		} else if got != targetNode {
			t.Fatalf("consistent_hash broke stickiness: want %s, got %s", targetNode, got)
		}
	}

	// 2. Weighted Round Robin (3:1) via LoadBalancer
	w1, _ := backend.New(srv1.URL, 3)
	w2, _ := backend.New(srv2.URL, 1)
	wrrLB, _ := proxy.New([]*backend.Backend{w1, w2}, strategy.NewWeightedRoundRobin(), nil)
	wrrSrv := httptest.NewServer(wrrLB)
	defer wrrSrv.Close()

	for i := 0; i < 8; i++ {
		resp, err := http.Get(wrrSrv.URL + "/")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		resp.Body.Close()
	}

	if w1.Stats().TotalRequests != 6 || w2.Stats().TotalRequests != 2 {
		t.Fatalf("expected 6:2 requests for weights 3:1, got %d:%d", w1.Stats().TotalRequests, w2.Stats().TotalRequests)
	}
}
