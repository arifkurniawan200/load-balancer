package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
	"github.com/arifkurniawan200/load-balancer/internal/strategy"
)

func TestLoadBalancerRoundRobinIntegration(t *testing.T) {
	names := []string{"b1", "b2", "b3"}
	servers := make([]*httptest.Server, 0, len(names))
	backends := make([]*backend.Backend, 0, len(names))

	for _, name := range names {
		instanceName := name
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(instanceName))
		}))
		defer srv.Close()
		servers = append(servers, srv)

		b, err := backend.New(srv.URL, 1)
		if err != nil {
			t.Fatalf("backend.New: %v", err)
		}
		backends = append(backends, b)
	}

	lb, err := New(backends, strategy.NewRoundRobin(), nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}

	lbServer := httptest.NewServer(lb)
	defer lbServer.Close()

	// 6 request harus terbagi rata ke b1, b2, b3 (masing-masing 2)
	hits := map[string]int{}
	for i := 0; i < 6; i++ {
		resp, err := http.Get(lbServer.URL + "/hello")
		if err != nil {
			t.Fatalf("GET failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
		}
		hits[strings.TrimSpace(string(body))]++
	}

	for _, name := range names {
		if hits[name] != 2 {
			t.Fatalf("expected 2 hits for %s, got %v", name, hits)
		}
	}

	// Jika semua backend mati, load balancer harus mengembalikan 503 Service Unavailable
	for _, b := range backends {
		b.SetAlive(false)
	}
	resp, err := http.Get(lbServer.URL + "/hello")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", resp.StatusCode)
	}
}

func newDroppingServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestPassiveHealthCheckAndRetryWithBody(t *testing.T) {
	deadURL1 := newDroppingServer(t)
	deadURL2 := newDroppingServer(t)

	aliveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alive:" + string(payload)))
	}))
	defer aliveSrv.Close()

	b1, _ := backend.New(deadURL1, 1)
	b2, _ := backend.New(deadURL2, 1)
	b3, _ := backend.New(aliveSrv.URL, 1)
	backends := []*backend.Backend{b1, b2, b3}

	lb, err := New(backends, strategy.NewRoundRobin(), nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	lbServer := httptest.NewServer(lb)
	defer lbServer.Close()

	// Request pertama mencoba b1 (gagal -> mati) -> retry #1 ke b2 (gagal -> mati) -> retry #2 ke b3 (sukses!)
	resp, err := http.Post(lbServer.URL+"/orders", "application/json", strings.NewReader(`{"item":"book"}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after 2 retries, got %d (%s)", resp.StatusCode, string(body))
	}
	if string(body) != `alive:{"item":"book"}` {
		t.Fatalf("expected replayed POST body at b3, got %q", string(body))
	}
	if b1.IsAlive() || b2.IsAlive() {
		t.Fatalf("expected passive check to mark b1 and b2 unhealthy")
	}
	if !b3.IsAlive() {
		t.Fatalf("expected b3 to remain healthy")
	}
}

func TestPassiveRetryRespectsMaxTwoRetries(t *testing.T) {
	// 3 backend mati berturut-turut + 1 backend hidup di urutan ke-4
	var backends []*backend.Backend
	for i := 0; i < 3; i++ {
		b, _ := backend.New(newDroppingServer(t), 1)
		backends = append(backends, b)
	}

	var aliveHits atomic.Int64
	aliveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		aliveHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("b4-ok"))
	}))
	defer aliveSrv.Close()
	b4, _ := backend.New(aliveSrv.URL, 1)
	backends = append(backends, b4)

	lb, err := New(backends, strategy.NewRoundRobin(), nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	lbServer := httptest.NewServer(lb)
	defer lbServer.Close()

	// Request ke-1: b1 (initial) + b2 (retry 1) + b3 (retry 2) -> habis batas 2 retry -> 502 Bad Gateway
	resp1, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET 1 failed: %v", err)
	}
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway after max 2 retries, got %d", resp1.StatusCode)
	}
	if aliveHits.Load() != 0 {
		t.Fatalf("expected b4 not to be hit on 1st request (max 2 retries), got %d", aliveHits.Load())
	}

	// Request ke-2: karena b1..b3 sudah ditandai mati secara pasif, request langsung sukses ke b4
	resp2, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET 2 failed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || aliveHits.Load() != 1 {
		t.Fatalf("expected 200 OK from b4 on 2nd request, got status=%d hits=%d", resp2.StatusCode, aliveHits.Load())
	}
}

func TestLeastConnectionsWithSlowBackendIntegration(t *testing.T) {
	var slowHits, fast1Hits, fast2Hits atomic.Int64

	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		slowHits.Add(1)
		time.Sleep(80 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slowSrv.Close()

	fastSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fast1Hits.Add(1)
		time.Sleep(3 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv1.Close()

	fastSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fast2Hits.Add(1)
		time.Sleep(3 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer fastSrv2.Close()

	bSlow, _ := backend.New(slowSrv.URL, 1)
	bFast1, _ := backend.New(fastSrv1.URL, 1)
	bFast2, _ := backend.New(fastSrv2.URL, 1)
	backends := []*backend.Backend{bSlow, bFast1, bFast2}

	lb, err := New(backends, strategy.NewLeastConnections(), nil)
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	lbServer := httptest.NewServer(lb)
	defer lbServer.Close()

	// Kirim 8 worker konkuren x 5 request = 40 request
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				resp, err := http.Get(lbServer.URL + "/work")
				if err != nil {
					t.Errorf("GET failed: %v", err)
					return
				}
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// Backend cepat harus melayani jauh lebih banyak request daripada backend lambat
	fastTotal := fast1Hits.Load() + fast2Hits.Load()
	if fastTotal <= slowHits.Load()*2 {
		t.Fatalf("expected fast backends (%d) to handle >2x slow backend (%d)", fastTotal, slowHits.Load())
	}

	// Seluruh counter koneksi aktif wajib kembali tepat ke 0 setelah seluruh request selesai
	for _, b := range backends {
		if got := b.ActiveConnections(); got != 0 {
			t.Fatalf("expected 0 active connections on %s after completion, got %d", b.URLString(), got)
		}
	}
}
