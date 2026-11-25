package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func TestPassiveHealthCheckAndRetryWithBody(t *testing.T) {
	// b1 dan b2 langsung ditutup (simulasi backend mati mendadak sebelum active check jalan)
	deadSrv1 := httptest.NewServer(http.NotFoundHandler())
	deadURL1 := deadSrv1.URL
	deadSrv1.Close()

	deadSrv2 := httptest.NewServer(http.NotFoundHandler())
	deadURL2 := deadSrv2.URL
	deadSrv2.Close()

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
		s := httptest.NewServer(http.NotFoundHandler())
		u := s.URL
		s.Close()
		b, _ := backend.New(u, 1)
		backends = append(backends, b)
	}

	aliveHits := 0
	aliveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		aliveHits++
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
	if aliveHits != 0 {
		t.Fatalf("expected b4 not to be hit on 1st request (max 2 retries), got %d", aliveHits)
	}

	// Request ke-2: karena b1..b3 sudah ditandai mati secara pasif, request langsung sukses ke b4
	resp2, err := http.Get(lbServer.URL + "/")
	if err != nil {
		t.Fatalf("GET 2 failed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || aliveHits != 1 {
		t.Fatalf("expected 200 OK from b4 on 2nd request, got status=%d hits=%d", resp2.StatusCode, aliveHits)
	}
}
