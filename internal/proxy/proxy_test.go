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
