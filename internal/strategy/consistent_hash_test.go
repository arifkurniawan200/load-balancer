package strategy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

func TestConsistentHashStickinessAndMinimalRemapping(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
		"http://localhost:9004",
	)
	ch := NewConsistentHash(DefaultVirtualNodes)

	if got := ch.Next(nil, httptest.NewRequest(http.MethodGet, "/", nil)); got != nil {
		t.Fatalf("expected nil for empty backends, got %v", got)
	}

	// 1. Key yang sama (IP client sama walau port sumber TCP berbeda) selalu ke backend yang sama
	reqPortA := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api", nil)
	reqPortA.RemoteAddr = "203.0.113.42:51001"

	reqPortB := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api", nil)
	reqPortB.RemoteAddr = "203.0.113.42:62999"

	reqXFF := httptest.NewRequest(http.MethodGet, "http://localhost:8080/api", nil)
	reqXFF.RemoteAddr = "10.0.0.1:40000"
	reqXFF.Header.Set("X-Forwarded-For", "203.0.113.42, 10.0.0.1")

	first := ch.Next(backends, reqPortA)
	if first == nil {
		t.Fatalf("expected non-nil backend")
	}
	for i := 0; i < 20; i++ {
		if got := ch.Next(backends, reqPortB); got != first {
			t.Fatalf("expected sticky backend %s across ephemeral ports, got %v", first.URLString(), got)
		}
		if got := ch.Next(backends, reqXFF); got != first {
			t.Fatalf("expected sticky backend %s via X-Forwarded-For, got %v", first.URLString(), got)
		}
	}

	// 2. Saat satu backend dihapus dari 4 backend, hanya sebagian kecil (~25%) key yang berpindah,
	// dan tidak ada key pada 3 backend sehat lain yang berpindah antar sesama mereka.
	const totalKeys = 2000
	before := make(map[string]string, totalKeys)
	for i := 0; i < totalKeys; i++ {
		ip := fmt.Sprintf("198.51.%d.%d:12345", (i/250)%256, (i%250)+1)
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = ip
		b := ch.Next(backends, r)
		if b == nil {
			t.Fatalf("unexpected nil for %s", ip)
		}
		before[ip] = b.URLString()
	}

	// Hapus backend ke-2 (http://localhost:9002) sehingga tersisa 3 backend
	reduced := []*backend.Backend{backends[0], backends[2], backends[3]}
	moved := 0
	for ip, prevURL := range before {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = ip
		now := ch.Next(reduced, r)
		if now == nil {
			t.Fatalf("unexpected nil after node removal")
		}
		if now.URLString() != prevURL {
			moved++
			// Key yang berpindah WAJIB hanya berasal dari backend yang dihapus!
			if prevURL != "http://localhost:9002" {
				t.Fatalf("key %s moved unnecessarily from surviving %s to %s", ip, prevURL, now.URLString())
			}
		}
	}

	movedRatio := float64(moved) / float64(totalKeys)
	if movedRatio <= 0.10 || movedRatio >= 0.40 {
		t.Fatalf("expected ~25%% keys to move when 1 of 4 backends removed, got %.2f%% (%d/%d)", movedRatio*100, moved, totalKeys)
	}
}

func TestConsistentHashDeadAndRevivedBackends(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)
	ch := NewConsistentHash(DefaultVirtualNodes)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.88:44321"

	primary := ch.Next(backends, req)
	if primary == nil {
		t.Fatalf("expected primary backend")
	}

	// Matikan primary backend di tengah jalan -> harus dialihkan ke backend sehat lain
	primary.SetAlive(false)
	fallback := ch.Next(backends, req)
	if fallback == nil || fallback == primary {
		t.Fatalf("expected healthy fallback backend different from dead %s, got %v", primary.URLString(), fallback)
	}

	// Hidupkan kembali primary -> harus kembali ke primary semula
	primary.SetAlive(true)
	if got := ch.Next(backends, req); got != primary {
		t.Fatalf("expected request to return to revived primary %s, got %v", primary.URLString(), got)
	}

	// Semua backend mati -> harus nil
	for _, b := range backends {
		b.SetAlive(false)
	}
	if got := ch.Next(backends, req); got != nil {
		t.Fatalf("expected nil when all backends are dead, got %v", got)
	}
}

func TestConsistentHashConcurrentRace(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)
	ch := NewConsistentHash(DefaultVirtualNodes)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				if i%12 == 0 {
					backends[1].SetAlive((id+i)%2 == 0)
				}
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.RemoteAddr = fmt.Sprintf("10.10.%d.%d:5000", id, i%50)
				if got := ch.Next(backends, r); got == nil {
					t.Errorf("unexpected nil while backend[0] and backend[2] remained alive")
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
