package strategy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

func mustBackends(t *testing.T, urls ...string) []*backend.Backend {
	t.Helper()
	out := make([]*backend.Backend, 0, len(urls))
	for _, u := range urls {
		b, err := backend.New(u, 1)
		if err != nil {
			t.Fatalf("backend.New(%q): %v", u, err)
		}
		out = append(out, b)
	}
	return out
}

func TestRoundRobinSequentialAndDeadTransition(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)
	rr := NewRoundRobin()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)

	// Semua hidup: harus berputar berurutan 9001 -> 9002 -> 9003 -> 9001
	wantOrder := []string{
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
		"http://localhost:9001",
	}
	for i, want := range wantOrder {
		got := rr.Next(backends, req)
		if got == nil || got.URLString() != want {
			t.Fatalf("step %d: want %s, got %v", i, want, got)
		}
	}

	// Matikan backend kedua di tengah jalan: distribusi tetap merata ke 9001 & 9003
	backends[1].SetAlive(false)
	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		got := rr.Next(backends, req)
		if got == nil {
			t.Fatalf("unexpected nil backend at step %d", i)
		}
		if got.URLString() == "http://localhost:9002" {
			t.Fatalf("dead backend 9002 should have been skipped")
		}
		counts[got.URLString()]++
	}
	if counts["http://localhost:9001"] != 3 || counts["http://localhost:9003"] != 3 {
		t.Fatalf("expected even 3/3 split across alive backends, got %v", counts)
	}

	// Hidupkan kembali backend kedua: harus kembali menerima trafik
	backends[1].SetAlive(true)
	seenRevived := false
	for i := 0; i < 3; i++ {
		if got := rr.Next(backends, req); got != nil && got.URLString() == "http://localhost:9002" {
			seenRevived = true
		}
	}
	if !seenRevived {
		t.Fatalf("expected revived backend 9002 to receive traffic again")
	}

	// Matikan semua backend: harus mengembalikan nil
	for _, b := range backends {
		b.SetAlive(false)
	}
	if got := rr.Next(backends, req); got != nil {
		t.Fatalf("expected nil when all backends are dead, got %s", got.URLString())
	}
}

func TestRoundRobinEmptyAndConcurrentRace(t *testing.T) {
	rr := NewRoundRobin()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)

	if got := rr.Next(nil, req); got != nil {
		t.Fatalf("expected nil for empty slice, got %v", got)
	}

	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// Backend ke-1 dan ke-2 berubah hidup/mati di tengah jalan,
				// sementara backend ke-0 selalu hidup sehingga Next tidak boleh nil.
				if i%10 == 0 {
					backends[1].SetAlive((id+i)%2 == 0)
					backends[2].SetAlive((id+i)%3 == 0)
				}
				got := rr.Next(backends, req)
				if got == nil {
					t.Errorf("Next returned nil while backend[0] remained alive")
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
