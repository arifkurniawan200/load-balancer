package strategy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestLeastConnectionsSelectionAndMidFlightTransitions(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)
	lc := NewLeastConnections()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)

	if got := lc.Next(nil, req); got != nil {
		t.Fatalf("expected nil for empty slice, got %v", got)
	}

	// Saat semua 0 koneksi aktif, tie-breaker berputar rata
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		b := lc.Next(backends, req)
		if b == nil {
			t.Fatalf("unexpected nil backend")
		}
		seen[b.URLString()]++
	}
	for _, b := range backends {
		if seen[b.URLString()] != 2 {
			t.Fatalf("expected tie-breaker 2 hits each, got %v", seen)
		}
	}

	// Simulasi beban aktif: 9001=5, 9002=1, 9003=3 -> harus memilih 9002
	for i := 0; i < 5; i++ {
		backends[0].IncConnections()
	}
	backends[1].IncConnections()
	for i := 0; i < 3; i++ {
		backends[2].IncConnections()
	}

	got := lc.Next(backends, req)
	if got == nil || got.URLString() != "http://localhost:9002" {
		t.Fatalf("expected least-loaded 9002, got %v", got)
	}

	// Backend 9002 mati di tengah jalan meski koneksinya paling kecil -> harus lewati ke 9003 (3 < 5)
	backends[1].SetAlive(false)
	got = lc.Next(backends, req)
	if got == nil || got.URLString() != "http://localhost:9003" {
		t.Fatalf("expected 9003 after 9002 died, got %v", got)
	}

	// Backend 9002 hidup kembali -> kembali dipilih karena koneksi aktifnya=1 paling kecil
	backends[1].SetAlive(true)
	got = lc.Next(backends, req)
	if got == nil || got.URLString() != "http://localhost:9002" {
		t.Fatalf("expected revived 9002 to be picked again, got %v", got)
	}

	// Semua backend mati -> harus nil
	for _, b := range backends {
		b.SetAlive(false)
	}
	if got := lc.Next(backends, req); got != nil {
		t.Fatalf("expected nil when all backends are dead, got %v", got)
	}
}

func TestLeastConnectionsConcurrentRace(t *testing.T) {
	backends := mustBackends(t,
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	)
	lc := NewLeastConnections()
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/", nil)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 150; i++ {
				if i%15 == 0 {
					backends[1].SetAlive((id+i)%2 == 0)
				}
				b := lc.Next(backends, req)
				if b == nil {
					t.Errorf("unexpected nil while backend[0] and backend[2] are alive")
					return
				}
				b.IncConnections()
				b.DecConnections()
			}
		}(g)
	}
	wg.Wait()
}
