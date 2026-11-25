package strategy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

func TestWeightedRoundRobinSmoothDistributionAndTransitions(t *testing.T) {
	b1, _ := backend.New("http://localhost:9001", 5)
	b2, _ := backend.New("http://localhost:9002", 2)
	b3, _ := backend.New("http://localhost:9003", 1)
	backends := []*backend.Backend{b1, b2, b3}

	wrr := NewWeightedRoundRobin()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	if got := wrr.Next(nil, req); got != nil {
		t.Fatalf("expected nil for empty slice")
	}

	// Total weight = 8 -> dalam 16 request harus persis 10 : 4 : 2 dan tidak burst 5x berturut-turut
	counts := map[string]int{}
	maxStreak := 0
	curStreak := 0
	var prev string

	for i := 0; i < 16; i++ {
		got := wrr.Next(backends, req)
		if got == nil {
			t.Fatalf("unexpected nil at step %d", i)
		}
		u := got.URLString()
		counts[u]++
		if u == prev {
			curStreak++
		} else {
			curStreak = 1
			prev = u
		}
		if curStreak > maxStreak {
			maxStreak = curStreak
		}
	}

	if counts[b1.URLString()] != 10 || counts[b2.URLString()] != 4 || counts[b3.URLString()] != 2 {
		t.Fatalf("expected 10:4:2 distribution, got %v", counts)
	}
	if maxStreak >= 5 {
		t.Fatalf("expected smooth interleaving (streak < 5), got maxStreak=%d", maxStreak)
	}

	// Matikan b1 (bobot 5) di tengah jalan -> sisa b2 (2) : b3 (1) -> dalam 6 request harus 4 : 2
	b1.SetAlive(false)
	countsAfterDown := map[string]int{}
	for i := 0; i < 6; i++ {
		got := wrr.Next(backends, req)
		if got == nil || got == b1 {
			t.Fatalf("expected alive b2/b3, got %v", got)
		}
		countsAfterDown[got.URLString()]++
	}
	if countsAfterDown[b2.URLString()] != 4 || countsAfterDown[b3.URLString()] != 2 {
		t.Fatalf("expected 4:2 split after b1 died, got %v", countsAfterDown)
	}

	// Hidupkan kembali b1 -> kembali dipilih proporsional
	b1.SetAlive(true)
	revivedCounts := map[string]int{}
	for i := 0; i < 8; i++ {
		got := wrr.Next(backends, req)
		revivedCounts[got.URLString()]++
	}
	if revivedCounts[b1.URLString()] != 5 || revivedCounts[b2.URLString()] != 2 || revivedCounts[b3.URLString()] != 1 {
		t.Fatalf("expected 5:2:1 after b1 revived, got %v", revivedCounts)
	}

	// Semua mati -> nil
	for _, b := range backends {
		b.SetAlive(false)
	}
	if got := wrr.Next(backends, req); got != nil {
		t.Fatalf("expected nil when all backends are dead, got %v", got)
	}
}

func TestStrategyFactoryAndWRRConcurrency(t *testing.T) {
	for _, valid := range []string{"", "round_robin", "least_conn", "consistent_hash", "weighted_round_robin", "wrr"} {
		if _, err := New(valid); err != nil {
			t.Fatalf("expected valid strategy %q, got error %v", valid, err)
		}
	}
	if _, err := New("random_magic"); err == nil {
		t.Fatalf("expected error for unknown strategy")
	}

	b1, _ := backend.New("http://localhost:9001", 3)
	b2, _ := backend.New("http://localhost:9002", 2)
	b3, _ := backend.New("http://localhost:9003", 1)
	backends := []*backend.Backend{b1, b2, b3}
	wrr := NewWeightedRoundRobin()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if i%10 == 0 {
					b2.SetAlive((id+i)%2 == 0)
				}
				if got := wrr.Next(backends, req); got == nil {
					t.Errorf("unexpected nil while b1 and b3 are alive")
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
