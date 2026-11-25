package strategy

import (
	"net/http"
	"sync"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// WeightedRoundRobin memilih backend sehat menggunakan algoritma Smooth Weighted Round Robin
// sehingga distribusi trafik proporsional terhadap bobot tanpa menumpuk beruntun di satu backend.
type WeightedRoundRobin struct {
	mu             sync.Mutex
	currentWeights map[*backend.Backend]int
}

// NewWeightedRoundRobin membuat instance algoritma WeightedRoundRobin baru.
func NewWeightedRoundRobin() *WeightedRoundRobin {
	return &WeightedRoundRobin{
		currentWeights: make(map[*backend.Backend]int),
	}
}

// Next memilih backend sehat berikutnya sesuai bobot efektif, atau nil jika semua backend mati.
func (wrr *WeightedRoundRobin) Next(backends []*backend.Backend, _ *http.Request) *backend.Backend {
	if len(backends) == 0 {
		return nil
	}

	wrr.mu.Lock()
	defer wrr.mu.Unlock()

	var best *backend.Backend
	var bestWeight int
	totalWeight := 0

	for _, b := range backends {
		if !b.IsAlive() {
			delete(wrr.currentWeights, b)
			continue
		}

		w := b.Weight()
		totalWeight += w
		cw := wrr.currentWeights[b] + w
		wrr.currentWeights[b] = cw

		if best == nil || cw > bestWeight {
			best = b
			bestWeight = cw
		}
	}

	if best == nil {
		return nil
	}

	wrr.currentWeights[best] -= totalWeight
	return best
}
