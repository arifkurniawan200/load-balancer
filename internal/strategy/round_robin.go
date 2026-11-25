package strategy

import (
	"net/http"
	"sync/atomic"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// RoundRobin memilih backend sehat secara bergiliran menggunakan counter atomic.
type RoundRobin struct {
	counter atomic.Uint64
}

// NewRoundRobin membuat instance algoritma RoundRobin baru.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Next mengembalikan backend sehat berikutnya secara bergiliran, atau nil jika semua backend mati.
func (rr *RoundRobin) Next(backends []*backend.Backend, _ *http.Request) *backend.Backend {
	n := len(backends)
	if n == 0 {
		return nil
	}

	// Majukan counter di setiap percobaan agar backend setelah node mati tidak menerima
	// beban ganda secara berurutan.
	for i := 0; i < n; i++ {
		idx := int((rr.counter.Add(1) - 1) % uint64(n))
		if b := backends[idx]; b.IsAlive() {
			return b
		}
	}

	// Fallback scan untuk menjamin tidak pernah mengembalikan nil apabila ada minimal
	// satu backend sehat saat beberapa goroutine memajukan counter secara bersamaan.
	for _, b := range backends {
		if b.IsAlive() {
			return b
		}
	}

	return nil
}
