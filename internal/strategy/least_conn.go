package strategy

import (
	"net/http"
	"sync/atomic"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// LeastConnections memilih backend sehat dengan jumlah koneksi aktif paling sedikit.
// Jika terdapat seri (tie), pemilihan diputar secara bergiliran menggunakan counter atomic.
type LeastConnections struct {
	counter atomic.Uint64
}

// NewLeastConnections membuat instance algoritma LeastConnections baru.
func NewLeastConnections() *LeastConnections {
	return &LeastConnections{}
}

// Next mengembalikan backend sehat dengan koneksi aktif terkecil, atau nil jika semua backend mati.
func (lc *LeastConnections) Next(backends []*backend.Backend, _ *http.Request) *backend.Backend {
	n := len(backends)
	if n == 0 {
		return nil
	}

	offset := int((lc.counter.Add(1) - 1) % uint64(n))

	var selected *backend.Backend
	var minConns int64

	for i := 0; i < n; i++ {
		candidate := backends[(offset+i)%n]
		if !candidate.IsAlive() {
			continue
		}
		conns := candidate.ActiveConnections()
		if selected == nil || conns < minConns {
			selected = candidate
			minConns = conns
		}
	}

	return selected
}
