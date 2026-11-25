package strategy

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

const (
	NameRoundRobin         = "round_robin"
	NameLeastConn          = "least_conn"
	NameConsistentHash     = "consistent_hash"
	NameWeightedRoundRobin = "weighted_round_robin"
)

// Strategy mendefinisikan kontrak algoritma pemilihan backend yang sehat.
type Strategy interface {
	Next(backends []*backend.Backend, r *http.Request) *backend.Backend
}

// New membangun implementasi Strategy berdasarkan nama konfigurasi.
func New(name string) (Strategy, error) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch normalized {
	case "", NameRoundRobin:
		return NewRoundRobin(), nil
	case NameLeastConn:
		return NewLeastConnections(), nil
	case NameConsistentHash:
		return NewConsistentHash(DefaultVirtualNodes), nil
	case NameWeightedRoundRobin, "wrr":
		return NewWeightedRoundRobin(), nil
	default:
		return nil, fmt.Errorf("unknown strategy %q (valid: round_robin, least_conn, consistent_hash, weighted_round_robin)", name)
	}
}
