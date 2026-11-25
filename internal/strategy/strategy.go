package strategy

import (
	"net/http"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// Strategy mendefinisikan kontrak algoritma pemilihan backend yang sehat.
type Strategy interface {
	Next(backends []*backend.Backend, r *http.Request) *backend.Backend
}
