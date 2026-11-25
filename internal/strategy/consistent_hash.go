package strategy

import (
	"crypto/sha256"
	"encoding/binary"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

// DefaultVirtualNodes adalah jumlah virtual node per backend pada hash ring sesuai spesifikasi.
const DefaultVirtualNodes = 100

type virtualNode struct {
	hash    uint64
	backend *backend.Backend
}

// ConsistentHash memetakan IP client ke hash ring berisi virtual node per backend.
type ConsistentHash struct {
	virtualNodes int

	mu          sync.RWMutex
	ring        []virtualNode
	cachedNodes []*backend.Backend
}

// NewConsistentHash membuat instance algoritma ConsistentHash dengan jumlah virtual node per backend.
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &ConsistentHash{
		virtualNodes: virtualNodes,
	}
}

// Next mengembalikan backend sehat pertama searah jarum jam pada hash ring berdasarkan IP client.
func (ch *ConsistentHash) Next(backends []*backend.Backend, r *http.Request) *backend.Backend {
	if len(backends) == 0 {
		return nil
	}

	// Quick check agar tidak perlu menyusuri seluruh virtual node jika semua backend mati.
	hasAlive := false
	for _, b := range backends {
		if b.IsAlive() {
			hasAlive = true
			break
		}
	}
	if !hasAlive {
		return nil
	}

	ring := ch.getOrBuildRing(backends)
	if len(ring) == 0 {
		return nil
	}

	key := extractClientIP(r)
	keyHash := hashKey(key)

	startIdx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= keyHash
	})
	if startIdx == len(ring) {
		startIdx = 0
	}

	for i := 0; i < len(ring); i++ {
		candidate := ring[(startIdx+i)%len(ring)].backend
		if candidate.IsAlive() {
			return candidate
		}
	}

	return nil
}

func (ch *ConsistentHash) getOrBuildRing(backends []*backend.Backend) []virtualNode {
	ch.mu.RLock()
	if sameBackendSlice(ch.cachedNodes, backends) {
		ring := ch.ring
		ch.mu.RUnlock()
		return ring
	}
	ch.mu.RUnlock()

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if sameBackendSlice(ch.cachedNodes, backends) {
		return ch.ring
	}

	ring := make([]virtualNode, 0, len(backends)*ch.virtualNodes)
	for _, b := range backends {
		base := b.URLString()
		for v := 0; v < ch.virtualNodes; v++ {
			vnodeKey := base + "#" + strconv.Itoa(v)
			ring = append(ring, virtualNode{
				hash:    hashKey(vnodeKey),
				backend: b,
			})
		}
	}

	sort.Slice(ring, func(i, j int) bool {
		if ring[i].hash == ring[j].hash {
			return ring[i].backend.URLString() < ring[j].backend.URLString()
		}
		return ring[i].hash < ring[j].hash
	})

	cached := make([]*backend.Backend, len(backends))
	copy(cached, backends)
	ch.cachedNodes = cached
	ch.ring = ring

	return ring
}

func sameBackendSlice(a, b []*backend.Backend) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func extractClientIP(r *http.Request) string {
	if r == nil {
		return ""
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if ip := strings.TrimSpace(first); ip != "" {
			return ip
		}
	}

	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}

	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return host
	}

	return strings.TrimSpace(r.RemoteAddr)
}
