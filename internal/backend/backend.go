package backend

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
)

const (
	// DefaultFailThreshold adalah jumlah kegagalan aktif berturut-turut sebelum backend ditandai mati.
	DefaultFailThreshold = 3
	// DefaultRiseThreshold adalah jumlah sukses aktif berturut-turut sebelum backend hidup kembali.
	DefaultRiseThreshold = 2
)

// Backend merepresentasikan satu server upstream HTTP beserta reverse proxy dan status kesehatannya.
type Backend struct {
	url          *url.URL
	weight       int
	alive        atomic.Bool
	reverseProxy *httputil.ReverseProxy

	stateMu              sync.Mutex
	consecutiveFailures  int
	consecutiveSuccesses int
}

// New memvalidasi rawURL dan membuat instance Backend baru yang secara default berstatus sehat (alive).
func New(rawURL string, weight int) (*Backend, error) {
	if rawURL == "" {
		return nil, errors.New("backend url cannot be empty")
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse backend url %q: %w", rawURL, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("backend url %q must include http/https scheme and host", rawURL)
	}

	if weight <= 0 {
		weight = 1
	}

	b := &Backend{
		url:          parsed,
		weight:       weight,
		reverseProxy: newSingleHostReverseProxy(parsed, nil),
	}
	b.alive.Store(true)

	return b, nil
}

func newSingleHostReverseProxy(target *url.URL, errHandler func(http.ResponseWriter, *http.Request, error)) *httputil.ReverseProxy {
	rp := httputil.NewSingleHostReverseProxy(target)
	origDirector := rp.Director
	rp.Director = func(req *http.Request) {
		origHost := req.Host
		origDirector(req)
		req.Host = target.Host
		if origHost != "" && req.Header.Get("X-Forwarded-Host") == "" {
			req.Header.Set("X-Forwarded-Host", origHost)
		}
	}
	if errHandler != nil {
		rp.ErrorHandler = errHandler
	}
	return rp
}

// NewReverseProxy membuat instance httputil.ReverseProxy ke backend ini dengan ErrorHandler kustom.
func (b *Backend) NewReverseProxy(errHandler func(http.ResponseWriter, *http.Request, error)) *httputil.ReverseProxy {
	return newSingleHostReverseProxy(b.url, errHandler)
}

// URL mengembalikan salinan URL upstream agar state internal tidak termodifikasi dari luar.
func (b *Backend) URL() *url.URL {
	u := *b.url
	return &u
}

// URLString mengembalikan representasi string dari URL backend.
func (b *Backend) URLString() string {
	return b.url.String()
}

// Weight mengembalikan bobot backend (minimal 1).
func (b *Backend) Weight() int {
	return b.weight
}

// IsAlive mengembalikan true apabila backend berstatus sehat secara atomic.
func (b *Backend) IsAlive() bool {
	return b.alive.Load()
}

// SetAlive mengubah status kesehatan backend secara eksplisit dan me-reset counter transisi.
func (b *Backend) SetAlive(alive bool) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()

	b.consecutiveFailures = 0
	b.consecutiveSuccesses = 0
	b.alive.Store(alive)
}

// MarkUnhealthy langsung menandai backend tidak sehat (dipakai oleh passive health check).
// Mengembalikan true apabila terjadi perubahan status dari hidup ke mati.
func (b *Backend) MarkUnhealthy() bool {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()

	wasAlive := b.alive.Load()
	b.alive.Store(false)
	b.consecutiveFailures = 0
	b.consecutiveSuccesses = 0
	return wasAlive
}

// RecordHealthCheck mencatat satu hasil active health check dan memperbarui status alive
// sesuai ambang batas kegagalan/sukses berturut-turut.
// Mengembalikan changed=true hanya ketika status alive berubah.
func (b *Backend) RecordHealthCheck(healthy bool, failThreshold, riseThreshold int) (changed bool, nowAlive bool) {
	if failThreshold <= 0 {
		failThreshold = DefaultFailThreshold
	}
	if riseThreshold <= 0 {
		riseThreshold = DefaultRiseThreshold
	}

	b.stateMu.Lock()
	defer b.stateMu.Unlock()

	currentlyAlive := b.alive.Load()
	if healthy {
		b.consecutiveFailures = 0
		if currentlyAlive {
			b.consecutiveSuccesses = 0
			return false, true
		}

		b.consecutiveSuccesses++
		if b.consecutiveSuccesses >= riseThreshold {
			b.alive.Store(true)
			b.consecutiveSuccesses = 0
			return true, true
		}
		return false, false
	}

	b.consecutiveSuccesses = 0
	if !currentlyAlive {
		return false, false
	}

	b.consecutiveFailures++
	if b.consecutiveFailures >= failThreshold {
		b.alive.Store(false)
		b.consecutiveFailures = 0
		return true, false
	}
	return false, true
}

// ServeHTTP meneruskan HTTP request ke upstream melalui httputil.ReverseProxy default.
func (b *Backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.reverseProxy.ServeHTTP(w, r)
}
