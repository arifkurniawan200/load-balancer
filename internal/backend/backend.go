package backend

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

// Backend merepresentasikan satu server upstream HTTP beserta reverse proxy dan status kesehatannya.
type Backend struct {
	url          *url.URL
	weight       int
	alive        atomic.Bool
	reverseProxy *httputil.ReverseProxy
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

	rp := httputil.NewSingleHostReverseProxy(parsed)
	origDirector := rp.Director
	rp.Director = func(req *http.Request) {
		origHost := req.Host
		origDirector(req)
		req.Host = parsed.Host
		if origHost != "" && req.Header.Get("X-Forwarded-Host") == "" {
			req.Header.Set("X-Forwarded-Host", origHost)
		}
	}

	b := &Backend{
		url:          parsed,
		weight:       weight,
		reverseProxy: rp,
	}
	b.alive.Store(true)

	return b, nil
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

// SetAlive mengubah status kesehatan backend secara atomic.
func (b *Backend) SetAlive(alive bool) {
	b.alive.Store(alive)
}

// ServeHTTP meneruskan HTTP request ke upstream melalui httputil.ReverseProxy.
func (b *Backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.reverseProxy.ServeHTTP(w, r)
}
