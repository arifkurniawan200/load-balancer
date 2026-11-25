package backend

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		weight  int
		wantErr bool
	}{
		{name: "valid http", rawURL: "http://localhost:9001", weight: 2, wantErr: false},
		{name: "valid https default weight", rawURL: "https://example.com:8443", weight: 0, wantErr: false},
		{name: "empty url", rawURL: "", weight: 1, wantErr: true},
		{name: "missing scheme", rawURL: "localhost:9001", weight: 1, wantErr: true},
		{name: "unsupported scheme", rawURL: "ftp://localhost:21", weight: 1, wantErr: true},
		{name: "missing host", rawURL: "http://", weight: 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(tc.rawURL, tc.weight)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.rawURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !b.IsAlive() {
				t.Fatalf("expected new backend to be alive by default")
			}
			if b.Weight() < 1 {
				t.Fatalf("expected weight >= 1, got %d", b.Weight())
			}
		})
	}
}

func TestAliveConcurrencyAndReverseProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Forwarded-Host", r.Header.Get("X-Forwarded-Host"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream-ok:" + r.URL.Path))
	}))
	defer upstream.Close()

	b, err := New(upstream.URL, 1)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			b.SetAlive(id%2 == 0)
			_ = b.IsAlive()
		}(i)
	}
	wg.Wait()
	b.SetAlive(true)

	req := httptest.NewRequest(http.MethodGet, "http://lb.local/items/42", nil)
	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	if string(body) != "upstream-ok:/items/42" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if got := resp.Header.Get("X-Seen-Forwarded-Host"); got != "lb.local" {
		t.Fatalf("expected X-Forwarded-Host lb.local, got %q", got)
	}
}
