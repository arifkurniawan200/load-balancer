package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arifkurniawan200/load-balancer/internal/backend"
)

func TestCheckerActiveThresholdsAndTicker(t *testing.T) {
	var shouldFail atomic.Bool
	var slow atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		if slow.Load() {
			time.Sleep(80 * time.Millisecond)
		}
		if shouldFail.Load() {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	b, err := backend.New(srv.URL, 1)
	if err != nil {
		t.Fatalf("backend.New: %v", err)
	}

	checker, err := New([]*backend.Backend{b}, Config{
		Path:          "/health",
		Interval:      15 * time.Millisecond,
		Timeout:       30 * time.Millisecond,
		FailThreshold: 3,
		RiseThreshold: 2,
	}, nil)
	if err != nil {
		t.Fatalf("health.New: %v", err)
	}

	ctx := context.Background()

	// Simulasi HTTP 500: mati tepat setelah 3 kegagalan berturut-turut
	shouldFail.Store(true)
	checker.CheckAll(ctx)
	if !b.IsAlive() {
		t.Fatalf("expected alive after 1st failure")
	}
	checker.CheckAll(ctx)
	if !b.IsAlive() {
		t.Fatalf("expected alive after 2nd failure")
	}
	checker.CheckAll(ctx)
	if b.IsAlive() {
		t.Fatalf("expected dead after 3rd consecutive failure")
	}

	// Simulasi pulih: butuh 2 sukses berturut-turut untuk hidup kembali
	shouldFail.Store(false)
	checker.CheckAll(ctx)
	if b.IsAlive() {
		t.Fatalf("expected still dead after 1st success")
	}
	checker.CheckAll(ctx)
	if !b.IsAlive() {
		t.Fatalf("expected alive after 2nd consecutive success")
	}

	// Simulasi timeout di ticker loop Start(ctx): melampaui Timeout dihitung gagal hingga backend mati
	slow.Store(true)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		checker.Start(runCtx)
		close(done)
	}()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && b.IsAlive() {
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	<-done

	if b.IsAlive() {
		t.Fatalf("expected ticker loop to mark slow timed-out backend unhealthy")
	}
}
