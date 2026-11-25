package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type backendOptions struct {
	port  string
	name  string
	delay time.Duration
	fail  bool
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	opts, err := parseArgs(os.Args[1:])
	if err != nil {
		logger.Error("invalid arguments", slog.String("error", err.Error()))
		os.Exit(2)
	}

	mux := newBackendHandler(opts, logger)
	addr := normalizeListenAddr(opts.port)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting dummy backend",
			slog.String("instance", opts.name),
			slog.String("addr", addr),
			slog.Duration("delay", opts.delay),
			slog.Bool("fail", opts.fail),
		)
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
		close(errCh)
	}()

	select {
	case serveErr := <-errCh:
		if serveErr != nil {
			logger.Error("backend server failed", slog.String("error", serveErr.Error()))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutting down dummy backend", slog.String("instance", opts.name))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Error("backend shutdown error", slog.String("error", shutdownErr.Error()))
			os.Exit(1)
		}
	}
}

func newBackendHandler(opts backendOptions, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	applyDelay := func(ctx context.Context) bool {
		if opts.delay <= 0 {
			return true
		}
		timer := time.NewTimer(opts.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if !applyDelay(r.Context()) {
			return
		}
		if opts.fail {
			http.Error(w, fmt.Sprintf("unhealthy: %s", opts.name), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "ok: %s\n", opts.name)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if !applyDelay(r.Context()) {
			return
		}

		if opts.fail {
			logger.Warn("simulated backend failure",
				slog.String("instance", opts.name),
				slog.String("path", r.URL.Path),
			)
			http.Error(w, fmt.Sprintf("simulated error from %s", opts.name), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "instance=%s path=%s\n", opts.name, r.URL.Path)

		logger.Info("handled request",
			slog.String("instance", opts.name),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Duration("duration", time.Since(start)),
		)
	})

	return mux
}

func parseArgs(rawArgs []string) (backendOptions, error) {
	flagArgs, posArgs := reorderFlagsFirst(rawArgs)

	fs := flag.NewFlagSet("backend", flag.ContinueOnError)
	var opts backendOptions
	fs.StringVar(&opts.port, "port", "", "listen port or address (can also be passed as positional arg, e.g. 9001)")
	fs.StringVar(&opts.name, "name", "", "instance name returned on / (defaults to backend-<port>)")
	fs.DurationVar(&opts.delay, "delay", 0, "artificial response delay (e.g. 200ms, 2s)")
	fs.BoolVar(&opts.fail, "fail", false, "return HTTP 500 on / and /health to simulate failure")

	if err := fs.Parse(flagArgs); err != nil {
		return backendOptions{}, err
	}

	remaining := append(posArgs, fs.Args()...)
	if opts.port == "" && len(remaining) > 0 {
		opts.port = remaining[0]
		remaining = remaining[1:]
	}
	if len(remaining) > 0 {
		return backendOptions{}, fmt.Errorf("unexpected positional arguments: %v", remaining)
	}
	if strings.TrimSpace(opts.port) == "" {
		return backendOptions{}, errors.New("port is required (usage: backend [flags] <port>)")
	}

	cleanPort := strings.TrimPrefix(opts.port, ":")
	if opts.name == "" {
		opts.name = "backend-" + cleanPort
	}

	return opts, nil
}

// reorderFlagsFirst memungkinkan pemanggilan baik `backend -delay 1s 9001` maupun `backend 9001 -delay 1s`.
func reorderFlagsFirst(args []string) (flags []string, positional []string) {
	valueFlags := map[string]bool{
		"-port": true, "--port": true,
		"-name": true, "--name": true,
		"-delay": true, "--delay": true,
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			if valueFlags[arg] && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		positional = append(positional, arg)
	}
	return flags, positional
}

func normalizeListenAddr(portOrAddr string) string {
	if strings.Contains(portOrAddr, ":") {
		return portOrAddr
	}
	return ":" + portOrAddr
}
