// Program entry point: assembly and startup only, no business logic.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ravenmk2/mayfly/internal/broker"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/server"
)

var version = "dev" // default when not injected; release builds inject via -ldflags "-X main.version="

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Hardcoded defaults until the configuration milestone lands.
	tcp, err := listener.NewTCP(listener.TCPConfig{Address: ":1883"})
	if err != nil {
		log.Fatal(err)
	}
	b := broker.New(broker.Options{Logger: logger})
	b.AddListener(tcp)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() { errCh <- b.Run(ctx) }()

	httpServer := &http.Server{Addr: ":8080", Handler: server.New(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("console listening", "addr", httpServer.Addr, "version", version)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	if err := <-errCh; err != nil {
		logger.Error("mayfly stopped with error", "error", err)
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = b.Close()
}
