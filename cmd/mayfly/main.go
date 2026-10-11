// Program entry point: assembly and startup only, no business logic.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/broker"
	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/server"
	"github.com/ravenmk2/mayfly/internal/store"
)

var version = "dev" // default when not injected; release builds inject via -ldflags "-X main.version="

// cliFlags carries the minimal auth and store configuration for this
// milestone; the configuration milestone replaces it with full file loading.
// Flags win over the MAYFLY_* environment variables.
type cliFlags struct {
	fileUsers      string
	fileACL        string
	dbDSN          string
	anonymousAllow bool
	storeDSN       string
}

func parseFlags() cliFlags {
	var f cliFlags
	flag.StringVar(&f.fileUsers, "auth-file-users", "", "users file for the file auth source (.json/.yaml/.toml)")
	flag.StringVar(&f.fileACL, "auth-file-acl", "", "acl file for the file authz source (.json/.yaml/.toml)")
	flag.StringVar(&f.dbDSN, "auth-db-dsn", "", "db auth source dsn: sqlite:<path> | mysql:<driver-dsn>")
	flag.BoolVar(&f.anonymousAllow, "auth-anonymous-allow", false, "admit passwordless CONNECTs that no source matched")
	flag.StringVar(&f.storeDSN, "store-dsn", "", "persistence store dsn: sqlite:<path> | mysql:<driver-dsn>; empty runs memory-only")
	flag.Parse()

	if f.fileUsers == "" {
		f.fileUsers = os.Getenv("MAYFLY_AUTH_FILE_USERS")
	}
	if f.fileACL == "" {
		f.fileACL = os.Getenv("MAYFLY_AUTH_FILE_ACL")
	}
	if f.dbDSN == "" {
		f.dbDSN = os.Getenv("MAYFLY_AUTH_DB_DSN")
	}
	if !f.anonymousAllow {
		f.anonymousAllow = os.Getenv("MAYFLY_AUTH_ANONYMOUS_ALLOW") == "true"
	}
	if f.storeDSN == "" {
		f.storeDSN = os.Getenv("MAYFLY_STORE_DSN")
	}
	return f
}

// buildAuth assembles the chains. With no source configured at all the
// broker stays on the allow-all stub and logs a warning; with a partially
// configured pair the missing side falls back to allow-all as well so a
// users-only demo stays usable.
func buildAuth(ctx context.Context, logger *slog.Logger, f cliFlags) (hook.Authenticator, hook.Gate, func(context.Context), func() error) {
	if f.fileUsers == "" && f.fileACL == "" && f.dbDSN == "" {
		logger.Warn("no authentication configured, allowing all clients")
		return hook.NewAuthChain(hook.AllowAll{}), hook.NewAuthzChain(hook.AllowAll{}), func(context.Context) {}, func() error { return nil }
	}

	cfg := auth.Config{AnonymousAllow: f.anonymousAllow}
	if f.dbDSN != "" {
		cfg.Authentication = append(cfg.Authentication, auth.SourceConfig{Type: "db", Enabled: true, DSN: f.dbDSN})
		cfg.Authorization = append(cfg.Authorization, auth.SourceConfig{Type: "db", Enabled: true, DSN: f.dbDSN})
	}
	if f.fileUsers != "" {
		cfg.Authentication = append(cfg.Authentication, auth.SourceConfig{Type: "file", Enabled: true, Path: f.fileUsers})
	}
	if f.fileACL != "" {
		cfg.Authorization = append(cfg.Authorization, auth.SourceConfig{Type: "file", Enabled: true, Path: f.fileACL})
	}

	m, err := auth.NewManager(ctx, cfg, logger)
	if err != nil {
		log.Fatalf("auth setup: %v", err)
	}

	var authn hook.Authenticator = m
	if len(cfg.Authentication) == 0 {
		logger.Warn("no authentication source configured, allowing all clients")
		authn = hook.NewAuthChain(hook.AllowAll{})
	}
	var authz hook.Gate = m
	if len(cfg.Authorization) == 0 {
		logger.Warn("no authorization source configured, allowing all operations")
		authz = hook.NewAuthzChain(hook.AllowAll{})
	}
	return authn, authz, m.Start, m.Close
}

// buildStore assembles the persistence layer: the SQL store from the DSN
// fronted by the asynchronous BatchWriter. An empty DSN keeps the broker
// memory-only (docs/design/persistence.md).
func buildStore(ctx context.Context, logger *slog.Logger, dsn string) store.Store {
	if dsn == "" {
		return nil
	}
	inner, err := store.Open(ctx, dsn)
	if err != nil {
		log.Fatalf("store setup: %v", err)
	}
	bw, err := store.NewBatchWriter(inner, store.BatchOptions{Logger: logger})
	if err != nil {
		log.Fatalf("store setup: %v", err)
	}
	logger.Info("persistence store configured", "backend", inner.Backend())
	return bw
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	flags := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authn, authz, startAuth, closeAuth := buildAuth(ctx, logger, flags)
	startAuth(ctx)
	defer func() { _ = closeAuth() }()

	st := buildStore(ctx, logger, flags.storeDSN)

	// Hardcoded defaults until the configuration milestone lands.
	tcp, err := listener.NewTCP(listener.TCPConfig{Address: ":1883"})
	if err != nil {
		log.Fatal(err)
	}
	b := broker.New(broker.Options{Logger: logger, Authenticator: authn, Authorizer: authz, Store: st})
	b.AddListener(tcp)

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
	if st != nil {
		// After the broker stops producing writes: drain the batch writer
		// and close the database.
		_ = st.Close()
	}
}
