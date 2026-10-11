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
)

var version = "dev" // default when not injected; release builds inject via -ldflags "-X main.version="

// authFlags carries the minimal auth configuration for this milestone; the
// configuration milestone replaces it with full file loading. Flags win
// over the MAYFLY_AUTH_* environment variables.
type authFlags struct {
	fileUsers      string
	fileACL        string
	dbDSN          string
	anonymousAllow bool
}

func parseAuthFlags() authFlags {
	var f authFlags
	flag.StringVar(&f.fileUsers, "auth-file-users", "", "users file for the file auth source (.json/.yaml/.toml)")
	flag.StringVar(&f.fileACL, "auth-file-acl", "", "acl file for the file authz source (.json/.yaml/.toml)")
	flag.StringVar(&f.dbDSN, "auth-db-dsn", "", "db auth source dsn: sqlite:<path> | mysql:<driver-dsn>")
	flag.BoolVar(&f.anonymousAllow, "auth-anonymous-allow", false, "admit passwordless CONNECTs that no source matched")
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
	return f
}

// buildAuth assembles the chains. With no source configured at all the
// broker stays on the allow-all stub and logs a warning; with a partially
// configured pair the missing side falls back to allow-all as well so a
// users-only demo stays usable.
func buildAuth(ctx context.Context, logger *slog.Logger, f authFlags) (hook.Authenticator, hook.Gate, func(context.Context), func() error) {
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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	flags := parseAuthFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authn, authz, startAuth, closeAuth := buildAuth(ctx, logger, flags)
	startAuth(ctx)
	defer func() { _ = closeAuth() }()

	// Hardcoded defaults until the configuration milestone lands.
	tcp, err := listener.NewTCP(listener.TCPConfig{Address: ":1883"})
	if err != nil {
		log.Fatal(err)
	}
	b := broker.New(broker.Options{Logger: logger, Authenticator: authn, Authorizer: authz})
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
}
