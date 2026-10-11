// Program entry point: assembly and startup only, no business logic.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/broker"
	"github.com/ravenmk2/mayfly/internal/config"
	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/metrics"
	"github.com/ravenmk2/mayfly/internal/server"
	"github.com/ravenmk2/mayfly/internal/store"
)

var version = "dev" // default when not injected; release builds inject via -ldflags "-X main.version="

// cliFlags are the few knobs kept outside the configuration file; they have
// the highest precedence over MAYFLY_* variables and mayfly.yaml.
type cliFlags struct {
	config      string
	dataDir     string
	showVersion bool
}

func parseFlags() cliFlags {
	var f cliFlags
	flag.StringVar(&f.config, "config", "", "path to mayfly.yaml (default "+config.DefaultPath+")")
	flag.StringVar(&f.dataDir, "data-dir", "", "data directory (overrides data_dir)")
	flag.BoolVar(&f.showVersion, "version", false, "print the version and exit")
	flag.Parse()
	return f
}

// newLogger builds the process logger from the configuration
// (docs/design/observability.md): text for development, JSON for
// production; output is stderr, stdout or a file path.
func newLogger(lc config.LogConfig) *slog.Logger {
	var level slog.Level
	switch lc.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var w io.Writer = os.Stderr
	switch lc.Output {
	case "", "stderr":
	case "stdout":
		w = os.Stdout
	default:
		f, err := os.OpenFile(lc.Output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mayfly: cannot open log output %s: %v; logging to stderr\n", lc.Output, err)
		} else {
			w = f
		}
	}
	opts := &slog.HandlerOptions{Level: level}
	if lc.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

// buildAuth assembles the authentication/authorization chains. With no
// source configured at all the broker stays on the allow-all stub and logs
// a warning; a chain without sources falls back to allow-all as well so a
// users-only demo stays usable.
func buildAuth(ctx context.Context, logger *slog.Logger, cfg config.AuthConfig) (hook.Authenticator, hook.Gate, func(context.Context), func() error) {
	if !cfg.HasSources() {
		logger.Warn("no authentication configured, allowing all clients")
		return hook.NewAuthChain(hook.AllowAll{}), hook.NewAuthzChain(hook.AllowAll{}), func(context.Context) {}, func() error { return nil }
	}

	authCfg := cfg.ToAuthConfig()
	m, err := auth.NewManager(ctx, authCfg, logger)
	if err != nil {
		log.Fatalf("auth setup: %v", err)
	}

	var authn hook.Authenticator = m
	if len(authCfg.Authentication) == 0 {
		logger.Warn("no authentication source configured, allowing all clients")
		authn = hook.NewAuthChain(hook.AllowAll{})
	}
	var authz hook.Gate = m
	if len(authCfg.Authorization) == 0 {
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

// buildListeners instantiates the configured listeners.
func buildListeners(cfg []config.ListenerConfig) ([]listener.Listener, error) {
	out := make([]listener.Listener, 0, len(cfg))
	for _, lc := range cfg {
		l, err := buildListener(lc)
		if err != nil {
			return nil, fmt.Errorf("listener %s: %w", lc.ID, err)
		}
		out = append(out, l)
	}
	return out, nil
}

// buildListener instantiates one listener from its configuration.
func buildListener(lc config.ListenerConfig) (listener.Listener, error) {
	tlsCfg := listener.TLSConfig{
		CertFile:        lc.TLS.CertFile,
		KeyFile:         lc.TLS.KeyFile,
		CAFile:          lc.TLS.CAFile,
		ClientAuth:      lc.TLS.ClientAuth,
		UseCNAsUsername: lc.TLS.UseCNAsUsername,
	}
	switch lc.Type {
	case config.TypeTCP:
		return listener.NewTCP(listener.TCPConfig{
			ID: lc.ID, Address: lc.Address, MaxConnections: lc.MaxConnections,
		})
	case config.TypeTLS:
		tlsCfg.ID = lc.ID
		tlsCfg.Address = lc.Address
		tlsCfg.MaxConnections = lc.MaxConnections
		return listener.NewTLS(tlsCfg)
	case config.TypeWS, config.TypeWSS:
		wsCfg := listener.WSConfig{
			ID: lc.ID, Address: lc.Address, MaxConnections: lc.MaxConnections,
			Path: lc.Path, AllowedOrigins: lc.AllowedOrigins,
		}
		if lc.Type == config.TypeWSS {
			wsCfg.TLS = &tlsCfg
		}
		return listener.NewWS(wsCfg)
	default:
		return nil, fmt.Errorf("unknown type %q", lc.Type)
	}
}

func main() {
	flags := parseFlags()
	if flags.showVersion {
		fmt.Println("mayfly", version)
		return
	}

	cfg, fromFile, err := config.Load(flags.config)
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}
	if flags.dataDir != "" {
		cfg.DataDir = flags.dataDir
	}
	logger := newLogger(cfg.Log)
	if !fromFile {
		logger.Warn("no configuration file found; running with built-in defaults",
			"path", config.DefaultPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	authn, authz, startAuth, closeAuth := buildAuth(ctx, logger, cfg.Auth)
	startAuth(ctx)
	defer func() { _ = closeAuth() }()

	st := buildStore(ctx, logger, cfg.Store.DSN)

	listeners, err := buildListeners(cfg.Listeners)
	if err != nil {
		log.Fatalf("listener setup: %v", err)
	}
	m := metrics.New(version)
	b := broker.New(broker.Options{
		MaxConnections:     cfg.Broker.MaxConnections,
		MaxPacketSize:      cfg.Broker.MaxPacketSize,
		OutboundQueueDepth: cfg.Broker.OutboundQueueDepth,
		ReceiveMaximum:     cfg.Broker.ReceiveMaximum,
		TopicAliasMaximum:  cfg.Broker.TopicAliasMaximum,
		Logger:             logger,
		Authenticator:      authn,
		Authorizer:         authz,
		Store:              st,
		Metrics:            m,
	})
	for _, l := range listeners {
		b.AddListener(l)
	}

	// The $SYS publisher republishes the broker status as retained
	// $SYS/broker/... messages on its cadence; a negative sys_interval
	// disables it (docs/design/observability.md section 2).
	sysPub := metrics.NewSysPublisher(m, metrics.SysOptions{
		Interval: time.Duration(cfg.Metrics.SysInterval),
		Publish:  func(topic string, payload []byte) error { return b.Publish(topic, payload, true) },
		Refresh:  b.RefreshMetrics,
		Logger:   logger,
	})
	go sysPub.Run(ctx)

	errCh := make(chan error, 2)
	go func() { errCh <- b.Run(ctx) }()

	httpServer := &http.Server{Addr: cfg.Admin.Addr, Handler: server.New(), ReadHeaderTimeout: 5 * time.Second}
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
