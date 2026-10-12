// Program entry point: assembly and startup only, no business logic.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ravenmk2/mayfly/internal/admin"
	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/broker"
	"github.com/ravenmk2/mayfly/internal/config"
	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/metrics"
	"github.com/ravenmk2/mayfly/internal/server"
	"github.com/ravenmk2/mayfly/internal/store"
	"gopkg.in/natefinch/lumberjack.v2"
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

// logOutput resolves the configured log destination; file paths get
// lumberjack rotation (docs/design/observability.md section 4).
func logOutput(output string) io.Writer {
	switch output {
	case "", "stderr":
		return os.Stderr
	case "stdout":
		return os.Stdout
	default:
		return &lumberjack.Logger{
			Filename:   output,
			MaxSize:    100, // MiB per file before rotation
			MaxBackups: 7,
			MaxAge:     30, // days
			Compress:   true,
		}
	}
}

// newLogger builds the process logger from the configuration
// (docs/design/observability.md): text for development, JSON for
// production; output is stderr, stdout or a rotating file. The returned
// closer releases the file handle (nil for the standard streams).
func newLogger(lc config.LogConfig) (*slog.Logger, io.Closer) {
	var level slog.Level
	switch lc.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	w := logOutput(lc.Output)
	closer, _ := w.(io.Closer)
	opts := &slog.HandlerOptions{Level: level}
	if lc.Format == "json" {
		return slog.New(slog.NewJSONHandler(w, opts)), closer
	}
	return slog.New(slog.NewTextHandler(w, opts)), closer
}

// buildAuth assembles the authentication/authorization chains. With no
// source configured at all the broker stays on the allow-all stub and logs
// a warning; a chain without sources falls back to allow-all as well so a
// users-only demo stays usable. The returned manager (nil without sources)
// feeds the per-source Prometheus breakdown and the admin API's chain
// management.
func buildAuth(ctx context.Context, logger *slog.Logger, cfg config.AuthConfig) (hook.Authenticator, hook.Gate, *auth.Manager) {
	if !cfg.HasSources() {
		logger.Warn("no authentication configured, allowing all clients")
		return hook.NewAuthChain(hook.AllowAll{}), hook.NewAuthzChain(hook.AllowAll{}), nil
	}

	authCfg := cfg.ToAuthConfig()
	m, err := auth.NewManager(ctx, authCfg, logger)
	if err != nil {
		log.Fatalf("auth setup: %v", err)
	}

	authn, authz := chainViews(m, authCfg)
	return authn, authz, m
}

// chainViews derives the broker-facing chains from a manager, falling back
// to the allow-all stub for a chain without sources.
func chainViews(m *auth.Manager, cfg auth.Config) (hook.Authenticator, hook.Gate) {
	var authn hook.Authenticator = m
	if len(cfg.Authentication) == 0 {
		authn = hook.NewAuthChain(hook.AllowAll{})
	}
	var authz hook.Gate = m
	if len(cfg.Authorization) == 0 {
		authz = hook.NewAuthzChain(hook.AllowAll{})
	}
	return authn, authz
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

// buildAdminStore opens the management store on the primary database, or the
// in-memory store for memory-only runs (docs/design/admin-api.md section 2).
func buildAdminStore(dsn string) admin.Store {
	if dsn == "" {
		return admin.NewMemoryStore()
	}
	st, err := admin.OpenSQLStore(dsn)
	if err != nil {
		log.Fatalf("admin store setup: %v", err)
	}
	return st
}

// envSetup runs the non-interactive installation when MAYFLY_SETUP_DB and
// MAYFLY_SETUP_ADMIN_PASSWORD are set and no configuration file exists yet
// (docs/design/setup.md section 5). It reports whether setup ran; the caller
// then reloads the generated configuration.
func envSetup(ctx context.Context, logger *slog.Logger, dataDir, configPath string) bool {
	dbType := os.Getenv("MAYFLY_SETUP_DB")
	password := os.Getenv("MAYFLY_SETUP_ADMIN_PASSWORD")
	if dbType == "" && password == "" {
		return false
	}
	if dbType == "" || password == "" {
		log.Fatalf("setup: MAYFLY_SETUP_DB and MAYFLY_SETUP_ADMIN_PASSWORD must both be set")
	}
	_, err := admin.PerformSetup(ctx, dataDir, configPath, admin.SetupInput{
		DBType:        dbType,
		DSN:           os.Getenv("MAYFLY_SETUP_MYSQL_DSN"),
		AdminPassword: password,
	}, logger)
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
	logger.Info("non-interactive setup completed", "db", dbType)
	return true
}

// randomPassword returns a random plaintext password for the generated
// initial console admin of memory-only runs.
func randomPassword() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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
	logger, closeLog := newLogger(cfg.Log)
	defer func() {
		if closeLog != nil {
			_ = closeLog.Close()
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Non-interactive setup runs before the first-run detection and is
	// followed by a configuration reload (docs/design/setup.md section 5).
	if !fromFile && envSetup(ctx, logger, cfg.DataDir, filepath.Join(cfg.DataDir, "mayfly.yaml")) {
		cfg, fromFile, err = config.Load(filepath.Join(cfg.DataDir, "mayfly.yaml"))
		if err != nil {
			log.Fatalf("configuration: %v", err)
		}
		if flags.dataDir != "" {
			cfg.DataDir = flags.dataDir
		}
	}
	if !fromFile {
		logger.Warn("no configuration file found; running with built-in defaults",
			"path", config.DefaultPath)
	}

	masterKey, persisted, err := admin.LoadMasterKey(cfg.DataDir, logger)
	if err != nil {
		log.Fatalf("master key: %v", err)
	}
	if !persisted && fromFile {
		logger.Warn("the master key is ephemeral; console sessions invalidate on restart")
	}

	adminStore := buildAdminStore(cfg.Store.DSN)

	// First-run detection (docs/design/setup.md section 2): initialized means
	// a configuration file plus a console admin account. Memory-only runs
	// cannot satisfy that across restarts, so they generate a random admin
	// password printed to the log exactly once.
	adminCount, err := adminStore.UserCount(ctx)
	if err != nil {
		log.Fatalf("admin store: %v", err)
	}
	initialized := fromFile && adminCount > 0
	if fromFile && adminCount == 0 {
		if _, memory := adminStore.(*admin.MemoryStore); memory {
			password := randomPassword()
			hash, err := admin.HashPassword(password)
			if err != nil {
				log.Fatalf("admin store: %v", err)
			}
			if err := adminStore.CreateUser(ctx, "admin", hash); err != nil {
				log.Fatalf("admin store: %v", err)
			}
			logger.Warn("generated initial console admin credentials (shown once; configure a store to persist them)",
				"username", "admin", "password", password)
			initialized = true
		}
	}

	// The data plane starts only after initialization; before that the
	// broker is built without a store, auth chains or listeners so the
	// management API can still report status.
	var (
		authn     hook.Authenticator = hook.NewAuthChain(hook.AllowAll{})
		authz     hook.Gate          = hook.NewAuthzChain(hook.AllowAll{})
		mgr       *auth.Manager
		st        store.Store
		listeners []listener.Listener
	)
	if initialized {
		authn, authz, mgr = buildAuth(ctx, logger, cfg.Auth)
		if mgr != nil {
			mgr.Start(ctx)
		}
		st = buildStore(ctx, logger, cfg.Store.DSN)
		listeners, err = buildListeners(cfg.Listeners)
		if err != nil {
			log.Fatalf("listener setup: %v", err)
		}
	} else {
		logger.Warn("mayfly is not initialized; MQTT listeners are disabled, waiting for the setup wizard",
			"console", cfg.Admin.Addr)
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

	var authStats func() []auth.SourceStat
	if mgr != nil {
		authStats = mgr.Stats
	}
	adminAPI := admin.NewServer(admin.Options{
		Logger:      logger,
		Store:       adminStore,
		MasterKey:   masterKey,
		Version:     version,
		Initialized: initialized,
		Broker:      b,
		AuthManager: mgr,
		ApplyChains: func(nm *auth.Manager) {
			authn, authz := chainViews(nm, nm.Config())
			b.SetChains(authn, authz)
		},
		DataDir: cfg.DataDir,
	})
	defer func() { _ = adminAPI.Close() }()

	// The $SYS publisher republishes the broker status as retained
	// $SYS/broker/... messages on its cadence; a negative sys_interval
	// disables it (docs/design/observability.md section 2).
	if initialized {
		sysPub := metrics.NewSysPublisher(m, metrics.SysOptions{
			Interval: time.Duration(cfg.Metrics.SysInterval),
			Publish:  func(topic string, payload []byte) error { return b.Publish(topic, payload, true) },
			Refresh:  b.RefreshMetrics,
			Logger:   logger,
		})
		go sysPub.Run(ctx)
	}

	errCh := make(chan error, 2)
	if initialized {
		go func() { errCh <- b.Run(ctx) }()
	}

	httpServer := &http.Server{
		Addr: cfg.Admin.Addr,
		Handler: server.New(server.Options{
			MetricsHandler: metrics.NewHandler(m, metrics.PromOptions{AuthStats: authStats}),
			APIRouter:      adminAPI,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
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
