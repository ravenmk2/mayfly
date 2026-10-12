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
	"sync"
	"sync/atomic"
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
	flag.StringVar(&f.config, "config", "", "path to mayfly.yaml (default <data-dir>/mayfly.yaml)")
	flag.StringVar(&f.dataDir, "data-dir", "", "data directory (overrides data_dir)")
	flag.BoolVar(&f.showVersion, "version", false, "print the version and exit")
	flag.Parse()
	return f
}

// effectiveDataDir resolves the data directory in precedence order:
// --data-dir flag > MAYFLY_DATA_DIR > the built-in default.
func effectiveDataDir(flags cliFlags) string {
	if flags.dataDir != "" {
		return flags.dataDir
	}
	if v := os.Getenv("MAYFLY_DATA_DIR"); v != "" {
		return v
	}
	return config.DefaultDataDir
}

// loadConfig loads the configuration: --config when given, otherwise
// mayfly.yaml inside the effective data directory (docs/design/setup.md
// section 1). The data directory is resolved before the configuration path
// so --data-dir relocates mayfly.yaml as well.
func loadConfig(flags cliFlags) (cfg *config.Config, fromFile bool, err error) {
	if flags.config != "" {
		return config.Load(flags.config)
	}
	return config.LoadDefault(effectiveDataDir(flags))
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

// dataPlane is the hot-loadable broker data plane built from one
// configuration: the auth chains, the persistence store and the MQTT
// listeners. It is assembled once at startup for an initialized instance
// and again by the setup wizard's in-process activation
// (docs/design/setup.md section 6).
type dataPlane struct {
	authn     hook.Authenticator
	authz     hook.Gate
	mgr       *auth.Manager
	store     store.Store
	listeners []listener.Listener
}

// close releases every assembled piece; used on partial activation failure.
func (dp *dataPlane) close() {
	if dp.mgr != nil {
		_ = dp.mgr.Close()
	}
	if dp.store != nil {
		_ = dp.store.Close()
	}
	for _, l := range dp.listeners {
		_ = l.Close()
	}
}

// buildDataPlane assembles the data plane from cfg.
func buildDataPlane(ctx context.Context, logger *slog.Logger, cfg *config.Config) (*dataPlane, error) {
	authn, authz, mgr, err := buildAuth(ctx, logger, cfg.Auth)
	if err != nil {
		return nil, fmt.Errorf("auth setup: %w", err)
	}
	st, err := buildStore(ctx, logger, cfg.Store.DSN)
	if err != nil {
		if mgr != nil {
			_ = mgr.Close()
		}
		return nil, fmt.Errorf("store setup: %w", err)
	}
	listeners, err := buildListeners(cfg.Listeners)
	if err != nil {
		if mgr != nil {
			_ = mgr.Close()
		}
		if st != nil {
			_ = st.Close()
		}
		return nil, err
	}
	return &dataPlane{authn: authn, authz: authz, mgr: mgr, store: st, listeners: listeners}, nil
}

// buildAuth assembles the authentication/authorization chains. With no
// source configured at all the broker stays on the allow-all stub and logs
// a warning; a chain without sources falls back to allow-all as well so a
// users-only demo stays usable. The returned manager (nil without sources)
// feeds the per-source Prometheus breakdown and the admin API's chain
// management.
func buildAuth(ctx context.Context, logger *slog.Logger, cfg config.AuthConfig) (hook.Authenticator, hook.Gate, *auth.Manager, error) {
	if !cfg.HasSources() {
		logger.Warn("no authentication configured, allowing all clients")
		return hook.NewAuthChain(hook.AllowAll{}), hook.NewAuthzChain(hook.AllowAll{}), nil, nil
	}

	authCfg := cfg.ToAuthConfig()
	m, err := auth.NewManager(ctx, authCfg, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	authn, authz := chainViews(m, authCfg)
	return authn, authz, m, nil
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
func buildStore(ctx context.Context, logger *slog.Logger, dsn string) (store.Store, error) {
	if dsn == "" {
		return nil, nil
	}
	inner, err := store.Open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	bw, err := store.NewBatchWriter(inner, store.BatchOptions{Logger: logger})
	if err != nil {
		_ = inner.Close()
		return nil, err
	}
	logger.Info("persistence store configured", "backend", inner.Backend())
	return bw, nil
}

// buildListeners instantiates the configured listeners.
func buildListeners(cfg []config.ListenerConfig) ([]listener.Listener, error) {
	out := make([]listener.Listener, 0, len(cfg))
	for _, lc := range cfg {
		l, err := buildListener(lc)
		if err != nil {
			for _, prev := range out {
				_ = prev.Close()
			}
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
func buildAdminStore(dsn string) (admin.Store, error) {
	if dsn == "" {
		return admin.NewMemoryStore(), nil
	}
	st, err := admin.OpenSQLStore(dsn)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// envSetup runs the non-interactive installation when MAYFLY_SETUP_DB and
// MAYFLY_SETUP_ADMIN_PASSWORD are set and no configuration file exists yet
// (docs/design/setup.md section 5). It reports whether setup ran; the caller
// then reloads the generated configuration.
func envSetup(ctx context.Context, logger *slog.Logger, dataDir, configPath string) (bool, error) {
	dbType := os.Getenv("MAYFLY_SETUP_DB")
	password := os.Getenv("MAYFLY_SETUP_ADMIN_PASSWORD")
	if dbType == "" && password == "" {
		return false, nil
	}
	if dbType == "" || password == "" {
		return false, errors.New("setup: MAYFLY_SETUP_DB and MAYFLY_SETUP_ADMIN_PASSWORD must both be set")
	}
	_, err := admin.PerformSetup(ctx, dataDir, configPath, admin.SetupInput{
		DBType:        dbType,
		DSN:           os.Getenv("MAYFLY_SETUP_MYSQL_DSN"),
		AdminPassword: password,
	}, logger)
	if err != nil {
		return false, fmt.Errorf("setup: %w", err)
	}
	logger.Info("non-interactive setup completed", "db", dbType)
	return true, nil
}

// randomPassword returns a random plaintext password for the generated
// initial console admin of memory-only runs.
func randomPassword() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// runtimeState tracks the hot-swappable pieces the setup activation replaces
// so shutdown releases the live instances.
type runtimeState struct {
	mu    sync.Mutex
	store store.Store
}

func (r *runtimeState) setStore(st store.Store) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store = st
}

// closeStore drains and closes the live store, if any.
func (r *runtimeState) closeStore() {
	r.mu.Lock()
	st := r.store
	r.store = nil
	r.mu.Unlock()
	if st != nil {
		// After the broker stops producing writes: drain the batch writer
		// and close the database.
		_ = st.Close()
	}
}

func main() {
	flags := parseFlags()
	if flags.showVersion {
		fmt.Println("mayfly", version)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runApp(ctx, flags); err != nil {
		log.Fatalf("mayfly: %v", err)
	}
}

// runApp assembles and runs mayfly until ctx is cancelled or a fatal server
// error occurs.
func runApp(ctx context.Context, flags cliFlags) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()

	cfg, fromFile, err := loadConfig(flags)
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
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

	// Non-interactive setup runs before the first-run detection and is
	// followed by a configuration reload (docs/design/setup.md section 5).
	if !fromFile {
		ran, err := envSetup(ctx, logger, cfg.DataDir, config.DefaultPathFor(cfg.DataDir))
		if err != nil {
			return err
		}
		if ran {
			cfg, fromFile, err = loadConfig(flags)
			if err != nil {
				return fmt.Errorf("configuration: %w", err)
			}
			if flags.dataDir != "" {
				cfg.DataDir = flags.dataDir
			}
		}
	}
	if !fromFile {
		logger.Warn("no configuration file found; running with built-in defaults",
			"path", config.DefaultPathFor(effectiveDataDir(flags)))
	}

	masterKey, persisted, err := admin.LoadMasterKey(cfg.DataDir, logger)
	if err != nil {
		return fmt.Errorf("master key: %w", err)
	}
	if !persisted && fromFile {
		logger.Warn("the master key is ephemeral; console sessions invalidate on restart")
	}

	adminStore, err := buildAdminStore(cfg.Store.DSN)
	if err != nil {
		return fmt.Errorf("admin store setup: %w", err)
	}

	// First-run detection (docs/design/setup.md section 2): initialized means
	// a configuration file plus a console admin account. Memory-only runs
	// cannot satisfy that across restarts, so they generate a random admin
	// password printed to the log exactly once.
	adminCount, err := adminStore.UserCount(ctx)
	if err != nil {
		return fmt.Errorf("admin store: %w", err)
	}
	initialized := fromFile && adminCount > 0
	if fromFile && adminCount == 0 {
		if _, memory := adminStore.(*admin.MemoryStore); memory {
			password := randomPassword()
			hash, err := admin.HashPassword(password)
			if err != nil {
				return fmt.Errorf("admin store: %w", err)
			}
			if err := adminStore.CreateUser(ctx, "admin", hash); err != nil {
				return fmt.Errorf("admin store: %w", err)
			}
			logger.Warn("generated initial console admin credentials (shown once; configure a store to persist them)",
				"username", "admin", "password", password)
			initialized = true
		}
	}

	// The data plane is assembled before the broker on an initialized boot;
	// pre-setup the broker starts bare and the setup wizard's activation
	// attaches store, chains and listeners in-process.
	var dp *dataPlane
	if initialized {
		dp, err = buildDataPlane(ctx, logger, cfg)
		if err != nil {
			return err
		}
		if dp.mgr != nil {
			dp.mgr.Start(ctx)
		}
	} else {
		logger.Warn("mayfly is not initialized; MQTT listeners are disabled, waiting for the setup wizard",
			"console", cfg.Admin.Addr)
	}

	rt := &runtimeState{}
	if dp != nil {
		rt.setStore(dp.store)
	}

	m := metrics.New(version)
	opts := broker.Options{
		MaxConnections:     cfg.Broker.MaxConnections,
		MaxPacketSize:      cfg.Broker.MaxPacketSize,
		OutboundQueueDepth: cfg.Broker.OutboundQueueDepth,
		ReceiveMaximum:     cfg.Broker.ReceiveMaximum,
		TopicAliasMaximum:  cfg.Broker.TopicAliasMaximum,
		Logger:             logger,
		Metrics:            m,
	}
	if dp != nil {
		opts.Authenticator = dp.authn
		opts.Authorizer = dp.authz
		opts.Store = dp.store
	}
	b := broker.New(opts)
	if dp != nil {
		for _, l := range dp.listeners {
			b.AddListener(l)
		}
	}

	// The per-source auth metrics delegate follows the live manager: pre-setup
	// there is none, and the setup activation installs one.
	var mgrRef atomic.Pointer[auth.Manager]
	if dp != nil && dp.mgr != nil {
		mgrRef.Store(dp.mgr)
	}
	authStats := func() []auth.SourceStat {
		if m := mgrRef.Load(); m != nil {
			return m.Stats()
		}
		return nil
	}

	// The $SYS publisher republishes the broker status as retained
	// $SYS/broker/... messages on its cadence; a negative sys_interval
	// disables it (docs/design/observability.md section 2). It starts once
	// the data plane exists: at boot for an initialized instance, or at
	// activation.
	var sysOnce sync.Once
	startSys := func(c *config.Config) {
		sysOnce.Do(func() {
			sysPub := metrics.NewSysPublisher(m, metrics.SysOptions{
				Interval: time.Duration(c.Metrics.SysInterval),
				Publish:  func(topic string, payload []byte) error { return b.Publish(topic, payload, true) },
				Refresh:  b.RefreshMetrics,
				Logger:   logger,
			})
			go sysPub.Run(ctx)
		})
	}

	var adminAPI *admin.Server
	// activate hot-loads the data plane from the freshly written
	// configuration after the setup wizard ran (docs/design/setup.md section
	// 6). The admin server calls it once; the 410 setup gate makes it
	// idempotent.
	activate := func(actCtx context.Context) error {
		newCfg, fromFile, err := loadConfig(flags)
		if err != nil {
			return fmt.Errorf("reload configuration: %w", err)
		}
		if !fromFile {
			return errors.New("setup produced no configuration file")
		}
		if flags.dataDir != "" {
			newCfg.DataDir = flags.dataDir
		}
		ndp, err := buildDataPlane(actCtx, logger, newCfg)
		if err != nil {
			return err
		}

		// The console store moves from memory to the primary database; the
		// bootstrap admin migrates with its password hash.
		if newCfg.Store.DSN != "" {
			st, err := admin.OpenSQLStore(newCfg.Store.DSN)
			if err != nil {
				ndp.close()
				return fmt.Errorf("admin store setup: %w", err)
			}
			if err := adminAPI.SwapStore(actCtx, st); err != nil {
				_ = st.Close()
				ndp.close()
				return fmt.Errorf("console store swap: %w", err)
			}
		}

		b.SetStore(ndp.store)
		if ndp.mgr != nil {
			mgrRef.Store(ndp.mgr)
			adminAPI.AdoptAuthManager(ndp.mgr) // applies the chains to the broker
		} else {
			b.SetChains(ndp.authn, ndp.authz)
		}
		b.Start(ctx)
		for _, l := range ndp.listeners {
			b.AddListener(l)
		}
		rt.setStore(ndp.store)
		startSys(newCfg)
		return nil
	}

	adminAPI = admin.NewServer(admin.Options{
		Logger:      logger,
		Store:       adminStore,
		MasterKey:   masterKey,
		Version:     version,
		Initialized: initialized,
		Broker:      b,
		AuthManager: dpManager(dp),
		ApplyChains: func(nm *auth.Manager) {
			authn, authz := chainViews(nm, nm.Config())
			b.SetChains(authn, authz)
		},
		Activate: activate,
		DataDir:  cfg.DataDir,
	})
	defer func() { _ = adminAPI.Close() }()

	if initialized {
		startSys(cfg)
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

	var runErr error
	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("mayfly stopped with error", "error", err)
			runErr = err
		}
	case <-ctx.Done():
	}
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = b.Close()
	rt.closeStore()
	return runErr
}

// dpManager unwraps the data plane's auth manager, nil before setup.
func dpManager(dp *dataPlane) *auth.Manager {
	if dp == nil {
		return nil
	}
	return dp.mgr
}
