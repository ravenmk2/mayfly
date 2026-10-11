package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// DefaultRefreshInterval is the background reload cadence for file and db
// sources (docs/design/auth.md section 5.2).
const DefaultRefreshInterval = 5 * time.Second

// SourceConfig describes one chain entry (docs/design/auth.md section 3).
// A file entry carries a users file in the authentication chain and an acl
// file in the authorization chain; a db entry serves both.
type SourceConfig struct {
	// Type is "file" or "db".
	Type string
	// Enabled gates the source without removing it from the configuration.
	Enabled bool
	// Path is the file source path (.json/.yaml/.yml/.toml).
	Path string
	// DSN is the db source DSN: "sqlite:<path>" or "mysql:<driver-dsn>".
	DSN string
}

// Config is the chain assembly input.
type Config struct {
	// AnonymousAllow admits CONNECTs without a password when every
	// authentication source missed (docs/design/auth.md section 2).
	AnonymousAllow bool
	// RefreshInterval is the background reload cadence; zero selects
	// DefaultRefreshInterval.
	RefreshInterval time.Duration
	// Authentication is the ordered authentication chain.
	Authentication []SourceConfig
	// Authorization is the ordered authorization chain.
	Authorization []SourceConfig
}

// validate enforces the assembly rules: known types, required fields, and
// at most one enabled db source per chain (the db is the built-in database
// chosen at setup time; it may be disabled but not duplicated).
func (c *Config) validate() error {
	for _, chain := range []struct {
		name    string
		sources []SourceConfig
	}{{"authentication", c.Authentication}, {"authorization", c.Authorization}} {
		dbCount := 0
		for i, sc := range chain.sources {
			if !sc.Enabled {
				continue
			}
			switch sc.Type {
			case "file":
				if sc.Path == "" {
					return fmt.Errorf("auth: %s source %d: file path is empty", chain.name, i)
				}
			case "db":
				if sc.DSN == "" {
					return fmt.Errorf("auth: %s source %d: db dsn is empty", chain.name, i)
				}
				dbCount++
			default:
				return fmt.Errorf("auth: %s source %d: unknown type %q", chain.name, i, sc.Type)
			}
		}
		if dbCount > 1 {
			return fmt.Errorf("auth: %s chain allows at most one db source, got %d", chain.name, dbCount)
		}
	}
	return nil
}

// resultCounters counts per-source chain outcomes; these feed
// mayfly_auth_results_total{source,result} once metrics land
// (docs/design/auth.md section 6).
type resultCounters struct {
	allow atomic.Int64
	deny  atomic.Int64
	miss  atomic.Int64
	fail  atomic.Int64
}

// namedSource couples a chain source with its label and counters.
type namedSource struct {
	name     string
	cfg      SourceConfig
	backend  string // db backend name, empty for file sources
	auth     hook.Authenticator
	authz    hook.Authorizer
	counters *resultCounters
}

// Manager owns the assembled chains and the background refresh. It
// implements hook.Authenticator and hook.Gate, so the broker consumes it
// directly.
type Manager struct {
	log          *slog.Logger
	anonymous    bool
	interval     time.Duration
	cfg          Config
	authSources  []*namedSource
	authzSources []*namedSource
	reloads      []func(context.Context) error
	closers      []io.Closer
	// db is the shared db source (at most one enabled db source per chain;
	// a source in both chains is built once), feeding the admin API's user
	// and ACL management.
	db *DBSource

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewManager validates the configuration, builds every enabled source and
// wires the chains. Initial load failures degrade the affected source to
// fail-closed instead of aborting startup; the background refresh recovers
// it.
func NewManager(ctx context.Context, cfg Config, logger *slog.Logger) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{log: logger, anonymous: cfg.AnonymousAllow, interval: cfg.RefreshInterval, cfg: cfg}
	if m.interval <= 0 {
		m.interval = DefaultRefreshInterval
	}

	// A db source appearing in both chains is built once and shared.
	dbByDSN := map[string]*DBSource{}
	buildDB := func(sc SourceConfig) *DBSource {
		if s, ok := dbByDSN[sc.DSN]; ok {
			return s
		}
		s, err := NewDBSource(ctx, sc.DSN)
		if s == nil {
			// DSN-level errors are configuration errors.
			m.log.Error("auth db source unusable", "dsn_scheme", dsnScheme(sc.DSN), "error", err)
			dbByDSN[sc.DSN] = nil
			return nil
		}
		if err != nil {
			m.log.Warn("auth db source initial load failed; fail-closed until refresh recovers",
				"backend", s.Backend().String(), "error", err)
		}
		dbByDSN[sc.DSN] = s
		if m.db == nil {
			m.db = s
		}
		m.reloads = append(m.reloads, s.Reload)
		m.closers = append(m.closers, s)
		return s
	}

	for _, sc := range cfg.Authentication {
		if !sc.Enabled {
			continue
		}
		switch sc.Type {
		case "file":
			f, err := NewFileUsers(sc.Path)
			if err != nil {
				m.log.Warn("auth users file initial load failed; fail-closed until refresh recovers",
					"path", sc.Path, "error", err)
			}
			m.authSources = append(m.authSources, &namedSource{name: "file(" + sc.Path + ")", cfg: sc, auth: f, counters: &resultCounters{}})
			m.reloads = append(m.reloads, func(ctx context.Context) error { return f.Reload() })
		case "db":
			if s := buildDB(sc); s != nil {
				m.authSources = append(m.authSources, &namedSource{name: "db(" + s.Backend().String() + ")", cfg: sc, backend: s.Backend().String(), auth: s, counters: &resultCounters{}})
			}
		}
	}
	for _, sc := range cfg.Authorization {
		if !sc.Enabled {
			continue
		}
		switch sc.Type {
		case "file":
			f, err := NewFileACL(sc.Path)
			if err != nil {
				m.log.Warn("auth acl file initial load failed; fail-closed until refresh recovers",
					"path", sc.Path, "error", err)
			}
			m.authzSources = append(m.authzSources, &namedSource{name: "file(" + sc.Path + ")", cfg: sc, authz: f, counters: &resultCounters{}})
			m.reloads = append(m.reloads, func(ctx context.Context) error { return f.Reload() })
		case "db":
			if s := buildDB(sc); s != nil {
				m.authzSources = append(m.authzSources, &namedSource{name: "db(" + s.Backend().String() + ")", cfg: sc, backend: s.Backend().String(), authz: s, counters: &resultCounters{}})
			}
		}
	}
	return m, nil
}

// dsnScheme extracts the scheme for logs without exposing credentials.
func dsnScheme(dsn string) string {
	scheme, _, _ := strings.Cut(dsn, ":")
	return scheme
}

// Authenticate implements hook.Authenticator over the assembled chain:
// fall-through in order, wrong password stops the chain, every-source-miss
// denies — unless anonymous access is enabled and the CONNECT carries no
// password, which admits a non-superuser anonymous client
// (docs/design/auth.md section 2).
func (m *Manager) Authenticate(ctx context.Context, info hook.ConnectInfo) (hook.Result, error) {
	for _, src := range m.authSources {
		res, err := src.auth.Authenticate(ctx, info)
		switch {
		case errors.Is(err, hook.ErrNotRegistered):
			src.counters.miss.Add(1)
			continue
		case err != nil:
			src.counters.fail.Add(1)
			return hook.Result{OK: false, Reason: "authentication source failure"}, err
		default:
			if res.OK {
				src.counters.allow.Add(1)
			} else {
				src.counters.deny.Add(1)
			}
			return res, nil
		}
	}
	if m.anonymous && info.Password == nil {
		return hook.Result{OK: true}, nil
	}
	return hook.Result{OK: false, Reason: "no authentication source matched"}, nil
}

// Allow implements hook.Gate over the assembled chain: superusers skip the
// chain, the first allow/deny verdict wins, every-source-miss denies.
func (m *Manager) Allow(ctx context.Context, client hook.ClientInfo, op hook.Op, topic string) bool {
	if client.Superuser {
		return true
	}
	for _, src := range m.authzSources {
		switch src.authz.Authorize(ctx, client, op, topic) {
		case hook.VerdictAllow:
			src.counters.allow.Add(1)
			return true
		case hook.VerdictDeny:
			src.counters.deny.Add(1)
			return false
		default:
			src.counters.miss.Add(1)
		}
	}
	return false
}

// Refresh reloads every source now, joining per-source errors. Failed
// sources keep serving their last good snapshot.
func (m *Manager) Refresh(ctx context.Context) error {
	var errs []error
	for _, reload := range m.reloads {
		if err := reload(ctx); err != nil {
			m.log.Warn("auth source refresh failed; last good snapshot keeps serving", "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Start launches the background refresh ticker; it stops when ctx is
// cancelled or Close is called.
func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = m.Refresh(ctx)
			}
		}
	}()
}

// Close stops the refresh loop and releases db pools.
func (m *Manager) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	var errs []error
	for _, c := range m.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SourceStat is one source's outcome counters.
type SourceStat struct {
	Source string
	Chain  string // "authentication" | "authorization"
	Allow  int64
	Deny   int64
	Miss   int64
	Fail   int64
}

// Stats reports per-source outcome counters.
func (m *Manager) Stats() []SourceStat {
	var out []SourceStat
	for _, src := range m.authSources {
		out = append(out, SourceStat{
			Source: src.name, Chain: "authentication",
			Allow: src.counters.allow.Load(), Deny: src.counters.deny.Load(),
			Miss: src.counters.miss.Load(), Fail: src.counters.fail.Load(),
		})
	}
	for _, src := range m.authzSources {
		out = append(out, SourceStat{
			Source: src.name, Chain: "authorization",
			Allow: src.counters.allow.Load(), Deny: src.counters.deny.Load(),
			Miss: src.counters.miss.Load(), Fail: src.counters.fail.Load(),
		})
	}
	return out
}

// Config returns the chain configuration the manager was built from. The
// admin API renders it in GET /api/v1/auth/chains.
func (m *Manager) Config() Config { return m.cfg }

// DBSource returns the shared db source, nil when no db source is
// configured. The admin API manages MQTT users and ACL rules through it.
func (m *Manager) DBSource() *DBSource { return m.db }

// SourceView is one configured chain entry with its runtime state, consumed
// by the admin API.
type SourceView struct {
	SourceConfig
	// Active reports whether the source is built into the live chain:
	// enabled and constructible. An enabled source whose initial build
	// failed (bad DSN) reports Active=false.
	Active bool
	// Backend names the db backend ("sqlite"/"mysql"), empty otherwise.
	Backend string
	Allow   int64
	Deny    int64
	Miss    int64
	Fail    int64
}

// Sources reports both configured chains with per-source runtime state,
// preserving configuration order.
func (m *Manager) Sources() (authn, authz []SourceView) {
	return sourceViews(m.cfg.Authentication, m.authSources),
		sourceViews(m.cfg.Authorization, m.authzSources)
}

// sourceViews merges one configured chain with its live sources, matching by
// type and identity (path for file sources, DSN for db sources).
func sourceViews(cfg []SourceConfig, live []*namedSource) []SourceView {
	out := make([]SourceView, 0, len(cfg))
	for _, sc := range cfg {
		v := SourceView{SourceConfig: sc}
		if sc.Enabled {
			for _, ns := range live {
				if ns.cfg.Type == sc.Type && ns.cfg.Path == sc.Path && ns.cfg.DSN == sc.DSN {
					v.Active = true
					v.Backend = ns.backend
					v.Allow = ns.counters.allow.Load()
					v.Deny = ns.counters.deny.Load()
					v.Miss = ns.counters.miss.Load()
					v.Fail = ns.counters.fail.Load()
					break
				}
			}
		}
		out = append(out, v)
	}
	return out
}
