package admin

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/broker"
)

// Options carries the dependencies of the management API.
type Options struct {
	// Logger receives the structured request and operation log; nil selects
	// slog.Default().
	Logger *slog.Logger
	// Store persists console users and API keys.
	Store Store
	// MasterKey signs console session tokens; Setup replaces it.
	MasterKey []byte
	// Version is reported by GET /api/v1/status.
	Version string
	// Initialized reports the first-run state (docs/design/setup.md section
	// 2): while false only the setup and login endpoints answer.
	Initialized bool
	// Broker serves the runtime data endpoints; may be a broker whose
	// listeners never started (pre-setup).
	Broker *broker.Server
	// AuthManager is the current chain manager; nil when no auth source is
	// configured.
	AuthManager *auth.Manager
	// ApplyChains swaps the broker's authenticator/authorizer to a rebuilt
	// manager (PUT /api/v1/auth/chains); nil disables chain updates.
	ApplyChains func(m *auth.Manager)
	// DataDir is the data directory (master key, generated config).
	DataDir string
	// ConfigPath is where the setup wizard writes mayfly.yaml.
	ConfigPath string
}

// Server is the management REST API (docs/design/admin-api.md). Register its
// routes with RegisterRoutes on the console echo instance.
type Server struct {
	log        *slog.Logger
	store      Store
	version    string
	broker     *broker.Server
	apply      func(m *auth.Manager)
	dataDir    string
	configPath string

	// mu guards masterKey, authMgr and initialized: the setup wizard and the
	// chain endpoints replace them at runtime.
	mu          sync.RWMutex
	masterKey   []byte
	authMgr     *auth.Manager
	initialized bool

	// ctx drives the background refresh of swapped-in chain managers.
	ctx    context.Context
	cancel context.CancelFunc

	now func() time.Time
}

// NewServer builds the management API. It takes ownership of Store and
// AuthManager: Close releases both.
func NewServer(opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		log:         logger,
		store:       opts.Store,
		version:     opts.Version,
		broker:      opts.Broker,
		apply:       opts.ApplyChains,
		dataDir:     opts.DataDir,
		configPath:  opts.ConfigPath,
		masterKey:   opts.MasterKey,
		authMgr:     opts.AuthManager,
		initialized: opts.Initialized,
		ctx:         ctx,
		cancel:      cancel,
		now:         time.Now,
	}
	return s
}

// Close stops swapped-in chain managers and releases the store and the
// current chain manager.
func (s *Server) Close() error {
	s.cancel()
	s.mu.RLock()
	mgr := s.authMgr
	s.mu.RUnlock()
	var err error
	if mgr != nil {
		err = mgr.Close()
	}
	if s.store != nil {
		if cerr := s.store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// RegisterRoutes mounts the API v1 routes on the console echo instance. API
// routes must be registered before the SPA fallback.
func (s *Server) RegisterRoutes(e *echo.Echo) {
	g := e.Group("/api/v1", s.logRequest, s.setupGate)

	g.POST("/login", s.login)

	g.GET("/apikeys", s.listAPIKeys, s.requireAuth(ScopeAdmin))
	g.POST("/apikeys", s.createAPIKey, s.requireAuth(ScopeAdmin))
	g.DELETE("/apikeys/:id", s.deleteAPIKey, s.requireAuth(ScopeAdmin))

	// Uniform error bodies for framework-level errors (404/405/...): echo's
	// default handler would answer {"message":"Not Found"}
	// (docs/design/admin-api.md section 3).
	e.HTTPErrorHandler = httpErrorHandler
}

// isInitialized reports the first-run state.
func (s *Server) isInitialized() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized
}

// currentMasterKey returns the active token signing key.
func (s *Server) currentMasterKey() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.masterKey
}

// currentAuthManager returns the active chain manager.
func (s *Server) currentAuthManager() *auth.Manager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authMgr
}

// abort answers a uniform error body (docs/design/admin-api.md section 3).
func abort(c echo.Context, status int, code, message string) error {
	return c.JSON(status, map[string]string{"code": code, "message": message})
}

// pageParams parses the uniform list pagination: page is 1-based, limit
// defaults to 50 and caps at 500.
func pageParams(c echo.Context) (page, limit int, err error) {
	page, limit = 1, 50
	if v := c.QueryParam("page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return 0, 0, errors.New("page must be a positive integer")
		}
		page = n
	}
	if v := c.QueryParam("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			return 0, 0, errors.New("limit must be between 1 and 500")
		}
		limit = n
	}
	return page, limit, nil
}

// paginate slices items for page/limit and returns the window.
func paginate[T any](items []T, page, limit int) []T {
	start := (page - 1) * limit
	if start >= len(items) {
		return []T{}
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}
