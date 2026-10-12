package admin

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/auth"
)

func TestSetupFlow(t *testing.T) {
	dir := t.TempDir()
	var s *Server
	var e *echo.Echo
	s, e = testAPI(t, func(o *Options) {
		o.Initialized = false
		o.DataDir = dir
		// The activation hook swaps the memory console store for the primary
		// database, like the cmd wiring does (docs/design/setup.md section 6).
		o.Activate = func(ctx context.Context) error {
			st, err := OpenSQLStore("sqlite:" + filepath.Join(dir, "mayfly.db"))
			if err != nil {
				return err
			}
			return s.SwapStore(ctx, st)
		}
	})

	// Pre-setup: status reports uninitialized.
	code, body := do(t, e, http.MethodGet, "/api/v1/setup/status", nil, nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, false, body["initialized"])

	// Validation: weak password and unknown db type.
	code, body = do(t, e, http.MethodPost, "/api/v1/setup",
		map[string]any{"db": map[string]any{"type": "sqlite"}, "admin_password": "short"}, nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])

	code, _ = do(t, e, http.MethodPost, "/api/v1/setup",
		map[string]any{"db": map[string]any{"type": "oracle"}, "admin_password": "long-enough"}, nil)
	assert.Equal(t, http.StatusBadRequest, code)

	// test-db probes MySQL only.
	code, body = do(t, e, http.MethodPost, "/api/v1/setup/test-db",
		map[string]any{"db": map[string]any{"type": "sqlite"}}, nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])
	code, body = do(t, e, http.MethodPost, "/api/v1/setup/test-db",
		map[string]any{"db": map[string]any{"type": "mysql", "dsn": "u:p@tcp(127.0.0.1:1)/mayfly"}}, nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "db_unreachable", body["code"])

	// Execute the wizard with SQLite.
	code, body = do(t, e, http.MethodPost, "/api/v1/setup",
		map[string]any{"db": map[string]any{"type": "sqlite"}, "admin_password": "long-enough"}, nil)
	require.Equal(t, http.StatusCreated, code, "body: %v", body)
	assert.Equal(t, true, body["initialized"])
	assert.NotContains(t, body, "restart_required", "the data plane activates in-process")

	// The master key landed on disk and the in-memory signing key swapped.
	raw, err := os.ReadFile(filepath.Join(dir, MasterKeyFile))
	require.NoError(t, err)
	assert.Len(t, raw, MasterKeySize)
	assert.Equal(t, raw, s.currentMasterKey())

	// The generated config parses and points at the SQLite primary database.
	cfgRaw, err := os.ReadFile(filepath.Join(dir, "mayfly.yaml"))
	require.NoError(t, err)
	assert.Contains(t, string(cfgRaw), "mayfly.db")

	// The primary database holds the admin account and the auth schema.
	dsn := "sqlite:" + filepath.Join(dir, "mayfly.db")
	st, err := OpenSQLStore(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	n, err := st.UserCount(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	hash, err := st.UserHash(t.Context(), "admin")
	require.NoError(t, err)
	assert.True(t, verifyPassword(hash, "long-enough"))
	src, err := auth.NewDBSource(t.Context(), dsn)
	require.NoError(t, err)
	require.NotNil(t, src)
	t.Cleanup(func() { _ = src.Close() })

	// Status flips and a second setup is rejected with 410.
	code, body = do(t, e, http.MethodGet, "/api/v1/setup/status", nil, nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, body["initialized"])
	code, body = do(t, e, http.MethodPost, "/api/v1/setup",
		map[string]any{"db": map[string]any{"type": "sqlite"}, "admin_password": "long-enough"}, nil)
	assert.Equal(t, http.StatusGone, code)
	assert.Equal(t, "gone", body["code"])

	// Activation swapped the console store to the primary database: login
	// works without a restart, and the token authenticates against the key
	// loaded from disk.
	token := loginToken(t, e, "admin", "long-enough")
	code, _ = do(t, e, http.MethodGet, "/api/v1/apikeys", nil, bearer(token))
	assert.Equal(t, http.StatusOK, code)
}

func TestSetupValidation(t *testing.T) {
	tests := []struct {
		name string
		in   SetupInput
		ok   bool
	}{
		{"sqlite", SetupInput{DBType: "sqlite", AdminPassword: "long-enough"}, true},
		{"mysql", SetupInput{DBType: "mysql", DSN: "u:p@tcp(h)/db", AdminPassword: "long-enough"}, true},
		{"mysql without dsn", SetupInput{DBType: "mysql", AdminPassword: "long-enough"}, false},
		{"bad type", SetupInput{DBType: "pg", AdminPassword: "long-enough"}, false},
		{"short password", SetupInput{DBType: "sqlite", AdminPassword: "short"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.in.validate()
			if tt.ok {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrInvalidSetup)
			}
		})
	}
}

func TestMainDSN(t *testing.T) {
	in := SetupInput{DBType: "sqlite"}
	assert.Equal(t, "sqlite:"+filepath.Join("/data", "mayfly.db"), in.MainDSN("/data"))
	in = SetupInput{DBType: "mysql", DSN: "u:p@tcp(h:3306)/db"}
	assert.Equal(t, "mysql:u:p@tcp(h:3306)/db", in.MainDSN("/data"))
}
