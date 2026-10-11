package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAPI builds a Server on a memory store behind a bare echo instance.
func testAPI(t *testing.T, mutate func(*Options)) (*Server, *echo.Echo) {
	t.Helper()
	opts := Options{
		Logger:      testLogger(),
		Store:       NewMemoryStore(),
		MasterKey:   []byte("0123456789abcdef0123456789abcdef"),
		Version:     "test",
		Initialized: true,
	}
	if mutate != nil {
		mutate(&opts)
	}
	s := NewServer(opts)
	t.Cleanup(func() { _ = s.Close() })
	e := echo.New()
	s.RegisterRoutes(e)
	return s, e
}

// do performs one JSON request and decodes the response body.
func do(t *testing.T, e *echo.Echo, method, path string, body any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), "body: %s", rec.Body.String())
	}
	return rec.Code, out
}

// seedUser inserts one console user with the given plaintext password.
func seedUser(t *testing.T, s *Server, username, password string) {
	t.Helper()
	hash, err := hashPassword(password)
	require.NoError(t, err)
	require.NoError(t, s.store.CreateUser(t.Context(), username, hash))
}

// loginToken logs in and returns the Bearer token.
func loginToken(t *testing.T, e *echo.Echo, username, password string) string {
	t.Helper()
	code, body := do(t, e, http.MethodPost, "/api/v1/login",
		map[string]any{"username": username, "password": password}, nil)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	token, _ := body["token"].(string)
	require.NotEmpty(t, token)
	return token
}

func bearer(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func TestLogin(t *testing.T) {
	s, e := testAPI(t, nil)
	seedUser(t, s, "admin", "s3cret")

	tests := []struct {
		name     string
		body     map[string]any
		wantCode int
		wantErr  string
	}{
		{"ok", map[string]any{"username": "admin", "password": "s3cret"}, http.StatusOK, ""},
		{"bad password", map[string]any{"username": "admin", "password": "wrong"}, http.StatusUnauthorized, "invalid_credentials"},
		{"unknown user", map[string]any{"username": "ghost", "password": "s3cret"}, http.StatusUnauthorized, "invalid_credentials"},
		{"missing fields", map[string]any{}, http.StatusBadRequest, "bad_request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := do(t, e, http.MethodPost, "/api/v1/login", tt.body, nil)
			assert.Equal(t, tt.wantCode, code, "body: %v", body)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, body["code"])
				assert.NotEmpty(t, body["message"])
			}
		})
	}

	token := loginToken(t, e, "admin", "s3cret")
	code, _ := do(t, e, http.MethodGet, "/api/v1/apikeys", nil, bearer(token))
	assert.Equal(t, http.StatusOK, code)
}

func TestRequireAuth(t *testing.T) {
	s, e := testAPI(t, nil)
	seedUser(t, s, "admin", "s3cret")

	// A read-only API key.
	plaintext, _, hash, err := GenerateAPIKey()
	require.NoError(t, err)
	_, err = s.store.CreateAPIKey(t.Context(), APIKey{
		Name: "ro", KeyHash: hash, Prefix: plaintext[:12], Scopes: []string{ScopeRead},
	})
	require.NoError(t, err)

	// An expired key.
	expPlain, _, expHash, err := GenerateAPIKey()
	require.NoError(t, err)
	_, err = s.store.CreateAPIKey(t.Context(), APIKey{
		Name: "old", KeyHash: expHash, Prefix: expPlain[:12], Scopes: []string{ScopeAdmin},
		ExpiresAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		headers  map[string]string
		wantCode int
	}{
		{"no credentials", nil, http.StatusUnauthorized},
		{"garbage bearer", map[string]string{"Authorization": "Bearer garbage"}, http.StatusUnauthorized},
		{"basic auth ignored", map[string]string{"Authorization": "Basic abc"}, http.StatusUnauthorized},
		{"read key on admin endpoint", map[string]string{"X-API-Key": plaintext}, http.StatusForbidden},
		{"expired key", map[string]string{"X-API-Key": expPlain}, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := do(t, e, http.MethodGet, "/api/v1/apikeys", nil, tt.headers)
			assert.Equal(t, tt.wantCode, code, "body: %v", body)
			assert.Contains(t, body, "code")
		})
	}
}

func TestSetupGate(t *testing.T) {
	s, e := testAPI(t, func(o *Options) { o.Initialized = false })
	seedUser(t, s, "admin", "s3cret")

	code, body := do(t, e, http.MethodGet, "/api/v1/apikeys", nil, bearer(loginToken(t, e, "admin", "s3cret")))
	assert.Equal(t, http.StatusPreconditionRequired, code)
	assert.Equal(t, "setup_required", body["code"])

	// Login itself stays reachable pre-initialization.
	code, _ = do(t, e, http.MethodPost, "/api/v1/login",
		map[string]any{"username": "admin", "password": "s3cret"}, nil)
	assert.Equal(t, http.StatusOK, code)
}

func TestAPIKeysCRUD(t *testing.T) {
	s, e := testAPI(t, nil)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	// Create: the plaintext key is returned once.
	code, body := do(t, e, http.MethodPost, "/api/v1/apikeys",
		map[string]any{"name": "ci", "scopes": []string{"read", "write"}, "expires_in_days": 30}, auth)
	require.Equal(t, http.StatusCreated, code, "body: %v", body)
	plaintext, _ := body["key"].(string)
	require.NotEmpty(t, plaintext)
	item, _ := body["item"].(map[string]any)
	require.NotNil(t, item)
	assert.Equal(t, "ci", item["name"])
	assert.Equal(t, plaintext[:12], item["prefix"])
	assert.NotContains(t, item, "key")
	assert.NotNil(t, item["expires_at"])
	id := int64(item["id"].(float64))

	// The key authenticates with write (which implies read) scope.
	code, _ = do(t, e, http.MethodGet, "/api/v1/apikeys", nil, map[string]string{"X-API-Key": plaintext})
	// ...but apikeys themselves require the admin scope.
	assert.Equal(t, http.StatusForbidden, code)

	// List: only prefixes are exposed.
	code, body = do(t, e, http.MethodGet, "/api/v1/apikeys", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])
	assert.EqualValues(t, 1, body["page"])
	items, _ := body["items"].([]any)
	require.Len(t, items, 1)
	assert.NotContains(t, items[0], "key")

	// Validation failures.
	code, body = do(t, e, http.MethodPost, "/api/v1/apikeys", map[string]any{"name": "x", "scopes": []string{"bogus"}}, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])
	code, _ = do(t, e, http.MethodPost, "/api/v1/apikeys", map[string]any{"scopes": []string{"read"}}, auth)
	assert.Equal(t, http.StatusBadRequest, code)

	// Delete, then 404 on retry.
	code, _ = do(t, e, http.MethodDelete, "/api/v1/apikeys/"+itoa(id), nil, auth)
	assert.Equal(t, http.StatusNoContent, code)
	code, body = do(t, e, http.MethodDelete, "/api/v1/apikeys/"+itoa(id), nil, auth)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])

	// The deleted key no longer authenticates.
	code, _ = do(t, e, http.MethodGet, "/api/v1/apikeys", nil, map[string]string{"X-API-Key": plaintext})
	assert.Equal(t, http.StatusUnauthorized, code)
}

func TestAPIKeysPagination(t *testing.T) {
	s, e := testAPI(t, nil)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	for range 5 {
		code, _ := do(t, e, http.MethodPost, "/api/v1/apikeys",
			map[string]any{"name": "k", "scopes": []string{"read"}}, auth)
		require.Equal(t, http.StatusCreated, code)
	}

	code, body := do(t, e, http.MethodGet, "/api/v1/apikeys?page=2&limit=2", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 5, body["total"])
	assert.EqualValues(t, 2, body["page"])
	assert.EqualValues(t, 2, body["limit"])
	assert.Len(t, body["items"], 2)

	code, body = do(t, e, http.MethodGet, "/api/v1/apikeys?page=x", nil, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])

	code, body = do(t, e, http.MethodGet, "/api/v1/apikeys?limit=999", nil, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])
}

func TestUnknownAPIPath(t *testing.T) {
	_, e := testAPI(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/nope", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "not_found")
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
