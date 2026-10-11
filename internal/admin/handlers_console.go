package admin

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
)

// loginRequest is the POST /api/v1/login body.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// login authenticates a console user and issues a session token
// (docs/design/admin-api.md section 2).
func (s *Server) login(c echo.Context) error {
	var req loginRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	if req.Username == "" || req.Password == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "username and password are required")
	}
	hash, err := s.store.UserHash(c.Request().Context(), req.Username)
	if errors.Is(err, ErrNotFound) || (err == nil && !verifyPassword(hash, req.Password)) {
		return abort(c, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
	}
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "credential lookup failed")
	}
	token, expires, err := signToken(s.currentMasterKey(), req.Username, s.now())
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "token signing failed")
	}
	return c.JSON(http.StatusOK, map[string]any{
		"token":      token,
		"token_type": "Bearer",
		"expires_at": expires.UTC(),
	})
}

// apiKeyJSON is the API view of one key; the plaintext key appears only in
// the creation response.
type apiKeyJSON struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Prefix    string     `json:"prefix"`
	Scopes    []string   `json:"scopes"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at"`
}

func apiKeyView(k APIKey) apiKeyJSON {
	return apiKeyJSON{
		ID: k.ID, Name: k.Name, Prefix: k.Prefix, Scopes: k.Scopes,
		CreatedAt: k.CreatedAt.UTC(), ExpiresAt: timePtr(k.ExpiresAt),
	}
}

// listAPIKeys answers GET /api/v1/apikeys; hashes never leave the server and
// the list shows only the display prefix (docs/design/admin-api.md section 3).
func (s *Server) listAPIKeys(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	keys, err := s.store.ListAPIKeys(c.Request().Context())
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "listing api keys failed")
	}
	items := make([]apiKeyJSON, 0, len(keys))
	for _, k := range paginate(keys, page, limit) {
		items = append(items, apiKeyView(k))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit, "total": len(keys),
	})
}

// createAPIKeyRequest is the POST /api/v1/apikeys body.
type createAPIKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
	// ExpiresInDays optionally bounds the key lifetime; zero never expires.
	ExpiresInDays int `json:"expires_in_days"`
}

// createAPIKey generates a key and returns its plaintext exactly once.
func (s *Server) createAPIKey(c echo.Context) error {
	var req createAPIKeyRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	if req.Name == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "name is required")
	}
	if !ValidScopes(req.Scopes) {
		return abort(c, http.StatusBadRequest, "bad_request",
			"scopes must be a non-empty subset of read, write, admin")
	}
	if req.ExpiresInDays < 0 {
		return abort(c, http.StatusBadRequest, "bad_request", "expires_in_days must be >= 0")
	}
	plaintext, prefix, hash, err := GenerateAPIKey()
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "key generation failed")
	}
	key := APIKey{
		Name: req.Name, KeyHash: hash, Prefix: prefix, Scopes: req.Scopes,
		CreatedAt: s.now().UTC(),
	}
	if req.ExpiresInDays > 0 {
		key.ExpiresAt = key.CreatedAt.Add(time.Duration(req.ExpiresInDays) * 24 * time.Hour)
	}
	id, err := s.store.CreateAPIKey(c.Request().Context(), key)
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "storing api key failed")
	}
	key.ID = id
	s.log.Info("api key created", "name", key.Name, "prefix", key.Prefix, "scopes", key.Scopes)
	return c.JSON(http.StatusCreated, map[string]any{
		"key":  plaintext,
		"item": apiKeyView(key),
	})
}

// deleteAPIKey removes one key; a missing key answers 404.
func (s *Server) deleteAPIKey(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid api key id")
	}
	ok, err := s.store.DeleteAPIKey(c.Request().Context(), id)
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "deleting api key failed")
	}
	if !ok {
		return abort(c, http.StatusNotFound, "not_found", "api key not found")
	}
	s.log.Info("api key deleted", "id", id)
	return c.NoContent(http.StatusNoContent)
}
