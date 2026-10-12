package admin

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

// principal describes an authenticated management caller.
type principal struct {
	name   string
	scopes []string
	via    string // "jwt" or "api_key"
}

// principalContextKey is the echo context key of the authenticated caller.
const principalContextKey = "admin.principal"

// principalOf returns the authenticated caller, nil when unauthenticated.
func principalOf(c echo.Context) *principal {
	p, _ := c.Get(principalContextKey).(principal)
	if p.name == "" {
		return nil
	}
	return &p
}

// errNoCredentials marks a request without usable credentials.
var errNoCredentials = errors.New("no credentials")

// logRequest records every management API call as one structured log entry
// (docs/design/admin-api.md section 3).
func (s *Server) logRequest(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		start := s.now()
		err := next(c)
		res := c.Response()
		attrs := []any{
			"method", c.Request().Method,
			"path", c.Request().URL.Path,
			"status", res.Status,
			"remote_addr", c.RealIP(),
			"duration_ms", s.now().Sub(start).Milliseconds(),
		}
		if p := principalOf(c); p != nil {
			attrs = append(attrs, "principal", p.name, "via", p.via)
		}
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		s.log.Info("admin api", attrs...)
		return err
	}
}

// setupGate enforces the first-run state (docs/design/setup.md section 2):
// before initialization only the setup and login endpoints answer; every
// other API path returns 428.
func (s *Server) setupGate(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if s.isInitialized() {
			return next(c)
		}
		p := c.Path()
		if p == "/api/v1/login" || strings.HasPrefix(p, "/api/v1/setup") {
			return next(c)
		}
		return abort(c, http.StatusPreconditionRequired, "setup_required",
			"mayfly is not initialized; complete the setup wizard first")
	}
}

// requireAuth authenticates the request by JWT (Authorization: Bearer) or
// API key (X-API-Key) and enforces the required scope.
func (s *Server) requireAuth(scope string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			p, err := s.authenticate(c)
			if err != nil {
				return abort(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			}
			if !ScopeSatisfies(p.scopes, scope) {
				return abort(c, http.StatusForbidden, "forbidden",
					"this endpoint requires the "+scope+" scope")
			}
			c.Set(principalContextKey, p)
			return next(c)
		}
	}
}

// authenticate resolves the request credentials into a principal.
func (s *Server) authenticate(c echo.Context) (principal, error) {
	r := c.Request()
	if h := r.Header.Get("Authorization"); h != "" {
		token, ok := strings.CutPrefix(h, "Bearer ")
		if !ok {
			return principal{}, errNoCredentials
		}
		sub, err := verifyToken(s.currentMasterKey(), token, s.now())
		if err != nil {
			return principal{}, err
		}
		// A console session token carries the admin scope
		// (docs/design/admin-api.md section 2).
		return principal{name: sub, scopes: []string{ScopeAdmin}, via: "jwt"}, nil
	}
	if key := r.Header.Get("X-API-Key"); key != "" {
		stored, err := s.store.APIKeyByHash(r.Context(), HashAPIKey(key))
		if err != nil {
			return principal{}, err
		}
		if stored.Expired(s.now()) {
			return principal{}, errNoCredentials
		}
		return principal{name: "apikey:" + stored.Name, scopes: stored.Scopes, via: "api_key"}, nil
	}
	return principal{}, errNoCredentials
}

// verifyPassword checks a plaintext password against a stored bcrypt hash.
func verifyPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// hashPassword hashes a plaintext password with bcrypt.
func hashPassword(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// HashPassword hashes a console password with bcrypt. It is exported for the
// generated initial admin of memory-only runs.
func HashPassword(password string) (string, error) {
	return hashPassword(password)
}

// timePtr maps the zero time to nil for optional JSON timestamps.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// httpErrorHandler renders echo framework errors (unknown routes, method
// mismatches, bind failures) in the uniform {code, message} shape.
func httpErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	status := http.StatusInternalServerError
	message := http.StatusText(status)
	var he *echo.HTTPError
	if errors.As(err, &he) {
		status = he.Code
		message = http.StatusText(status)
		if m, ok := he.Message.(string); ok {
			message = m
		}
	}
	_ = c.JSON(status, map[string]string{"code": statusSlug(status), "message": message})
}

// statusSlug maps an HTTP status onto the machine-readable error code.
func statusSlug(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusConflict:
		return "conflict"
	case http.StatusGone:
		return "gone"
	case http.StatusPreconditionRequired:
		return "setup_required"
	default:
		return "internal"
	}
}
