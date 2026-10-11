package admin

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/ravenmk2/mayfly/internal/auth"
)

// dbSource returns the shared db source of the current chain manager; the
// user and ACL management endpoints answer 503 without one.
func (s *Server) dbSource() *auth.DBSource {
	if m := s.currentAuthManager(); m != nil {
		return m.DBSource()
	}
	return nil
}

// requireDB answers 503 and reports false when no db source backs the
// chains.
func (s *Server) requireDB(c echo.Context) (*auth.DBSource, bool) {
	db := s.dbSource()
	if db == nil {
		return nil, false
	}
	return db, true
}

// noDBSource is the uniform error for user/ACL management without a db
// source.
func (s *Server) noDBSource(c echo.Context) error {
	return abort(c, http.StatusServiceUnavailable, "no_db_source",
		"no db auth source configured; user and ACL management requires one")
}

// userJSON is the API view of one MQTT user.
type userJSON struct {
	Username  string    `json:"username"`
	Superuser bool      `json:"superuser"`
	CreatedAt time.Time `json:"created_at"`
}

func userView(u auth.User) userJSON {
	return userJSON{Username: u.Username, Superuser: u.Superuser, CreatedAt: u.CreatedAt.UTC()}
}

// listUsers answers GET /api/v1/users.
func (s *Server) listUsers(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	users, err := db.ListUsers(c.Request().Context())
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "listing users failed")
	}
	items := make([]userJSON, 0, limit)
	for _, u := range paginate(users, page, limit) {
		items = append(items, userView(u))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit, "total": len(users),
	})
}

// userRequest is the POST/PUT /api/v1/users body.
type userRequest struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Superuser *bool  `json:"superuser"`
}

// createUser answers POST /api/v1/users; the write refreshes the auth cache
// inside DBSource.
func (s *Server) createUser(c echo.Context) error {
	var req userRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	if req.Username == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "username is required")
	}
	if req.Password == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "password is required")
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	ctx := c.Request().Context()
	if _, err := db.GetUser(ctx, req.Username); err == nil {
		return abort(c, http.StatusConflict, "conflict", "user already exists")
	} else if !isNoRows(err) {
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	superuser := req.Superuser != nil && *req.Superuser
	if err := db.CreateUser(ctx, req.Username, req.Password, superuser); err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "creating user failed")
	}
	u, err := db.GetUser(ctx, req.Username)
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	s.log.Info("mqtt user created", "username", req.Username, "superuser", superuser)
	return c.JSON(http.StatusCreated, userView(u))
}

// getUser answers GET /api/v1/users/{username}.
func (s *Server) getUser(c echo.Context) error {
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	u, err := db.GetUser(c.Request().Context(), c.Param("username"))
	if isNoRows(err) {
		return abort(c, http.StatusNotFound, "not_found", "user not found")
	}
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	return c.JSON(http.StatusOK, userView(u))
}

// updateUser answers PUT /api/v1/users/{username}; nil fields stay
// unchanged.
func (s *Server) updateUser(c echo.Context) error {
	var req userRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	ctx := c.Request().Context()
	username := c.Param("username")
	if _, err := db.GetUser(ctx, username); err != nil {
		if isNoRows(err) {
			return abort(c, http.StatusNotFound, "not_found", "user not found")
		}
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	var password *string
	if req.Password != "" {
		password = &req.Password
	}
	if err := db.UpdateUser(ctx, username, password, req.Superuser); err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "updating user failed")
	}
	u, err := db.GetUser(ctx, username)
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	s.log.Info("mqtt user updated", "username", username)
	return c.JSON(http.StatusOK, userView(u))
}

// deleteUser answers DELETE /api/v1/users/{username}.
func (s *Server) deleteUser(c echo.Context) error {
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	ctx := c.Request().Context()
	username := c.Param("username")
	if _, err := db.GetUser(ctx, username); err != nil {
		if isNoRows(err) {
			return abort(c, http.StatusNotFound, "not_found", "user not found")
		}
		return abort(c, http.StatusInternalServerError, "internal", "reading user failed")
	}
	if err := db.DeleteUser(ctx, username); err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "deleting user failed")
	}
	s.log.Info("mqtt user deleted", "username", username)
	return c.NoContent(http.StatusNoContent)
}

// aclJSON is the API view of one ACL rule.
type aclJSON struct {
	ID         int64  `json:"id"`
	Seq        int    `json:"seq"`
	Username   string `json:"username,omitempty"`
	ClientID   string `json:"clientid,omitempty"`
	Action     string `json:"action"`
	Permission string `json:"permission"`
	Topic      string `json:"topic"`
}

// aclView maps one rule onto its API form.
func aclView(r auth.ACLRule) aclJSON {
	action := "allow"
	if r.Action == auth.ActionDeny {
		action = "deny"
	}
	permission := "pubsub"
	switch r.Permission {
	case auth.PermissionPublish:
		permission = "publish"
	case auth.PermissionSubscribe:
		permission = "subscribe"
	}
	return aclJSON{
		ID: r.ID, Seq: r.Seq, Username: r.Username, ClientID: r.ClientID,
		Action: action, Permission: permission, Topic: r.Topic,
	}
}

// parseACL maps the API form of a rule back, validating action/permission
// strings.
func parseACL(in aclJSON) (auth.ACLRule, error) {
	rule := auth.ACLRule{
		ID: in.ID, Seq: in.Seq, Username: in.Username, ClientID: in.ClientID, Topic: in.Topic,
	}
	switch in.Action {
	case "allow":
		rule.Action = auth.ActionAllow
	case "deny":
		rule.Action = auth.ActionDeny
	default:
		return rule, fmt.Errorf("action must be allow or deny, got %q", in.Action)
	}
	switch in.Permission {
	case "publish":
		rule.Permission = auth.PermissionPublish
	case "subscribe":
		rule.Permission = auth.PermissionSubscribe
	case "pubsub":
		rule.Permission = auth.PermissionPubsub
	default:
		return rule, fmt.Errorf("permission must be publish, subscribe or pubsub, got %q", in.Permission)
	}
	return rule, nil
}

// listACLs answers GET /api/v1/acls.
func (s *Server) listACLs(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	rules, err := db.ListACLRules(c.Request().Context())
	if err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "listing acl rules failed")
	}
	items := make([]aclJSON, 0, limit)
	for _, r := range paginate(rules, page, limit) {
		items = append(items, aclView(r))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit, "total": len(rules),
	})
}

// createACL answers POST /api/v1/acls; the write rebuilds the compiled rule
// set inside DBSource.
func (s *Server) createACL(c echo.Context) error {
	var req aclJSON
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	rule, err := parseACL(req)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	id, err := db.CreateACLRule(c.Request().Context(), rule)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	rule.ID = id
	s.log.Info("acl rule created", "id", id, "topic", rule.Topic)
	return c.JSON(http.StatusCreated, aclView(rule))
}

// updateACL answers PUT /api/v1/acls/{id}, replacing the mutable fields.
func (s *Server) updateACL(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid acl rule id")
	}
	var req aclJSON
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	rule, err := parseACL(req)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	ctx := c.Request().Context()
	if !aclExists(db, ctx, id) {
		return abort(c, http.StatusNotFound, "not_found", "acl rule not found")
	}
	rule.ID = id
	if err := db.UpdateACLRule(ctx, rule); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	s.log.Info("acl rule updated", "id", id)
	return c.JSON(http.StatusOK, aclView(rule))
}

// deleteACL answers DELETE /api/v1/acls/{id}.
func (s *Server) deleteACL(c echo.Context) error {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid acl rule id")
	}
	db, ok := s.requireDB(c)
	if !ok {
		return s.noDBSource(c)
	}
	ctx := c.Request().Context()
	if !aclExists(db, ctx, id) {
		return abort(c, http.StatusNotFound, "not_found", "acl rule not found")
	}
	if err := db.DeleteACLRule(ctx, id); err != nil {
		return abort(c, http.StatusInternalServerError, "internal", "deleting acl rule failed")
	}
	s.log.Info("acl rule deleted", "id", id)
	return c.NoContent(http.StatusNoContent)
}

// aclExists reports whether the rule id exists.
func aclExists(db *auth.DBSource, ctx context.Context, id int64) bool {
	rules, err := db.ListACLRules(ctx)
	if err != nil {
		return false
	}
	for _, r := range rules {
		if r.ID == id {
			return true
		}
	}
	return false
}

// chainSourceJSON is the API view of one configured chain source.
type chainSourceJSON struct {
	Type    string `json:"type"`
	Enabled bool   `json:"enabled"`
	Path    string `json:"path,omitempty"`
	// DSN is masked: the password never leaves the server.
	DSN     string `json:"dsn,omitempty"`
	Status  string `json:"status"` // active | disabled | failed
	Backend string `json:"backend,omitempty"`
	Allow   int64  `json:"allow"`
	Deny    int64  `json:"deny"`
	Miss    int64  `json:"miss"`
	Fail    int64  `json:"fail"`
}

// chainSourceView maps one source, masking the DSN password.
func chainSourceView(v auth.SourceView) chainSourceJSON {
	status := "disabled"
	if v.Enabled {
		status = "failed"
		if v.Active {
			status = "active"
		}
	}
	return chainSourceJSON{
		Type: v.Type, Enabled: v.Enabled, Path: v.Path, DSN: maskDSN(v.DSN),
		Status: status, Backend: v.Backend,
		Allow: v.Allow, Deny: v.Deny, Miss: v.Miss, Fail: v.Fail,
	}
}

// maskDSN hides the password of a MySQL DSN.
func maskDSN(dsn string) string {
	scheme, rest, ok := strings.Cut(dsn, ":")
	if !ok {
		return dsn
	}
	if scheme != "mysql" {
		return dsn
	}
	cfg, err := mysqldrv.ParseDSN(rest)
	if err != nil {
		return "mysql:***"
	}
	if cfg.Passwd != "" {
		cfg.Passwd = "***"
	}
	return "mysql:" + cfg.FormatDSN()
}

// getChains answers GET /api/v1/auth/chains.
func (s *Server) getChains(c echo.Context) error {
	m := s.currentAuthManager()
	resp := map[string]any{
		"anonymous_allow": false,
		"authentication":  []chainSourceJSON{},
		"authorization":   []chainSourceJSON{},
	}
	if m == nil {
		return c.JSON(http.StatusOK, resp)
	}
	authn, authz := m.Sources()
	authnViews := make([]chainSourceJSON, 0, len(authn))
	for _, v := range authn {
		authnViews = append(authnViews, chainSourceView(v))
	}
	authzViews := make([]chainSourceJSON, 0, len(authz))
	for _, v := range authz {
		authzViews = append(authzViews, chainSourceView(v))
	}
	resp["anonymous_allow"] = m.Config().AnonymousAllow
	resp["authentication"] = authnViews
	resp["authorization"] = authzViews
	return c.JSON(http.StatusOK, resp)
}

// putChainsRequest is the PUT /api/v1/auth/chains body: the whole chain
// configuration (docs/design/admin-api.md section 3).
type putChainsRequest struct {
	AnonymousAllow bool `json:"anonymous_allow"`
	Authentication []struct {
		Type    string `json:"type"`
		Enabled *bool  `json:"enabled"`
		Path    string `json:"path"`
		DSN     string `json:"dsn"`
	} `json:"authentication"`
	Authorization []struct {
		Type    string `json:"type"`
		Enabled *bool  `json:"enabled"`
		Path    string `json:"path"`
		DSN     string `json:"dsn"`
	} `json:"authorization"`
}

// toSourceConfig maps one API entry onto the chain configuration; sources
// default to enabled.
func toSourceConfig(in []struct {
	Type    string `json:"type"`
	Enabled *bool  `json:"enabled"`
	Path    string `json:"path"`
	DSN     string `json:"dsn"`
}) []auth.SourceConfig {
	out := make([]auth.SourceConfig, 0, len(in))
	for _, sc := range in {
		out = append(out, auth.SourceConfig{
			Type: sc.Type, Enabled: sc.Enabled == nil || *sc.Enabled,
			Path: sc.Path, DSN: sc.DSN,
		})
	}
	return out
}

// putChains validates the new chain configuration by assembling a manager
// from it (unknown types, missing fields and a second db source per chain
// are rejected), then swaps the broker's chains atomically. The update is
// runtime-only: restart re-reads mayfly.yaml.
func (s *Server) putChains(c echo.Context) error {
	if s.apply == nil {
		return abort(c, http.StatusServiceUnavailable, "unavailable",
			"chain updates are not available in this build")
	}
	var req putChainsRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	cfg := auth.Config{
		AnonymousAllow:  req.AnonymousAllow,
		Authentication:  toSourceConfig(req.Authentication),
		Authorization:   toSourceConfig(req.Authorization),
		RefreshInterval: auth.DefaultRefreshInterval,
	}
	mgr, err := auth.NewManager(c.Request().Context(), cfg, s.log)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	s.apply(mgr)
	mgr.Start(s.ctx)

	s.mu.Lock()
	old := s.authMgr
	s.authMgr = mgr
	s.mu.Unlock()
	if old != nil {
		if err := old.Close(); err != nil {
			s.log.Warn("closing the replaced auth manager failed", "error", err)
		}
	}
	s.log.Info("auth chains updated", "sources_authn", len(cfg.Authentication),
		"sources_authz", len(cfg.Authorization))
	return s.getChains(c)
}

// refreshAuth answers POST /api/v1/auth/refresh.
func (s *Server) refreshAuth(c echo.Context) error {
	m := s.currentAuthManager()
	if m == nil {
		return abort(c, http.StatusServiceUnavailable, "unavailable", "no auth sources configured")
	}
	if err := m.Refresh(c.Request().Context()); err != nil {
		return abort(c, http.StatusBadGateway, "refresh_failed", err.Error())
	}
	s.log.Info("auth chains refreshed")
	return c.JSON(http.StatusOK, map[string]bool{"refreshed": true})
}

// isNoRows reports whether err marks a missing row from auth.DBSource.
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
