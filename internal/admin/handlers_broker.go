package admin

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/ravenmk2/mayfly/internal/broker"
)

// retainedPayloadPreview caps the payload excerpt returned by GET
// /api/v1/retained.
const retainedPayloadPreview = 256

// statusJSON is the GET /api/v1/status response.
func (s *Server) status(c echo.Context) error {
	s.broker.RefreshMetrics()
	snap := s.broker.Stats.Snapshot()
	return c.JSON(http.StatusOK, map[string]any{
		"version":             s.version,
		"uptime_seconds":      snap.Uptime.Seconds(),
		"initialized":         s.isInitialized(),
		"connections_current": snap.ConnectionsCurrent,
		"connections_total":   snap.ConnectionsTotal,
		"messages_received":   snap.MessagesReceived,
		"messages_sent":       snap.MessagesSent,
		"messages_dropped":    snap.MessagesDropped,
		"bytes_received":      snap.BytesReceived,
		"bytes_sent":          snap.BytesSent,
		"auth_succeeded":      snap.AuthSucceeded,
		"auth_failures":       snap.AuthFailures,
		"subscriptions":       snap.Subscriptions,
		"retained":            snap.Retained,
		"inflight":            snap.Inflight,
		"goroutines":          snap.Goroutines,
	})
}

// connJSON is the API view of one connection.
type connJSON struct {
	ClientID        string `json:"client_id"`
	Username        string `json:"username"`
	RemoteAddr      string `json:"remote_addr"`
	ListenerID      string `json:"listener_id"`
	ProtocolVersion byte   `json:"protocol_version"`
	Connected       bool   `json:"connected"`
	ConnectedAt     string `json:"connected_at,omitempty"`
	CreatedAt       string `json:"created_at,omitempty"`
	StoppedAt       string `json:"stopped_at,omitempty"`
	Keepalive       uint16 `json:"keepalive"`
	CleanStart      bool   `json:"clean_start"`
	SessionExpiry   uint32 `json:"session_expiry"`
	Superuser       bool   `json:"superuser"`
	Subscriptions   int    `json:"subscriptions"`
	InflightOut     int    `json:"inflight_out"`
	InflightIn      int    `json:"inflight_in"`
}

func connView(info broker.ConnInfo) connJSON {
	out := connJSON{
		ClientID: info.ClientID, Username: info.Username,
		RemoteAddr: info.RemoteAddr, ListenerID: info.ListenerID,
		ProtocolVersion: info.ProtocolVersion, Connected: info.Connected,
		Keepalive: info.Keepalive, CleanStart: info.CleanStart,
		SessionExpiry: info.SessionExpiry, Superuser: info.Superuser,
		Subscriptions: info.Subscriptions,
		InflightOut:   info.InflightOut, InflightIn: info.InflightIn,
	}
	if !info.ConnectedAt.IsZero() {
		out.ConnectedAt = info.ConnectedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if !info.CreatedAt.IsZero() {
		out.CreatedAt = info.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	if !info.StoppedAt.IsZero() {
		out.StoppedAt = info.StoppedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	return out
}

// listConnections answers GET /api/v1/connections with client_id/username
// substring filters.
func (s *Server) listConnections(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	idFilter := c.QueryParam("client_id")
	userFilter := c.QueryParam("username")
	var filtered []broker.ConnInfo
	for _, info := range s.broker.ListConnections() {
		if idFilter != "" && !strings.Contains(info.ClientID, idFilter) {
			continue
		}
		if userFilter != "" && !strings.Contains(info.Username, userFilter) {
			continue
		}
		filtered = append(filtered, info)
	}
	items := make([]connJSON, 0, limit)
	for _, info := range paginate(filtered, page, limit) {
		items = append(items, connView(info))
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit, "total": len(filtered),
	})
}

// getConnection answers GET /api/v1/connections/{clientID} with the
// subscription list attached.
func (s *Server) getConnection(c echo.Context) error {
	info, ok := s.broker.ConnectionInfo(c.Param("clientID"))
	if !ok {
		return abort(c, http.StatusNotFound, "not_found", "connection not found")
	}
	return c.JSON(http.StatusOK, map[string]any{
		"connection":    connView(info),
		"subscriptions": subViews(s.broker.ListSubscriptions(info.ClientID)),
	})
}

// deleteConnection kicks a live connection (docs/design/admin-api.md
// section 3).
func (s *Server) deleteConnection(c echo.Context) error {
	if !s.broker.KickConnection(c.Param("clientID")) {
		return abort(c, http.StatusNotFound, "not_found", "no live connection for this client")
	}
	return c.NoContent(http.StatusNoContent)
}

// getSession answers GET /api/v1/sessions/{clientID}.
func (s *Server) getSession(c echo.Context) error {
	info, ok := s.broker.ConnectionInfo(c.Param("clientID"))
	if !ok {
		return abort(c, http.StatusNotFound, "not_found", "session not found")
	}
	return c.JSON(http.StatusOK, map[string]any{
		"session":       connView(info),
		"subscriptions": subViews(s.broker.ListSubscriptions(info.ClientID)),
	})
}

// deleteSession clears one persistent session.
func (s *Server) deleteSession(c echo.Context) error {
	if !s.broker.ClearSession(c.Param("clientID")) {
		return abort(c, http.StatusNotFound, "not_found", "session not found")
	}
	return c.NoContent(http.StatusNoContent)
}

// subJSON is the API view of one subscription.
type subJSON struct {
	ClientID          string `json:"client_id"`
	Filter            string `json:"filter"`
	Qos               byte   `json:"qos"`
	NoLocal           bool   `json:"no_local"`
	RetainAsPublished bool   `json:"retain_as_published"`
	RetainHandling    byte   `json:"retain_handling"`
	Shared            string `json:"shared,omitempty"`
}

func subViews(subs []broker.SubInfo) []subJSON {
	out := make([]subJSON, 0, len(subs))
	for _, sub := range subs {
		out = append(out, subJSON{
			ClientID: sub.ClientID, Filter: sub.Filter, Qos: sub.Qos,
			NoLocal: sub.NoLocal, RetainAsPublished: sub.RetainAsPublished,
			RetainHandling: sub.RetainHandling, Shared: sub.Shared,
		})
	}
	return out
}

// listSubscriptions answers GET /api/v1/subscriptions with client_id/filter
// substring filters.
func (s *Server) listSubscriptions(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	idFilter := c.QueryParam("client_id")
	filterFilter := c.QueryParam("filter")
	var filtered []broker.SubInfo
	for _, sub := range s.broker.ListSubscriptions("") {
		if idFilter != "" && !strings.Contains(sub.ClientID, idFilter) {
			continue
		}
		if filterFilter != "" && !strings.Contains(sub.Filter, filterFilter) {
			continue
		}
		filtered = append(filtered, sub)
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": subViews(paginate(filtered, page, limit)),
		"page":  page, "limit": limit, "total": len(filtered),
	})
}

// retainedJSON is the API view of one retained message with a bounded
// payload preview.
type retainedJSON struct {
	Topic        string `json:"topic"`
	PayloadSize  int    `json:"payload_size"`
	// PayloadPreview is the base64 of the first retainedPayloadPreview bytes.
	PayloadPreview string `json:"payload_preview"`
	Truncated      bool   `json:"truncated"`
}

// listRetained answers GET /api/v1/retained with a topic substring filter.
func (s *Server) listRetained(c echo.Context) error {
	page, limit, err := pageParams(c)
	if err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	topicFilter := c.QueryParam("topic")
	var items []retainedJSON
	for _, pk := range s.broker.ListRetained() {
		if topicFilter != "" && !strings.Contains(pk.TopicName, topicFilter) {
			continue
		}
		preview := pk.Payload
		truncated := false
		if len(preview) > retainedPayloadPreview {
			preview = preview[:retainedPayloadPreview]
			truncated = true
		}
		items = append(items, retainedJSON{
			Topic: pk.TopicName, PayloadSize: len(pk.Payload),
			PayloadPreview: base64.StdEncoding.EncodeToString(preview),
			Truncated:      truncated,
		})
	}
	return c.JSON(http.StatusOK, map[string]any{
		"items": paginate(items, page, limit),
		"page":  page, "limit": limit, "total": len(items),
	})
}

// deleteRetained removes one retained message; deleting a topic without one
// is a no-op (idempotent DELETE).
func (s *Server) deleteRetained(c echo.Context) error {
	topic := c.QueryParam("topic")
	if topic == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "topic query parameter is required")
	}
	if err := s.broker.DeleteRetained(topic); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	s.log.Info("retained message deleted", "topic", topic)
	return c.NoContent(http.StatusNoContent)
}

// publishRequest is the POST /api/v1/publish body. Internal publishes are
// QoS 0 (broker.Server.Publish).
type publishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	// PayloadEncoding is "utf8" (default) or "base64".
	PayloadEncoding string `json:"payload_encoding"`
	Qos             byte   `json:"qos"`
	Retain          bool   `json:"retain"`
}

// publish injects a server-side message (docs/design/admin-api.md section 3).
func (s *Server) publish(c echo.Context) error {
	var req publishRequest
	if err := c.Bind(&req); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", "invalid JSON body")
	}
	if req.Topic == "" {
		return abort(c, http.StatusBadRequest, "bad_request", "topic is required")
	}
	if req.Qos != 0 {
		return abort(c, http.StatusBadRequest, "bad_request", "internal publishes are QoS 0")
	}
	payload := []byte(req.Payload)
	switch req.PayloadEncoding {
	case "", "utf8":
	case "base64":
		raw, err := base64.StdEncoding.DecodeString(req.Payload)
		if err != nil {
			return abort(c, http.StatusBadRequest, "bad_request", "payload is not valid base64")
		}
		payload = raw
	default:
		return abort(c, http.StatusBadRequest, "bad_request", "payload_encoding must be utf8 or base64")
	}
	if err := s.broker.Publish(req.Topic, payload, req.Retain); err != nil {
		return abort(c, http.StatusBadRequest, "bad_request", err.Error())
	}
	s.log.Info("message published via api", "topic", req.Topic, "bytes", len(payload), "retain", req.Retain)
	return c.JSON(http.StatusAccepted, map[string]bool{"ok": true})
}
