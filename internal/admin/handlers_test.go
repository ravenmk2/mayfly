package admin

import (
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/broker"
	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// hookConnectInfo builds a CONNECT authentication attempt.
func hookConnectInfo(username, password string) hook.ConnectInfo {
	return hook.ConnectInfo{Username: username, Password: []byte(password)}
}

// hookClientInfo builds an authorization client view.
func hookClientInfo(username string) hook.ClientInfo {
	return hook.ClientInfo{Username: username}
}

// testFullAPI builds a Server backed by a real broker and a db-source chain
// manager on an in-memory SQLite database.
func testFullAPI(t *testing.T) (*Server, *echo.Echo, *broker.Server) {
	t.Helper()
	b := broker.New(broker.Options{Logger: testLogger(), FlushInterval: 0})
	mgr, err := auth.NewManager(t.Context(), auth.Config{
		Authentication: []auth.SourceConfig{{Type: "db", Enabled: true, DSN: "sqlite::memory:"}},
		Authorization:  []auth.SourceConfig{{Type: "db", Enabled: true, DSN: "sqlite::memory:"}},
	}, testLogger())
	require.NoError(t, err)
	s, e := testAPI(t, func(o *Options) {
		o.Broker = b
		o.AuthManager = mgr
		o.ApplyChains = func(m *auth.Manager) { b.SetChains(m, m) }
	})
	return s, e, b
}

// writePacket encodes and writes one packet over the pipe.
func writePacket(t *testing.T, c net.Conn, pk *packet.Packet) {
	t.Helper()
	raw, err := pk.Encode()
	require.NoError(t, err)
	_, err = c.Write(raw)
	require.NoError(t, err)
}

// readFrame reads one raw MQTT frame.
func readFrame(t *testing.T, c net.Conn) []byte {
	t.Helper()
	first := make([]byte, 1)
	_, err := io.ReadFull(c, first)
	require.NoError(t, err)
	raw := first
	remaining := 0
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		_, err := io.ReadFull(c, b)
		require.NoError(t, err)
		raw = append(raw, b[0])
		remaining |= int(b[0]&0x7f) << (7 * i)
		if b[0]&0x80 == 0 {
			break
		}
	}
	if remaining > 0 {
		body := make([]byte, remaining)
		_, err := io.ReadFull(c, body)
		require.NoError(t, err)
		raw = append(raw, body...)
	}
	return raw
}

// mqttConnect opens a v3.1.1 connection through the broker and returns the
// client side of the pipe.
func mqttConnect(t *testing.T, b *broker.Server, connID, clientID string) net.Conn {
	t.Helper()
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	b.ServeConn(connID, srv)
	writePacket(t, cli, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion: packet.Version311, Clean: true, ClientIdentifier: clientID,
		},
	})
	ack := readFrame(t, cli)
	require.Equal(t, byte(0x20), ack[0], "expected CONNACK")
	require.Equal(t, byte(0), ack[len(ack)-1], "CONNACK rejected")
	return cli
}

// mqttSubscribe subscribes one filter at QoS 0.
func mqttSubscribe(t *testing.T, c net.Conn, packetID uint16, filter string) {
	t.Helper()
	writePacket(t, c, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Subscribe, Qos: 1},
		PacketID:    packetID,
		Filters:     []packet.Subscription{{Filter: filter}},
	})
	ack := readFrame(t, c)
	require.Equal(t, byte(0x90), ack[0], "expected SUBACK")
}

func TestStatus(t *testing.T) {
	s, e, _ := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	code, body := do(t, e, http.MethodGet, "/api/v1/status", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	assert.Equal(t, "test", body["version"])
	assert.Equal(t, true, body["initialized"])
	assert.Contains(t, body, "uptime_seconds")
	assert.Contains(t, body, "connections_current")
}

func TestConnectionsEndpoints(t *testing.T) {
	s, e, b := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	cli := mqttConnect(t, b, "t-1", "dev-1")
	mqttSubscribe(t, cli, 1, "sensors/#")
	_ = mqttConnect(t, b, "t-2", "dev-2")

	// List with pagination and filter.
	code, body := do(t, e, http.MethodGet, "/api/v1/connections", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	assert.EqualValues(t, 2, body["total"])

	code, body = do(t, e, http.MethodGet, "/api/v1/connections?client_id=dev-1", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])

	// Detail includes the subscription list.
	code, body = do(t, e, http.MethodGet, "/api/v1/connections/dev-1", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	conn, _ := body["connection"].(map[string]any)
	require.NotNil(t, conn)
	assert.Equal(t, "dev-1", conn["client_id"])
	assert.Equal(t, true, conn["connected"])
	assert.EqualValues(t, 1, conn["subscriptions"])
	subs, _ := body["subscriptions"].([]any)
	require.Len(t, subs, 1)

	code, body = do(t, e, http.MethodGet, "/api/v1/connections/ghost", nil, auth)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])

	// Kick: the client connection drops.
	code, _ = do(t, e, http.MethodDelete, "/api/v1/connections/dev-2", nil, auth)
	assert.Equal(t, http.StatusNoContent, code)
	code, body = do(t, e, http.MethodDelete, "/api/v1/connections/ghost", nil, auth)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])
}

func TestSessionsEndpoints(t *testing.T) {
	s, e, b := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))
	mqttConnect(t, b, "t-1", "sess-1")

	code, body := do(t, e, http.MethodGet, "/api/v1/sessions/sess-1", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	sess, _ := body["session"].(map[string]any)
	require.NotNil(t, sess)
	assert.Equal(t, "sess-1", sess["client_id"])

	code, _ = do(t, e, http.MethodGet, "/api/v1/sessions/ghost", nil, auth)
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = do(t, e, http.MethodDelete, "/api/v1/sessions/sess-1", nil, auth)
	assert.Equal(t, http.StatusNoContent, code)
	code, body = do(t, e, http.MethodGet, "/api/v1/sessions/sess-1", nil, auth)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])
}

func TestSubscriptionsEndpoint(t *testing.T) {
	s, e, b := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	c1 := mqttConnect(t, b, "t-1", "dev-1")
	mqttSubscribe(t, c1, 1, "a/#")
	mqttSubscribe(t, c1, 2, "b/+")
	c2 := mqttConnect(t, b, "t-2", "dev-2")
	mqttSubscribe(t, c2, 1, "a/#")

	code, body := do(t, e, http.MethodGet, "/api/v1/subscriptions", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	assert.EqualValues(t, 3, body["total"])

	code, body = do(t, e, http.MethodGet, "/api/v1/subscriptions?client_id=dev-2", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])

	code, body = do(t, e, http.MethodGet, "/api/v1/subscriptions?filter=a/&limit=1", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 2, body["total"])
	assert.Len(t, body["items"], 1)
}

func TestRetainedEndpoints(t *testing.T) {
	s, e, b := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))
	require.NoError(t, b.Publish("sensors/1", []byte("on"), true))
	require.NoError(t, b.Publish("sensors/2", []byte("off"), true))

	code, body := do(t, e, http.MethodGet, "/api/v1/retained", nil, auth)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	assert.EqualValues(t, 2, body["total"])
	items, _ := body["items"].([]any)
	require.Len(t, items, 2)
	first, _ := items[0].(map[string]any)
	assert.Equal(t, "sensors/1", first["topic"])
	assert.EqualValues(t, 2, first["payload_size"])
	assert.Equal(t, "b24=", first["payload_preview"]) // base64("on")

	code, body = do(t, e, http.MethodGet, "/api/v1/retained?topic=sensors/2", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])

	code, _ = do(t, e, http.MethodDelete, "/api/v1/retained", nil, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = do(t, e, http.MethodDelete, "/api/v1/retained?topic=sensors/1", nil, auth)
	assert.Equal(t, http.StatusNoContent, code)

	code, body = do(t, e, http.MethodGet, "/api/v1/retained", nil, auth)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])
}

func TestPublishEndpoint(t *testing.T) {
	s, e, b := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	auth := bearer(loginToken(t, e, "admin", "s3cret"))

	cli := mqttConnect(t, b, "t-1", "dev-1")
	mqttSubscribe(t, cli, 1, "announce/#")

	code, body := do(t, e, http.MethodPost, "/api/v1/publish",
		map[string]any{"topic": "announce/all", "payload": "aGVsbG8=", "payload_encoding": "base64"}, auth)
	require.Equal(t, http.StatusAccepted, code, "body: %v", body)

	// The subscribed client receives the message.
	frame := readFrame(t, cli)
	pk := &packet.Packet{ProtocolVersion: packet.Version311}
	require.NoError(t, pk.Decode(frame))
	assert.Equal(t, packet.Publish, pk.FixedHeader.Type)
	assert.Equal(t, "announce/all", pk.TopicName)
	assert.Equal(t, []byte("hello"), pk.Payload)

	// Validation.
	code, _ = do(t, e, http.MethodPost, "/api/v1/publish", map[string]any{"topic": "", "payload": "x"}, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = do(t, e, http.MethodPost, "/api/v1/publish", map[string]any{"topic": "a/b", "payload": "x", "qos": 1}, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = do(t, e, http.MethodPost, "/api/v1/publish", map[string]any{"topic": "a/b", "payload": "!!", "payload_encoding": "base64"}, auth)
	assert.Equal(t, http.StatusBadRequest, code)
	code, _ = do(t, e, http.MethodPost, "/api/v1/publish", map[string]any{"topic": "bad/#", "payload": "x"}, auth)
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestUsersCRUD(t *testing.T) {
	s, e, _ := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	authH := bearer(loginToken(t, e, "admin", "s3cret"))

	// Create.
	code, body := do(t, e, http.MethodPost, "/api/v1/users",
		map[string]any{"username": "device-1", "password": "pw", "superuser": false}, authH)
	require.Equal(t, http.StatusCreated, code, "body: %v", body)
	assert.Equal(t, "device-1", body["username"])

	// Duplicate.
	code, body = do(t, e, http.MethodPost, "/api/v1/users",
		map[string]any{"username": "device-1", "password": "pw"}, authH)
	assert.Equal(t, http.StatusConflict, code)
	assert.Equal(t, "conflict", body["code"])

	// The auth cache saw the write without a manual refresh.
	res, err := s.currentAuthManager().Authenticate(t.Context(), hookConnectInfo("device-1", "pw"))
	require.NoError(t, err)
	assert.True(t, res.OK)

	// List and detail.
	code, body = do(t, e, http.MethodGet, "/api/v1/users", nil, authH)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])
	code, body = do(t, e, http.MethodGet, "/api/v1/users/device-1", nil, authH)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, false, body["superuser"])

	// Update password and flag.
	code, _ = do(t, e, http.MethodPut, "/api/v1/users/device-1",
		map[string]any{"password": "pw2", "superuser": true}, authH)
	require.Equal(t, http.StatusOK, code)
	res, err = s.currentAuthManager().Authenticate(t.Context(), hookConnectInfo("device-1", "pw2"))
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.True(t, res.Superuser)

	code, _ = do(t, e, http.MethodPut, "/api/v1/users/ghost", map[string]any{"superuser": true}, authH)
	assert.Equal(t, http.StatusNotFound, code)

	// Delete, then 404 on retry.
	code, _ = do(t, e, http.MethodDelete, "/api/v1/users/device-1", nil, authH)
	assert.Equal(t, http.StatusNoContent, code)
	code, body = do(t, e, http.MethodDelete, "/api/v1/users/device-1", nil, authH)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])
}

func TestACLsCRUD(t *testing.T) {
	s, e, _ := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	authH := bearer(loginToken(t, e, "admin", "s3cret"))

	code, body := do(t, e, http.MethodPost, "/api/v1/acls",
		map[string]any{"seq": 1, "username": "device-1", "action": "allow", "permission": "pubsub", "topic": "devices/#"}, authH)
	require.Equal(t, http.StatusCreated, code, "body: %v", body)
	id := int64(body["id"].(float64))
	require.Positive(t, id)

	// The compiled rule set saw the write.
	assert.True(t, s.currentAuthManager().Allow(t.Context(),
		hookClientInfo("device-1"), hook.OpPublish, "devices/1"))

	// Invalid rules are rejected.
	code, body = do(t, e, http.MethodPost, "/api/v1/acls",
		map[string]any{"action": "maybe", "permission": "pubsub", "topic": "a/#"}, authH)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "bad_request", body["code"])

	code, body = do(t, e, http.MethodGet, "/api/v1/acls", nil, authH)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"])

	code, _ = do(t, e, http.MethodPut, "/api/v1/acls/"+itoa(id),
		map[string]any{"seq": 2, "action": "deny", "permission": "subscribe", "topic": "devices/secret"}, authH)
	require.Equal(t, http.StatusOK, code)

	code, _ = do(t, e, http.MethodPut, "/api/v1/acls/999",
		map[string]any{"action": "allow", "permission": "publish", "topic": "a/#"}, authH)
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = do(t, e, http.MethodDelete, "/api/v1/acls/"+itoa(id), nil, authH)
	assert.Equal(t, http.StatusNoContent, code)
	code, body = do(t, e, http.MethodDelete, "/api/v1/acls/"+itoa(id), nil, authH)
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "not_found", body["code"])
}

func TestChainsEndpoints(t *testing.T) {
	s, e, _ := testFullAPI(t)
	seedUser(t, s, "admin", "s3cret")
	authH := bearer(loginToken(t, e, "admin", "s3cret"))

	// GET shows both chains with the shared db source active.
	code, body := do(t, e, http.MethodGet, "/api/v1/auth/chains", nil, authH)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	authn, _ := body["authentication"].([]any)
	require.Len(t, authn, 1)
	src, _ := authn[0].(map[string]any)
	assert.Equal(t, "db", src["type"])
	assert.Equal(t, "active", src["status"])
	assert.Equal(t, "sqlite", src["backend"])
	authz, _ := body["authorization"].([]any)
	require.Len(t, authz, 1)

	// PUT rejects a second db source per chain.
	code, body = do(t, e, http.MethodPut, "/api/v1/auth/chains", map[string]any{
		"authentication": []any{
			map[string]any{"type": "db", "dsn": "sqlite::memory:"},
			map[string]any{"type": "db", "dsn": "sqlite:other.db"},
		},
	}, authH)
	assert.Equal(t, http.StatusBadRequest, code, "body: %v", body)
	assert.Equal(t, "bad_request", body["code"])
	assert.Contains(t, body["message"], "at most one db source")

	// PUT replaces the chains; the broker swaps to the new manager.
	code, body = do(t, e, http.MethodPut, "/api/v1/auth/chains", map[string]any{
		"authentication": []any{map[string]any{"type": "db", "dsn": "sqlite::memory:"}},
		"authorization":  []any{map[string]any{"type": "db", "dsn": "sqlite::memory:"}},
	}, authH)
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	authn, _ = body["authentication"].([]any)
	require.Len(t, authn, 1)

	// Refresh works against the current manager.
	code, body = do(t, e, http.MethodPost, "/api/v1/auth/refresh", nil, authH)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, body["refreshed"])
}

func TestMaskDSN(t *testing.T) {
	assert.Equal(t, "sqlite:./data/mayfly.db", maskDSN("sqlite:./data/mayfly.db"))
	masked := maskDSN("mysql:root:secret@tcp(localhost:3306)/mayfly")
	assert.Contains(t, masked, "***")
	assert.NotContains(t, masked, "secret")
}

// TestPutChainsMaskedDSN covers the DSN round-trip through GET: the masked
// password sentinel must resolve back to the current source's real DSN
// instead of replacing it, while an explicit new DSN must take effect.
func TestPutChainsMaskedDSN(t *testing.T) {
	// The MySQL DSNs point at an unreachable port: managers build a
	// fail-closed source without needing a live database.
	const realDSN = "mysql:root:secret@tcp(127.0.0.1:1)/mayfly"
	const otherDSN = "mysql:root:other-pass@tcp(127.0.0.1:1)/mayfly"
	masked := maskDSN(realDSN)
	require.Contains(t, masked, "***")

	newServer := func(t *testing.T, dsn string) (*Server, *echo.Echo) {
		t.Helper()
		mgr, err := auth.NewManager(t.Context(), auth.Config{
			Authentication: []auth.SourceConfig{{Type: "db", Enabled: true, DSN: dsn}},
			Authorization:  []auth.SourceConfig{{Type: "db", Enabled: true, DSN: dsn}},
		}, testLogger())
		require.NoError(t, err)
		s, e := testAPI(t, func(o *Options) {
			o.AuthManager = mgr
			o.ApplyChains = func(m *auth.Manager) {}
		})
		seedUser(t, s, "admin", "s3cret")
		return s, e
	}
	putBody := func(dsn string) map[string]any {
		src := map[string]any{"type": "db", "dsn": dsn}
		return map[string]any{"authentication": []any{src}, "authorization": []any{src}}
	}

	t.Run("masked dsn round-trip keeps the real dsn", func(t *testing.T) {
		s, e := newServer(t, realDSN)
		authH := bearer(loginToken(t, e, "admin", "s3cret"))

		// GET masks the password.
		code, body := do(t, e, http.MethodGet, "/api/v1/auth/chains", nil, authH)
		require.Equal(t, http.StatusOK, code)
		authn, _ := body["authentication"].([]any)
		require.Len(t, authn, 1)
		src, _ := authn[0].(map[string]any)
		assert.Equal(t, masked, src["dsn"])

		// PUTting the masked view back must not corrupt the source.
		code, body = do(t, e, http.MethodPut, "/api/v1/auth/chains", putBody(masked), authH)
		require.Equal(t, http.StatusOK, code, "body: %v", body)
		assert.Equal(t, realDSN, s.currentAuthManager().Config().Authentication[0].DSN)
		assert.Equal(t, realDSN, s.currentAuthManager().Config().Authorization[0].DSN)
	})

	t.Run("explicit dsn change takes effect", func(t *testing.T) {
		s, e := newServer(t, realDSN)
		authH := bearer(loginToken(t, e, "admin", "s3cret"))

		code, body := do(t, e, http.MethodPut, "/api/v1/auth/chains", putBody(otherDSN), authH)
		require.Equal(t, http.StatusOK, code, "body: %v", body)
		assert.Equal(t, otherDSN, s.currentAuthManager().Config().Authentication[0].DSN)
	})

	t.Run("masked dsn without a current source is rejected", func(t *testing.T) {
		s, e := testAPI(t, func(o *Options) {
			o.ApplyChains = func(m *auth.Manager) {}
		})
		seedUser(t, s, "admin", "s3cret")
		authH := bearer(loginToken(t, e, "admin", "s3cret"))

		code, body := do(t, e, http.MethodPut, "/api/v1/auth/chains", putBody(masked), authH)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "bad_request", body["code"])
		assert.Contains(t, body["message"], "masked dsn")
	})
}

func TestUsersWithoutDBSource(t *testing.T) {
	s, e := testAPI(t, func(o *Options) {
		o.Broker = broker.New(broker.Options{Logger: testLogger()})
	})
	seedUser(t, s, "admin", "s3cret")
	authH := bearer(loginToken(t, e, "admin", "s3cret"))

	code, body := do(t, e, http.MethodGet, "/api/v1/users", nil, authH)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "no_db_source", body["code"])

	code, body = do(t, e, http.MethodPost, "/api/v1/auth/refresh", nil, authH)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "unavailable", body["code"])
}
