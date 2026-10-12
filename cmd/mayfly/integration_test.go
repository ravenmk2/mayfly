package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// freePort reserves an ephemeral TCP port on the loopback interface.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	return port
}

// httpJSON performs one JSON request against the console and decodes the
// response body.
func httpJSON(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), "body: %s", raw)
	}
	return resp.StatusCode, out
}

// waitHTTP polls url until it answers 200 or the deadline passes.
func waitHTTP(t *testing.T, url string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "console at %s never came up", url)
}

// writeMQTT encodes and writes one packet.
func writeMQTT(t *testing.T, c net.Conn, pk *packet.Packet) {
	t.Helper()
	raw, err := pk.Encode()
	require.NoError(t, err)
	_, err = c.Write(raw)
	require.NoError(t, err)
}

// readMQTT reads one packet frame.
func readMQTT(t *testing.T, c net.Conn) *packet.Packet {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
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
	pk := &packet.Packet{ProtocolVersion: packet.Version311}
	require.NoError(t, pk.Decode(raw))
	return pk
}

// mqttDial connects and authenticates one MQTT 3.1.1 client over TCP.
func mqttDial(t *testing.T, addr, clientID, username, password string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	writeMQTT(t, c, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion: packet.Version311, Clean: true, ClientIdentifier: clientID,
			UsernameFlag: username != "", Username: username,
			PasswordFlag: password != "", Password: []byte(password),
		},
	})
	ack := readMQTT(t, c)
	require.Equal(t, packet.Connack, ack.FixedHeader.Type)
	require.Equal(t, byte(0), ack.ReasonCode, "CONNACK rejected")
	return c
}

// TestSetupHotActivationFlow is the end-to-end activation scenario
// (docs/design/setup.md): an uninitialized process boots memory-only, the
// setup wizard activates the data plane without a restart (MQTT CONNECT,
// console login, publish), and a restart recovers the persisted state.
func TestSetupHotActivationFlow(t *testing.T) {
	adminAddr := "127.0.0.1:" + freePort(t)
	mqttAddr := "127.0.0.1:" + freePort(t)
	t.Setenv("MAYFLY_ADMIN_ADDR", adminAddr)
	t.Setenv("MAYFLY_SETUP_LISTEN_ADDR", mqttAddr)
	dir := t.TempDir()
	base := "http://" + adminAddr + "/api/v1"

	run := func(ctx context.Context) chan error {
		errCh := make(chan error, 1)
		go func() { errCh <- runApp(ctx, cliFlags{dataDir: dir}) }()
		return errCh
	}

	// --- First boot: uninitialized, memory-only.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := run(ctx)
	waitHTTP(t, base+"/setup/status")

	code, body := httpJSON(t, http.MethodGet, base+"/setup/status", "", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, false, body["initialized"])

	// The data plane is gated before setup.
	code, body = httpJSON(t, http.MethodGet, base+"/status", "", nil)
	assert.Equal(t, http.StatusPreconditionRequired, code)
	assert.Equal(t, "setup_required", body["code"])

	// --- Setup with SQLite; the data plane activates in-process.
	code, body = httpJSON(t, http.MethodPost, base+"/setup", "", map[string]any{
		"db":             map[string]any{"type": "sqlite"},
		"admin_password": "integration-secret",
	})
	require.Equal(t, http.StatusCreated, code, "body: %v", body)
	assert.Equal(t, true, body["initialized"])
	assert.NotContains(t, body, "restart_required")

	// Console login works without a restart (store swapped to SQL).
	code, body = httpJSON(t, http.MethodPost, base+"/login", "",
		map[string]any{"username": "admin", "password": "integration-secret"})
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	token, _ := body["token"].(string)
	require.NotEmpty(t, token)

	// The swapped-in auth manager manages MQTT users on the primary database.
	code, _ = httpJSON(t, http.MethodPost, base+"/users", token,
		map[string]any{"username": "dev", "password": "dev-secret"})
	require.Equal(t, http.StatusCreated, code)

	// The db authorization chain defaults to deny; grant dev access to it/#.
	code, _ = httpJSON(t, http.MethodPost, base+"/acls", token,
		map[string]any{"seq": 1, "username": "dev", "action": "allow", "permission": "pubsub", "topic": "it/#"})
	require.Equal(t, http.StatusCreated, code)

	// The MQTT listener came up; CONNECT authenticates against the db chain.
	c := mqttDial(t, mqttAddr, "it-1", "dev", "dev-secret")
	writeMQTT(t, c, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Subscribe, Qos: 1},
		PacketID:    1,
		Filters:     []packet.Subscription{{Filter: "it/#", Qos: 1}},
	})
	suback := readMQTT(t, c)
	require.Equal(t, packet.Suback, suback.FixedHeader.Type)
	require.Equal(t, []byte{packet.CodeGrantedQos1}, suback.ReasonCodes, "subscription granted")

	// Publish at QoS 1 through the admin API; the subscriber receives it.
	code, _ = httpJSON(t, http.MethodPost, base+"/publish", token,
		map[string]any{"topic": "it/live", "payload": "hello", "qos": 1})
	require.Equal(t, http.StatusAccepted, code)
	pub := readMQTT(t, c)
	require.Equal(t, packet.Publish, pub.FixedHeader.Type)
	assert.Equal(t, byte(1), pub.FixedHeader.Qos)
	assert.Equal(t, []byte("hello"), pub.Payload)
	writeMQTT(t, c, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Puback}, PacketID: pub.PacketID})

	// A retained message survives the restart through the store.
	code, _ = httpJSON(t, http.MethodPost, base+"/publish", token,
		map[string]any{"topic": "it/retained", "payload": "kept", "retain": true})
	require.Equal(t, http.StatusAccepted, code)

	// Graceful stop of the first instance.
	cancel()
	require.NoError(t, <-errCh)

	// --- Restart on the same data directory: state is complete.
	ctx2, cancel2 := context.WithCancel(context.Background())
	errCh2 := run(ctx2)
	waitHTTP(t, base+"/setup/status")

	code, body = httpJSON(t, http.MethodGet, base+"/setup/status", "", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, body["initialized"])

	// Setup is locked after initialization.
	code, body = httpJSON(t, http.MethodPost, base+"/setup", "", map[string]any{
		"db":             map[string]any{"type": "sqlite"},
		"admin_password": "integration-secret",
	})
	assert.Equal(t, http.StatusGone, code)
	assert.Equal(t, "gone", body["code"])

	// The admin account persisted.
	code, body = httpJSON(t, http.MethodPost, base+"/login", "",
		map[string]any{"username": "admin", "password": "integration-secret"})
	require.Equal(t, http.StatusOK, code, "body: %v", body)
	token, _ = body["token"].(string)
	require.NotEmpty(t, token)

	// The MQTT user persisted and authenticates after the restart.
	c2 := mqttDial(t, mqttAddr, "it-2", "dev", "dev-secret")
	_ = c2

	// The retained message persisted (the $SYS topics account for the rest of
	// the retained set).
	code, body = httpJSON(t, http.MethodGet, base+"/retained?topic=it/retained", token, nil)
	require.Equal(t, http.StatusOK, code)
	assert.EqualValues(t, 1, body["total"], "retained message recovered from the store")

	// Graceful stop of the second instance.
	cancel2()
	require.NoError(t, <-errCh2)
}
