package listener

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serveWS starts l and reports every established connection.
func serveWS(t *testing.T, l *WS, seen chan<- net.Conn) {
	t.Helper()
	go func() {
		_ = l.Serve(context.Background(), func(_ string, c net.Conn) {
			if seen != nil {
				seen <- c
			}
		})
	}()
	t.Cleanup(func() { _ = l.Close() })
}

// wsURL builds the ws:// URL for l at path.
func wsURL(l *WS, scheme, path string) string {
	return scheme + "://" + l.Addr() + path
}

// dialWS connects to path, returning the connection and the HTTP response.
func dialWS(t *testing.T, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	return d.Dial(url, header)
}

func TestWSBinaryEchoAcrossFrames(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	go func() {
		_ = l.Serve(context.Background(), func(_ string, c net.Conn) {
			// Read 5 bytes spanning two frames, echo one message back.
			buf := make([]byte, 5)
			if _, err := io.ReadFull(c, buf); err != nil {
				return
			}
			_, _ = c.Write([]byte("world"))
			_ = c.Close()
		})
	}()
	t.Cleanup(func() { _ = l.Close() })

	c, resp, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	defer func() { _ = c.Close() }()

	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte("hel")))
	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte("lo")))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, msg, err := c.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, websocket.BinaryMessage, mt)
	assert.Equal(t, "world", string(msg))
}

func TestWSPathMismatch404(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0", Path: "/mqtt"})
	require.NoError(t, err)
	serveWS(t, l, nil)

	_, resp, err := dialWS(t, wsURL(l, "ws", "/other"), nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestWSCustomPath(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0", Path: "/custom"})
	require.NoError(t, err)
	serveWS(t, l, nil)

	c, _, err := dialWS(t, wsURL(l, "ws", "/custom"), nil)
	require.NoError(t, err)
	_ = c.Close()
}

func TestWSNonBinaryFrameCloses(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveWS(t, l, seen)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	srv := <-seen
	// Drain the server-side connection like the broker would.
	readErr := make(chan error, 1)
	go func() {
		_, err := srv.Read(make([]byte, 16))
		readErr <- err
	}()

	require.NoError(t, c.WriteMessage(websocket.TextMessage, []byte("not mqtt")))
	require.ErrorIs(t, <-readErr, errNonBinary)

	// The client receives a close frame with status 1003 (unsupported data).
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = c.ReadMessage()
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseUnsupportedData, closeErr.Code)
}

func TestWSOriginPolicy(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	serveWS(t, l, nil)

	// Cross-origin without allow-list: rejected with 403.
	cross := http.Header{"Origin": {"http://evil.example"}}
	_, resp, err := dialWS(t, wsURL(l, "ws", "/mqtt"), cross)
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	// Same origin: accepted.
	same := http.Header{"Origin": {"http://" + l.Addr()}}
	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), same)
	require.NoError(t, err)
	_ = c.Close()

	// No Origin header (non-browser client): accepted.
	c2, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	_ = c2.Close()
}

func TestWSAllowedOrigins(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0", AllowedOrigins: []string{"example.com"}})
	require.NoError(t, err)
	serveWS(t, l, nil)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), http.Header{"Origin": {"https://example.com"}})
	require.NoError(t, err)
	_ = c.Close()

	_, resp, err := dialWS(t, wsURL(l, "ws", "/mqtt"), http.Header{"Origin": {"https://other.com"}})
	require.Error(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestWSAllowedOriginsWildcard(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0", AllowedOrigins: []string{"*"}})
	require.NoError(t, err)
	serveWS(t, l, nil)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), http.Header{"Origin": {"https://anything.example"}})
	require.NoError(t, err)
	_ = c.Close()
}

func TestWSSHandshake(t *testing.T) {
	pki := newTestPKI(t)
	l, err := NewWS(WSConfig{
		Address: "127.0.0.1:0",
		TLS:     &TLSConfig{CertFile: pki.ServerCert, KeyFile: pki.ServerKey},
	})
	require.NoError(t, err)
	assert.Equal(t, "wss", l.ID())
	serveWS(t, l, nil)

	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second, TLSClientConfig: clientTLSConfig(t, pki)}
	c, _, err := d.Dial(wsURL(l, "wss", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
}

func TestWSCloseSendsCloseMessage(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveWS(t, l, seen)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	srv := <-seen
	require.NoError(t, srv.Close())
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = c.ReadMessage()
	var closeErr *websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	assert.Equal(t, websocket.CloseNormalClosure, closeErr.Code)
}

func TestWSReadDeadlineTimeout(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveWS(t, l, seen)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	srv := <-seen
	// The broker drives keepalive through SetReadDeadline: a timeout must
	// surface as a net.Error timeout, not EOF.
	require.NoError(t, srv.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
	_, err = srv.Read(make([]byte, 1))
	require.Error(t, err)
	nerr, ok := err.(net.Error)
	require.True(t, ok, "want net.Error, got %T (%v)", err, err)
	assert.True(t, nerr.Timeout())
	_ = srv.Close()
}

func TestWSMaxConnections(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0", MaxConnections: 1})
	require.NoError(t, err)
	release := make(chan struct{})
	go func() {
		_ = l.Serve(context.Background(), func(_ string, c net.Conn) {
			<-release
			_ = c.Close()
		})
	}()
	t.Cleanup(func() { close(release); _ = l.Close() })

	held, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = held.Close() }()
	require.Eventually(t, func() bool { return l.AcceptCount() == 1 },
		2*time.Second, 10*time.Millisecond)

	_, resp, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.Error(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestWSReadConcatenatesFrames checks the stream adaptation directly.
func TestWSReadConcatenatesFrames(t *testing.T) {
	l, err := NewWS(WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveWS(t, l, seen)

	c, _, err := dialWS(t, wsURL(l, "ws", "/mqtt"), nil)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	srv := <-seen
	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte("MQ")))
	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte("TT")))

	buf := make([]byte, 4)
	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := io.ReadFull(srv, buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "MQTT", string(buf))
	assert.True(t, strings.HasPrefix(srv.RemoteAddr().String(), "127.0.0.1"))
	_ = srv.Close()
}
