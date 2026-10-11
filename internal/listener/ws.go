package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// wsReadHeaderTimeout bounds the HTTP upgrade handshake of a WS connection.
const wsReadHeaderTimeout = 10 * time.Second

// errNonBinary ends the connection when a non-binary frame arrives: MQTT
// over WebSocket MUST use binary frames [MQTT-6.0.0-1].
var errNonBinary = errors.New("listener: websocket frame is not binary")

// WSConfig configures a WebSocket listener; a non-nil TLS field makes it a
// WSS listener (docs/design/listeners.md section 2).
type WSConfig struct {
	// ID names the listener ("ws"/"wss" when empty).
	ID string
	// Address is the listen address, e.g. ":8083".
	Address string
	// MaxConnections caps concurrently accepted connections; zero is
	// unlimited.
	MaxConnections int
	// Path is the endpoint path; empty defaults to /mqtt.
	Path string
	// AllowedOrigins lists the Origin hosts the upgrader accepts. Empty
	// accepts same-origin requests only; "*" accepts any origin.
	AllowedOrigins []string
	// TLS terminates TLS in front of the WebSocket transport (WSS). Its
	// ClientAuth/UseCNAsUsername behave as on the TLS listener.
	TLS *TLSConfig
}

// WS is a WebSocket (or WebSocket-secure) listener. Binary message streams
// are adapted to net.Conn for the broker.
type WS struct {
	cfg WSConfig
	id  string
	ln  net.Listener
	srv *http.Server

	establish EstablishFunc
	seq       atomic.Uint64
	accepted  atomic.Int64
	conns     sync.Map // *wsConn -> struct{}, for forced close

	closeOnce sync.Once
	serving   atomic.Bool
	closed    atomic.Bool
	done      chan struct{} // closed by Serve on return
}

// NewWS binds address and returns a listener ready to Serve. Requests to
// paths other than the configured one receive a 404.
func NewWS(cfg WSConfig) (*WS, error) {
	raw, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, errBind(cfg.Address, err)
	}
	w := &WS{cfg: cfg, done: make(chan struct{})}
	w.ln = raw
	if cfg.Path == "" {
		w.cfg.Path = "/mqtt"
	}
	w.id = cfg.ID
	if cfg.TLS != nil {
		tc, err := BuildTLSConfig(*cfg.TLS)
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		w.ln = tls.NewListener(raw, tc)
		if w.id == "" {
			w.id = TypeWSS
		}
	} else if w.id == "" {
		w.id = TypeWS
	}
	mux := http.NewServeMux()
	mux.HandleFunc(w.cfg.Path, w.handleUpgrade)
	w.srv = &http.Server{Handler: mux, ReadHeaderTimeout: wsReadHeaderTimeout}
	return w, nil
}

// ID implements Listener.
func (w *WS) ID() string { return w.id }

// Addr implements Listener, returning the effective bound address.
func (w *WS) Addr() string { return w.ln.Addr().String() }

// Serve implements Listener: it runs the HTTP upgrade server until ctx is
// cancelled or Close is called, then closes the upgraded connections and
// returns nil.
func (w *WS) Serve(ctx context.Context, establish EstablishFunc) error {
	w.establish = establish
	w.serving.Store(true)
	defer close(w.done)

	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			w.closeConns()
			_ = w.srv.Close()
		case <-stop:
		}
	}()
	defer close(stop)

	err := w.srv.Serve(w.ln)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil || w.closed.Load() {
		return nil
	}
	return err
}

// Close implements Listener: stops accepting, sends a WebSocket close
// message to every upgraded connection and closes them.
func (w *WS) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		w.closeConns()
		_ = w.srv.Close()
	})
	if w.serving.Load() {
		<-w.done
	}
	return nil
}

// AcceptCount reports the number of currently upgraded connections.
func (w *WS) AcceptCount() int64 { return w.accepted.Load() }

// closeConns force-closes every upgraded connection.
func (w *WS) closeConns() {
	w.conns.Range(func(k, _ any) bool {
		_ = k.(*wsConn).Close()
		return true
	})
}

// handleUpgrade upgrades one HTTP request and hands the adapted connection
// to the establish callback.
func (w *WS) handleUpgrade(rw http.ResponseWriter, r *http.Request) {
	if w.cfg.MaxConnections > 0 && w.accepted.Load() >= int64(w.cfg.MaxConnections) {
		http.Error(rw, "too many connections", http.StatusServiceUnavailable)
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: w.checkOrigin}
	ws, err := upgrader.Upgrade(rw, r, nil)
	if err != nil {
		// The upgrader has already written the error response.
		return
	}
	c := newWSConn(ws)
	w.accepted.Add(1)
	w.conns.Store(c, struct{}{})
	defer w.accepted.Add(-1)
	defer w.conns.Delete(c)

	conn := &Conn{Conn: c, ListenerID: w.id}
	if w.cfg.TLS != nil && w.cfg.TLS.UseCNAsUsername && r.TLS != nil {
		if peer := r.TLS.PeerCertificates; len(peer) > 0 {
			conn.CertUsername = peer[0].Subject.CommonName
		}
	}
	w.establish(fmt.Sprintf("%s-%d", w.id, w.seq.Add(1)), conn)
}

// checkOrigin rejects cross-origin browser requests unless the origin host
// is explicitly allowed; requests without an Origin header (non-browser
// clients) are accepted.
func (w *WS) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	for _, allowed := range w.cfg.AllowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, u.Host) || strings.EqualFold(allowed, u.Hostname()) {
			return true
		}
	}
	return strings.EqualFold(u.Host, r.Host)
}

// wsConn adapts a WebSocket connection to net.Conn: reads concatenate binary
// message payloads, writes send one binary message per call, Close sends a
// close message first (docs/design/listeners.md section 2).
type wsConn struct {
	ws *websocket.Conn
	r  io.Reader // payload reader of the binary message being consumed

	closeOnce sync.Once
}

func newWSConn(ws *websocket.Conn) *wsConn {
	return &wsConn{ws: ws}
}

// Read implements net.Conn. Message boundaries are invisible to the reader;
// ping/pong frames are answered by the websocket layer. A non-binary frame
// fails the connection per [MQTT-6.0.0-1].
func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				return 0, translateWSError(err)
			}
			if mt != websocket.BinaryMessage {
				_ = c.ws.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "binary frames only"),
					time.Now().Add(time.Second))
				return 0, errNonBinary
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if errors.Is(err, io.EOF) {
			// Message exhausted; continue with the next one, returning
			// what was read so far.
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

// Write implements net.Conn: one call produces exactly one binary message.
func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, translateWSError(err)
	}
	return len(p), nil
}

// Close implements net.Conn: a normal-closure close message is sent before
// the underlying connection shuts down.
func (c *wsConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		_ = c.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
		err = c.ws.Close()
	})
	return err
}

// LocalAddr implements net.Conn.
func (c *wsConn) LocalAddr() net.Addr { return c.ws.LocalAddr() }

// RemoteAddr implements net.Conn.
func (c *wsConn) RemoteAddr() net.Addr { return c.ws.RemoteAddr() }

// SetDeadline implements net.Conn.
func (c *wsConn) SetDeadline(t time.Time) error {
	return errors.Join(c.ws.SetReadDeadline(t), c.ws.SetWriteDeadline(t))
}

// SetReadDeadline implements net.Conn; the broker drives the keepalive
// timeout through it.
func (c *wsConn) SetReadDeadline(t time.Time) error { return c.ws.SetReadDeadline(t) }

// SetWriteDeadline implements net.Conn.
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

// translateWSError maps websocket close conditions to io.EOF and keeps
// network errors (read timeouts drive the broker keepalive logic) intact.
func translateWSError(err error) error {
	var nerr net.Error
	if errors.As(err, &nerr) {
		return err
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return io.EOF
	}
	return err
}
