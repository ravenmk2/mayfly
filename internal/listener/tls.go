package listener

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"
)

// handshakeTimeout bounds the TLS handshake of a freshly accepted
// connection so a stalled peer does not hold a connection slot forever.
const handshakeTimeout = 10 * time.Second

// TLSConfig configures a TLS listener (docs/design/listeners.md section 2).
type TLSConfig struct {
	// ID names the listener ("tls" when empty).
	ID string
	// Address is the listen address, e.g. ":8883".
	Address string
	// MaxConnections caps concurrently accepted connections; zero is
	// unlimited.
	MaxConnections int

	// CertFile and KeyFile are the server certificate chain and private key
	// (PEM).
	CertFile string
	KeyFile  string
	// CAFile is the PEM bundle verifying client certificates; required when
	// ClientAuth is require_and_verify.
	CAFile string
	// ClientAuth is "" / "none" or "require_and_verify" (mTLS).
	ClientAuth string
	// UseCNAsUsername extracts the verified client certificate Common Name
	// and presents it as the authentication username.
	UseCNAsUsername bool
}

// BuildTLSConfig loads the certificate material and returns the TLS server
// configuration. It is shared by the TLS and WSS listeners.
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("listener: load certificate: %w", err)
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if cfg.ClientAuth == "require_and_verify" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("listener: read client CA file: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("listener: client CA file %s holds no PEM certificates", cfg.CAFile)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, nil
}

// TLS is a TLS listener; with ClientAuth require_and_verify it terminates
// mTLS and can forward the certificate CN as the username.
type TLS struct {
	base *base
}

// NewTLS binds address and returns a listener ready to Serve.
func NewTLS(cfg TLSConfig) (*TLS, error) {
	tc, err := BuildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, errBind(cfg.Address, err)
	}
	id := cfg.ID
	if id == "" {
		id = TypeTLS
	}
	prepare := func(c net.Conn) (net.Conn, error) {
		_ = c.SetDeadline(time.Now().Add(handshakeTimeout))
		tlsConn := tls.Server(c, tc)
		if err := tlsConn.Handshake(); err != nil {
			return nil, fmt.Errorf("listener: TLS handshake from %s: %w", c.RemoteAddr(), err)
		}
		_ = tlsConn.SetDeadline(time.Time{})
		wrapped := &Conn{Conn: tlsConn, ListenerID: id}
		if cfg.UseCNAsUsername {
			if peer := tlsConn.ConnectionState().PeerCertificates; len(peer) > 0 {
				wrapped.CertUsername = peer[0].Subject.CommonName
			}
		}
		return wrapped, nil
	}
	return &TLS{base: newBase(id, ln, cfg.MaxConnections, prepare)}, nil
}

// ID implements Listener.
func (t *TLS) ID() string { return t.base.id }

// Addr implements Listener, returning the effective bound address.
func (t *TLS) Addr() string { return t.base.ln.Addr().String() }

// Serve implements Listener: accept, handshake, establish; handshake
// failures are closed without reaching the broker.
func (t *TLS) Serve(ctx context.Context, establish EstablishFunc) error {
	return t.base.serve(ctx, establish)
}

// Close implements Listener.
func (t *TLS) Close() error { return t.base.close() }

// AcceptCount reports the number of currently accepted connections.
func (t *TLS) AcceptCount() int64 { return t.base.acceptCount() }
