package listener

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clientTLSConfig trusts the test CA.
func clientTLSConfig(t *testing.T, pki testPKI) *tls.Config {
	t.Helper()
	pem, err := os.ReadFile(pki.CAFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))
	return &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
}

// serveTLS starts l and reports every established connection.
func serveTLS(t *testing.T, l *TLS, seen chan<- net.Conn) {
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

func TestTLSHandshakeAndEstablish(t *testing.T) {
	pki := newTestPKI(t)
	l, err := NewTLS(TLSConfig{Address: "127.0.0.1:0", CertFile: pki.ServerCert, KeyFile: pki.ServerKey})
	require.NoError(t, err)

	seen := make(chan net.Conn, 1)
	serveTLS(t, l, seen)

	c, err := tls.Dial("tcp", l.Addr(), clientTLSConfig(t, pki))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	lc := (<-seen).(*Conn)
	assert.Equal(t, "tls", lc.ListenerID)
	assert.Empty(t, lc.CertUsername)

	// Bytes pass through the encrypted transport.
	_, err = c.Write([]byte("ping"))
	require.NoError(t, err)
	require.NoError(t, lc.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 4)
	_, err = lc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(buf))
}

func TestTLSRejectsTLS11(t *testing.T) {
	pki := newTestPKI(t)
	l, err := NewTLS(TLSConfig{Address: "127.0.0.1:0", CertFile: pki.ServerCert, KeyFile: pki.ServerKey})
	require.NoError(t, err)
	serveTLS(t, l, nil)

	cfg := clientTLSConfig(t, pki)
	cfg.MinVersion = tls.VersionTLS10
	cfg.MaxVersion = tls.VersionTLS11
	c, err := tls.Dial("tcp", l.Addr(), cfg)
	if err == nil {
		_ = c.Close()
		t.Fatal("TLS 1.1 handshake succeeded; minimum is TLS 1.2")
	}
}

func TestMTLSRequiresClientCert(t *testing.T) {
	pki := newTestPKI(t)
	l, err := NewTLS(TLSConfig{
		Address: "127.0.0.1:0", CertFile: pki.ServerCert, KeyFile: pki.ServerKey,
		CAFile: pki.CAFile, ClientAuth: "require_and_verify",
	})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveTLS(t, l, seen)

	// Without a client certificate the handshake fails server-side; the
	// client observes an alert and nothing is established.
	cfg := clientTLSConfig(t, pki)
	c, dialErr := tls.Dial("tcp", l.Addr(), cfg)
	if dialErr == nil {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := c.Read(make([]byte, 1))
		require.Error(t, err)
		_ = c.Close()
	}
	select {
	case conn := <-seen:
		_ = conn.Close()
		t.Fatal("connection established without a client certificate")
	case <-time.After(200 * time.Millisecond):
	}

	// With a certificate signed by the CA the connection is established.
	cfg.Certificates = []tls.Certificate{loadCert(t, pki.ClientCert, pki.ClientKey)}
	c2, err := tls.Dial("tcp", l.Addr(), cfg)
	require.NoError(t, err)
	defer func() { _ = c2.Close() }()
	lc := (<-seen).(*Conn)
	_ = lc.Close()
}

func TestMTLSCertUsername(t *testing.T) {
	pki := newTestPKI(t)
	l, err := NewTLS(TLSConfig{
		Address: "127.0.0.1:0", CertFile: pki.ServerCert, KeyFile: pki.ServerKey,
		CAFile: pki.CAFile, ClientAuth: "require_and_verify", UseCNAsUsername: true,
	})
	require.NoError(t, err)
	seen := make(chan net.Conn, 1)
	serveTLS(t, l, seen)

	cfg := clientTLSConfig(t, pki)
	cfg.Certificates = []tls.Certificate{loadCert(t, pki.ClientCert, pki.ClientKey)}
	c, err := tls.Dial("tcp", l.Addr(), cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	lc := (<-seen).(*Conn)
	assert.Equal(t, "device-001", lc.CertUsername)
	_ = lc.Close()
}

func loadCert(t *testing.T, certFile, keyFile string) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	return cert
}
