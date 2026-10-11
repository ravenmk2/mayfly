package broker

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// transportPKI carries a self-signed CA with a server and a client
// certificate (CN "device-001") as PEM files.
type transportPKI struct {
	caFile     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

// newTransportPKI issues the certificates used by the transport integration
// tests.
func newTransportPKI(t *testing.T) transportPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mayfly transport test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	issue := func(stem string, serial int64, tpl *x509.Certificate) (certPath, keyPath string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tpl.SerialNumber = big.NewInt(serial)
		tpl.NotBefore = time.Now().Add(-time.Minute)
		tpl.NotAfter = time.Now().Add(time.Hour)
		tpl.KeyUsage = x509.KeyUsageDigitalSignature
		der, err := x509.CreateCertificate(rand.Reader, tpl, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		certPath = filepath.Join(dir, stem+".crt")
		require.NoError(t, os.WriteFile(certPath,
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
		keyDER, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		keyPath = filepath.Join(dir, stem+".key")
		require.NoError(t, os.WriteFile(keyPath,
			pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
		return certPath, keyPath
	}

	serverCert, serverKey := issue("server", 2, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	clientCert, clientKey := issue("client", 3, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "device-001"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	return transportPKI{caFile: caFile, serverCert: serverCert, serverKey: serverKey,
		clientCert: clientCert, clientKey: clientKey}
}

// trustCA returns a client TLS config trusting the test CA.
func (p transportPKI) trustCA(t *testing.T) *tls.Config {
	t.Helper()
	pemBytes, err := os.ReadFile(p.caFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes))
	return &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
}

// clientCert loads the client certificate pair.
func (p transportPKI) clientCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(p.clientCert, p.clientKey)
	require.NoError(t, err)
	return cert
}

// captureAuth records every ConnectInfo and admits the client.
type captureAuth struct {
	got chan hook.ConnectInfo
}

// Authenticate implements hook.Authenticator.
func (a *captureAuth) Authenticate(_ context.Context, info hook.ConnectInfo) (hook.Result, error) {
	a.got <- info
	return hook.Result{OK: true}, nil
}

// serveBroker runs a broker with l until the test ends.
func serveBroker(t *testing.T, l listener.Listener, mutate func(*Options)) *Server {
	t.Helper()
	s := newTestServer(t, mutate)
	s.AddListener(l)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-errCh)
		require.NoError(t, s.Close())
	})
	return s
}

func TestBrokerOverTLS(t *testing.T) {
	pki := newTransportPKI(t)
	l, err := listener.NewTLS(listener.TLSConfig{
		Address: "127.0.0.1:0", CertFile: pki.serverCert, KeyFile: pki.serverKey,
	})
	require.NoError(t, err)
	serveBroker(t, l, nil)

	nc, err := tls.Dial("tcp", l.Addr(), pki.trustCA(t))
	require.NoError(t, err)
	c := (&testClient{t: t, nc: nc, br: bufio.NewReader(nc)}).v5()
	ack := c.connect("tls-client", true, packet.Properties{})
	assert.False(t, ack.SessionPresent)
	c.close()
}

func TestBrokerOverMTLSClientCertRequired(t *testing.T) {
	pki := newTransportPKI(t)
	l, err := listener.NewTLS(listener.TLSConfig{
		Address: "127.0.0.1:0", CertFile: pki.serverCert, KeyFile: pki.serverKey,
		CAFile: pki.caFile, ClientAuth: "require_and_verify",
	})
	require.NoError(t, err)
	serveBroker(t, l, nil)

	// No client certificate: the TLS handshake fails, no CONNECT is read.
	nc, err := tls.Dial("tcp", l.Addr(), pki.trustCA(t))
	if err == nil {
		_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = nc.Read(make([]byte, 1))
		require.Error(t, err)
		_ = nc.Close()
	}

	// With the client certificate the MQTT handshake completes.
	cfg := pki.trustCA(t)
	cfg.Certificates = []tls.Certificate{pki.clientCertificate(t)}
	nc2, err := tls.Dial("tcp", l.Addr(), cfg)
	require.NoError(t, err)
	c := (&testClient{t: t, nc: nc2, br: bufio.NewReader(nc2)}).v5()
	ack := c.connect("mtls-client", true, packet.Properties{})
	assert.False(t, ack.SessionPresent)
	c.close()
}

func TestBrokerOverMTLSCNAsUsername(t *testing.T) {
	pki := newTransportPKI(t)
	l, err := listener.NewTLS(listener.TLSConfig{
		Address: "127.0.0.1:0", CertFile: pki.serverCert, KeyFile: pki.serverKey,
		CAFile: pki.caFile, ClientAuth: "require_and_verify", UseCNAsUsername: true,
	})
	require.NoError(t, err)
	auth := &captureAuth{got: make(chan hook.ConnectInfo, 1)}
	serveBroker(t, l, func(o *Options) {
		o.Authenticator = hook.NewAuthChain(auth)
	})

	cfg := pki.trustCA(t)
	cfg.Certificates = []tls.Certificate{pki.clientCertificate(t)}
	nc, err := tls.Dial("tcp", l.Addr(), cfg)
	require.NoError(t, err)
	c := (&testClient{t: t, nc: nc, br: bufio.NewReader(nc)}).v5()
	c.connect("mtls-cn-client", true, packet.Properties{})

	select {
	case info := <-auth.got:
		// The CONNECT carried no username; the certificate CN replaces it.
		assert.Equal(t, "device-001", info.Username)
		assert.Equal(t, "tls", info.ListenerID)
	case <-time.After(2 * time.Second):
		t.Fatal("authenticator was not consulted")
	}
	c.close()
}

// wsTestConn adapts a client-side websocket connection to net.Conn for the
// byte-level testClient.
type wsTestConn struct {
	ws *websocket.Conn
	r  io.Reader
}

func (c *wsTestConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			mt, r, err := c.ws.NextReader()
			if err != nil {
				return 0, io.EOF
			}
			if mt != websocket.BinaryMessage {
				continue
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if errors.Is(err, io.EOF) {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsTestConn) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsTestConn) Close() error                       { return c.ws.Close() }
func (c *wsTestConn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *wsTestConn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *wsTestConn) SetDeadline(t time.Time) error      { return c.ws.SetReadDeadline(t) }
func (c *wsTestConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsTestConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

func TestBrokerOverWS(t *testing.T) {
	l, err := listener.NewWS(listener.WSConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	serveBroker(t, l, nil)

	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	ws, _, err := d.Dial("ws://"+l.Addr()+"/mqtt", nil)
	require.NoError(t, err)
	nc := &wsTestConn{ws: ws}
	c := (&testClient{t: t, nc: nc, br: bufio.NewReader(nc)}).v5()

	ack := c.connect("ws-client", true, packet.Properties{})
	assert.False(t, ack.SessionPresent)

	// PINGREQ/PINGRESP over binary frames.
	c.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pingreq}})
	pong := c.read()
	assert.Equal(t, packet.Pingresp, pong.FixedHeader.Type)
	c.close()
}

func TestBrokerOverWSS(t *testing.T) {
	pki := newTransportPKI(t)
	l, err := listener.NewWS(listener.WSConfig{
		Address: "127.0.0.1:0",
		TLS:     &listener.TLSConfig{CertFile: pki.serverCert, KeyFile: pki.serverKey},
	})
	require.NoError(t, err)
	serveBroker(t, l, nil)

	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second, TLSClientConfig: pki.trustCA(t)}
	ws, _, err := d.Dial("wss://"+l.Addr()+"/mqtt", nil)
	require.NoError(t, err)
	nc := &wsTestConn{ws: ws}
	c := (&testClient{t: t, nc: nc, br: bufio.NewReader(nc)}).v5()
	ack := c.connect("wss-client", true, packet.Properties{})
	assert.False(t, ack.SessionPresent)
	c.close()
}
