package listener

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testPKI carries a self-signed CA with a server and a client certificate,
// written to PEM files under the test temp dir.
type testPKI struct {
	CAFile     string
	ServerCert string
	ServerKey  string
	ClientCert string
	ClientKey  string
}

// newTestPKI issues a CA, a localhost server certificate and a client
// certificate with Common Name "device-001".
func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mayfly test CA"},
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

	serverCert, serverKey := issueCert(t, dir, "server", 2, ca, caKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	clientCert, clientKey := issueCert(t, dir, "client", 3, ca, caKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "device-001"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})

	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))

	return testPKI{
		CAFile: caFile, ServerCert: serverCert, ServerKey: serverKey,
		ClientCert: clientCert, ClientKey: clientKey,
	}
}

// issueCert signs tpl (partially filled) with the CA and writes cert/key PEM
// files named by stem, returning their paths.
func issueCert(t *testing.T, dir, stem string, serial int64, ca *x509.Certificate,
	caKey *ecdsa.PrivateKey, tpl *x509.Certificate) (certPath, keyPath string) {
	t.Helper()
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
