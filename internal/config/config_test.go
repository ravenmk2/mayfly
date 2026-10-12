package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeFile creates one file under the test temp dir and returns its path.
func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestDefaultIsValid(t *testing.T) {
	cfg := Default()
	cfg.normalize()
	require.NoError(t, cfg.Validate())
	assert.Equal(t, "./data", cfg.DataDir)
	require.Len(t, cfg.Listeners, 1)
	assert.Equal(t, TypeTCP, cfg.Listeners[0].Type)
	assert.Equal(t, ":1883", cfg.Listeners[0].Address)
	assert.Equal(t, ":8080", cfg.Admin.Addr)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
}

func TestLoadMissingDefaultFallsBack(t *testing.T) {
	t.Chdir(t.TempDir()) // no ./data/mayfly.yaml here
	cfg, fromFile, err := Load("")
	require.NoError(t, err)
	assert.False(t, fromFile)
	assert.Equal(t, ":1883", cfg.Listeners[0].Address)
}

func TestLoadMissingExplicitFails(t *testing.T) {
	_, _, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope.yaml")
}

func TestLoadFullYAML(t *testing.T) {
	cert := writeFile(t, "server.crt", "pem")
	key := writeFile(t, "server.key", "pem")
	ca := writeFile(t, "ca.crt", "pem")
	path := writeFile(t, "mayfly.yaml", `
data_dir: /var/lib/mayfly
listeners:
  - type: tcp
    address: ":1883"
    max_connections: 10000
  - type: tls
    address: ":8883"
    tls:
      cert_file: `+cert+`
      key_file: `+key+`
      ca_file: `+ca+`
      client_auth: require_and_verify
      use_cn_as_username: true
  - type: ws
    address: ":8083"
    path: /mqtt
    allowed_origins: ["example.com"]
  - type: wss
    id: wss-eu
    address: ":8084"
    tls:
      cert_file: `+cert+`
      key_file: `+key+`
broker:
  max_connections: 50000
  max_packet_size: 1048576
  receive_maximum: 64
  topic_alias_maximum: 16
  outbound_queue_depth: 1024
store:
  dsn: sqlite:/var/lib/mayfly/mayfly.db
auth:
  anonymous_allow: true
  refresh_interval: 10s
  authentication:
    - type: db
      dsn: sqlite:/var/lib/mayfly/mayfly.db
    - type: file
      enabled: false
      path: /etc/mayfly/users.yaml
  authorization:
    - type: db
      dsn: sqlite:/var/lib/mayfly/mayfly.db
log:
  level: debug
  format: json
  output: stdout
admin:
  addr: ":9090"
`)
	cfg, fromFile, err := Load(path)
	require.NoError(t, err)
	assert.True(t, fromFile)

	assert.Equal(t, "/var/lib/mayfly", cfg.DataDir)
	require.Len(t, cfg.Listeners, 4)
	assert.Equal(t, 10000, cfg.Listeners[0].MaxConnections)
	assert.Equal(t, "tcp", cfg.Listeners[0].ID, "id defaults to the type")
	tlsL := cfg.Listeners[1]
	assert.Equal(t, ClientAuthRequireAndVerify, tlsL.TLS.ClientAuth)
	assert.True(t, tlsL.TLS.UseCNAsUsername)
	assert.Equal(t, "wss-eu", cfg.Listeners[3].ID)

	assert.Equal(t, 50000, cfg.Broker.MaxConnections)
	assert.Equal(t, uint16(64), cfg.Broker.ReceiveMaximum)
	assert.Equal(t, "sqlite:/var/lib/mayfly/mayfly.db", cfg.Store.DSN)

	assert.True(t, cfg.Auth.AnonymousAllow)
	assert.Equal(t, Duration(10*time.Second), cfg.Auth.RefreshInterval)

	authCfg := cfg.Auth.ToAuthConfig()
	require.Len(t, authCfg.Authentication, 2)
	assert.True(t, authCfg.Authentication[0].Enabled, "enabled defaults to true")
	assert.False(t, authCfg.Authentication[1].Enabled)
	assert.Equal(t, "sqlite:/var/lib/mayfly/mayfly.db", authCfg.Authorization[0].DSN)

	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, ":9090", cfg.Admin.Addr)
}

func TestEnvOverridesFile(t *testing.T) {
	path := writeFile(t, "mayfly.yaml", `
log:
  level: warn
broker:
  max_packet_size: 1024
`)
	t.Setenv("MAYFLY_LOG_LEVEL", "debug")
	t.Setenv("MAYFLY_BROKER__MAX_PACKET_SIZE", "2048")
	t.Setenv("MAYFLY_DATA_DIR", "/tmp/data")
	t.Setenv("MAYFLY_ADMIN_ADDR", ":9999")
	t.Setenv("MAYFLY_AUTH_ANONYMOUS_ALLOW", "true")
	t.Setenv("MAYFLY_AUTH_REFRESH_INTERVAL", "30s")
	t.Setenv("MAYFLY_STORE_DSN", "")
	// Reserved namespaces are ignored, not rejected.
	t.Setenv("MAYFLY_SETUP_DB", "sqlite")
	t.Setenv("MAYFLY_TEST_MYSQL_DSN", "user@tcp(local)/db")

	cfg, _, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, 2048, cfg.Broker.MaxPacketSize)
	assert.Equal(t, "/tmp/data", cfg.DataDir)
	assert.Equal(t, ":9999", cfg.Admin.Addr)
	assert.True(t, cfg.Auth.AnonymousAllow)
	assert.Equal(t, Duration(30*time.Second), cfg.Auth.RefreshInterval)
}

func TestEnvUnknownKeyFails(t *testing.T) {
	t.Setenv("MAYFLY_BROKR_MAX_CONNECTIONS", "10") // typo
	_, _, err := Load("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAYFLY_BROKR_MAX_CONNECTIONS")
}

func TestEnvInvalidValueFails(t *testing.T) {
	t.Setenv("MAYFLY_BROKER_MAX_CONNECTIONS", "many")
	_, _, err := Load("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid integer")
}

func TestLoadUnknownYAMLFieldFails(t *testing.T) {
	path := writeFile(t, "mayfly.yaml", "log:\n  depth: debug\n")
	_, _, err := Load(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "depth")
}

func TestValidateErrors(t *testing.T) {
	cert := writeFile(t, "server.crt", "pem")
	key := writeFile(t, "server.key", "pem")
	ca := writeFile(t, "ca.crt", "pem")
	enabled := true

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"unknown listener type", func(c *Config) { c.Listeners[0].Type = "quic" }, "unknown type"},
		{"bad port", func(c *Config) { c.Listeners[0].Address = ":99999" }, "out of range"},
		{"no port", func(c *Config) { c.Listeners[0].Address = "localhost" }, "host:port"},
		{"duplicate ids", func(c *Config) {
			c.Listeners = append(c.Listeners, ListenerConfig{Type: TypeTCP, Address: ":1884"})
		}, "duplicate id"},
		{"tls without certs", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883"}}
		}, "cert_file"},
		{"tls missing cert file", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883",
				TLS: TLSConfig{CertFile: "no-such.crt", KeyFile: key}}}
		}, "no-such.crt"},
		{"mtls without ca", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883",
				TLS: TLSConfig{CertFile: cert, KeyFile: key, ClientAuth: ClientAuthRequireAndVerify}}}
		}, "ca_file"},
		{"cn username without mtls", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883",
				TLS: TLSConfig{CertFile: cert, KeyFile: key, UseCNAsUsername: true}}}
		}, "use_cn_as_username"},
		{"unknown client_auth", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883",
				TLS: TLSConfig{CertFile: cert, KeyFile: key, ClientAuth: "maybe"}}}
		}, "client_auth"},
		{"tls block on tcp", func(c *Config) {
			c.Listeners[0].TLS = TLSConfig{CertFile: cert, KeyFile: key}
		}, "only applies to tls/wss"},
		{"ws path without slash", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeWS, Address: ":8083", Path: "mqtt"}}
		}, "must start with /"},
		{"wss requires certs", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeWSS, Address: ":8084"}}
		}, "cert_file"},
		{"mtls valid", func(c *Config) {
			c.Listeners = []ListenerConfig{{Type: TypeTLS, Address: ":8883",
				TLS: TLSConfig{CertFile: cert, KeyFile: key, CAFile: ca,
					ClientAuth: ClientAuthRequireAndVerify, UseCNAsUsername: true}}}
		}, ""},
		{"oversized packet limit", func(c *Config) { c.Broker.MaxPacketSize = 1 << 30 }, "max_packet_size"},
		{"negative connections", func(c *Config) { c.Broker.MaxConnections = -1 }, "max_connections"},
		{"bad store scheme", func(c *Config) { c.Store.DSN = "postgres://x" }, "store.dsn"},
		{"file source without path", func(c *Config) {
			c.Auth.Authentication = []AuthSourceConfig{{Type: "file"}}
		}, "requires a path"},
		{"two db sources", func(c *Config) {
			c.Auth.Authentication = []AuthSourceConfig{
				{Type: "db", DSN: "sqlite:a.db"}, {Type: "db", DSN: "sqlite:b.db"}}
		}, "at most one enabled db source"},
		{"disabled db source ignored", func(c *Config) {
			disabled := false
			c.Auth.Authentication = []AuthSourceConfig{
				{Type: "db", DSN: "sqlite:a.db", Enabled: &enabled},
				{Type: "db", DSN: "sqlite:b.db", Enabled: &disabled}}
		}, ""},
		{"bad log level", func(c *Config) { c.Log.Level = "shout" }, "log.level"},
		{"bad log format", func(c *Config) { c.Log.Format = "xml" }, "log.format"},
		{"bad admin addr", func(c *Config) { c.Admin.Addr = "nope" }, "admin.addr"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mutate(cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestDurationUnmarshal(t *testing.T) {
	path := writeFile(t, "mayfly.yaml", "auth:\n  refresh_interval: 250ms\n")
	cfg, _, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, Duration(250*time.Millisecond), cfg.Auth.RefreshInterval)
}

func TestLoadDefaultFollowsDataDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mayfly.yaml"),
		[]byte("log:\n  level: debug\n"), 0o600))

	cfg, fromFile, err := LoadDefault(dir)
	require.NoError(t, err)
	assert.True(t, fromFile)
	assert.Equal(t, "debug", cfg.Log.Level)
}

func TestLoadDefaultMissingFallsBack(t *testing.T) {
	cfg, fromFile, err := LoadDefault(t.TempDir())
	require.NoError(t, err)
	assert.False(t, fromFile)
	assert.Equal(t, ":1883", cfg.Listeners[0].Address)
}

func TestDefaultPathFor(t *testing.T) {
	assert.Equal(t, filepath.Join("custom/dir", "mayfly.yaml"), DefaultPathFor("custom/dir"))
	// filepath.Join cleans the "./" prefix; the result names the same file.
	assert.Equal(t, filepath.Join("data", "mayfly.yaml"), DefaultPathFor(DefaultDataDir))
}
