// Package config loads the broker configuration: a YAML file (default
// ./data/mayfly.yaml, docs/design/setup.md section 1) overlaid with
// MAYFLY_* environment variables and, at the cmd layer, CLI flags. The
// struct mirrors what cmd/mayfly assembles: listeners, broker quotas,
// store, auth chains, logging and the admin console.
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultPath is the configuration file used when --config is not given
// (docs/design/setup.md section 1).
const DefaultPath = "./data/mayfly.yaml"

// Listener types accepted in the listeners array.
const (
	TypeTCP = "tcp"
	TypeTLS = "tls"
	TypeWS  = "ws"
	TypeWSS = "wss"
)

// Client authentication modes for TLS listeners.
const (
	// ClientAuthNone does not request a client certificate.
	ClientAuthNone = "none"
	// ClientAuthRequireAndVerify requires a client certificate signed by the
	// configured CA (mTLS).
	ClientAuthRequireAndVerify = "require_and_verify"
)

// maxRemainingLength is the MQTT protocol limit for one control packet
// [MQTT-2.1.4-1]; max_packet_size above it is meaningless.
const maxRemainingLength = 268435455

// Duration is a time.Duration that unmarshals from either a Go duration
// string ("5s", "100ms") or an integer number of nanoseconds.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	if v, err := time.ParseDuration(s); err == nil {
		*d = Duration(v)
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("config: invalid duration %q", s)
	}
	*d = Duration(time.Duration(n))
	return nil
}

// ListenerConfig is one entry of the listeners array. The listener type
// selects the transport; the TLS block applies to tls and wss only
// (docs/design/listeners.md section 2).
type ListenerConfig struct {
	// Type is tcp, tls, ws or wss.
	Type string `yaml:"type"`
	// ID names the listener for logs and hook metadata; it defaults to the
	// type and must be unique.
	ID string `yaml:"id"`
	// Address is the listen address, e.g. ":1883".
	Address string `yaml:"address"`
	// MaxConnections caps concurrent connections on this listener; zero is
	// unlimited. The broker-level cap applies on top.
	MaxConnections int `yaml:"max_connections"`
	// Path is the WebSocket endpoint path (ws/wss only), default /mqtt.
	Path string `yaml:"path"`
	// AllowedOrigins lists the Origin hosts the WebSocket upgrader accepts
	// (ws/wss only). Empty accepts same-origin requests only; "*" accepts
	// any origin.
	AllowedOrigins []string `yaml:"allowed_origins"`
	// TLS configures tls/wss listeners.
	TLS TLSConfig `yaml:"tls"`
}

// TLSConfig configures the TLS transport of a listener.
type TLSConfig struct {
	// CertFile and KeyFile are the server certificate chain and private key
	// (PEM).
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// CAFile is the PEM bundle verifying client certificates; required when
	// ClientAuth is require_and_verify.
	CAFile string `yaml:"ca_file"`
	// ClientAuth is "none" (default) or "require_and_verify" (mTLS).
	ClientAuth string `yaml:"client_auth"`
	// UseCNAsUsername presents the verified client certificate Common Name
	// as the authentication username; it requires mTLS.
	UseCNAsUsername bool `yaml:"use_cn_as_username"`
}

// BrokerConfig carries the broker quotas (architecture section 5). Zero
// values select the broker defaults documented in internal/broker.
type BrokerConfig struct {
	// MaxConnections caps the global concurrent connection count.
	MaxConnections int `yaml:"max_connections"`
	// MaxPacketSize bounds one control packet in bytes (default 1 MiB).
	MaxPacketSize int `yaml:"max_packet_size"`
	// ReceiveMaximum is the inbound QoS 1/2 window advertised in CONNACK.
	ReceiveMaximum uint16 `yaml:"receive_maximum"`
	// TopicAliasMaximum is the inbound topic alias count advertised in
	// CONNACK.
	TopicAliasMaximum uint16 `yaml:"topic_alias_maximum"`
	// OutboundQueueDepth bounds the per-connection write queue.
	OutboundQueueDepth int `yaml:"outbound_queue_depth"`
}

// StoreConfig configures persistence; an empty DSN runs memory-only
// (docs/design/persistence.md).
type StoreConfig struct {
	// DSN is "sqlite:<path>" or "mysql:<driver-dsn>".
	DSN string `yaml:"dsn"`
}

// AuthSourceConfig is one chain entry, aligned with auth.SourceConfig.
// Enabled defaults to true when omitted.
type AuthSourceConfig struct {
	// Type is "file" or "db".
	Type string `yaml:"type"`
	// Enabled gates the source without removing it; nil means true.
	Enabled *bool `yaml:"enabled"`
	// Path is the file source path (.json/.yaml/.yml/.toml).
	Path string `yaml:"path"`
	// DSN is the db source DSN: "sqlite:<path>" or "mysql:<driver-dsn>".
	DSN string `yaml:"dsn"`
}

// AuthConfig is the chain assembly input, aligned with auth.Config
// (docs/design/auth.md section 3).
type AuthConfig struct {
	// AnonymousAllow admits passwordless CONNECTs that no source matched.
	AnonymousAllow bool `yaml:"anonymous_allow"`
	// RefreshInterval is the background reload cadence of file/db sources.
	RefreshInterval Duration `yaml:"refresh_interval"`
	// Authentication is the ordered authentication chain.
	Authentication []AuthSourceConfig `yaml:"authentication"`
	// Authorization is the ordered authorization chain.
	Authorization []AuthSourceConfig `yaml:"authorization"`
}

// LogConfig configures slog (docs/design/observability.md).
type LogConfig struct {
	// Level is debug, info, warn or error.
	Level string `yaml:"level"`
	// Format is text or json.
	Format string `yaml:"format"`
	// Output is stderr, stdout or a file path.
	Output string `yaml:"output"`
}

// AdminConfig configures the console/REST listener.
type AdminConfig struct {
	// Addr is the listen address of the admin HTTP server.
	Addr string `yaml:"addr"`
}

// Config is the root of mayfly.yaml.
type Config struct {
	// DataDir holds all mutable data (docs/design/setup.md section 1).
	DataDir   string           `yaml:"data_dir"`
	Listeners []ListenerConfig `yaml:"listeners"`
	Broker    BrokerConfig     `yaml:"broker"`
	Store     StoreConfig      `yaml:"store"`
	Auth      AuthConfig       `yaml:"auth"`
	Log       LogConfig        `yaml:"log"`
	Admin     AdminConfig      `yaml:"admin"`
}

// Default returns the built-in configuration used when no config file
// exists: one plaintext TCP listener, memory-only store, no auth sources
// (the broker falls back to allow-all with a warning).
func Default() *Config {
	return &Config{
		DataDir: "./data",
		Listeners: []ListenerConfig{{
			Type:    TypeTCP,
			Address: ":1883",
		}},
		Log:   LogConfig{Level: "info", Format: "text", Output: "stderr"},
		Admin: AdminConfig{Addr: ":8080"},
	}
}

// normalize fills derived defaults after file/env/flag layering.
func (c *Config) normalize() {
	for i := range c.Listeners {
		l := &c.Listeners[i]
		if l.ID == "" {
			l.ID = l.Type
		}
		if (l.Type == TypeWS || l.Type == TypeWSS) && l.Path == "" {
			l.Path = "/mqtt"
		}
		if l.TLS.ClientAuth == "" {
			l.TLS.ClientAuth = ClientAuthNone
		}
	}
}

// Validate checks the fully layered configuration and reports every problem
// with a field path so the operator can locate it in mayfly.yaml.
func (c *Config) Validate() error {
	var errs []string
	seenIDs := map[string]int{}
	for i, l := range c.Listeners {
		where := fmt.Sprintf("listeners[%d] (%s)", i, l.Type)
		if l.ID == "" {
			l.ID = l.Type
		}
		switch l.Type {
		case TypeTCP, TypeTLS, TypeWS, TypeWSS:
		default:
			errs = append(errs, fmt.Sprintf("%s: unknown type %q (want tcp, tls, ws or wss)", where, l.Type))
		}
		if prev, dup := seenIDs[l.ID]; dup {
			errs = append(errs, fmt.Sprintf("%s: duplicate id %q (already used by listeners[%d]); set a unique id", where, l.ID, prev))
		}
		seenIDs[l.ID] = i
		if err := validateAddress(l.Address); err != nil {
			errs = append(errs, fmt.Sprintf("%s: address: %v", where, err))
		}
		if l.MaxConnections < 0 {
			errs = append(errs, fmt.Sprintf("%s: max_connections must be >= 0", where))
		}
		errs = append(errs, l.validateShape(where)...)
	}
	if len(c.Listeners) == 0 {
		errs = append(errs, "listeners: at least one listener is required")
	}
	errs = append(errs, c.Broker.validate()...)
	errs = append(errs, c.Store.validate()...)
	errs = append(errs, c.Auth.validate()...)
	errs = append(errs, c.Log.validate()...)
	if err := validateAddress(c.Admin.Addr); err != nil {
		errs = append(errs, fmt.Sprintf("admin.addr: %v", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("config: invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// validateShape checks the type-specific fields of one listener.
func (l *ListenerConfig) validateShape(where string) []string {
	var errs []string
	hasTLS := l.TLS.CertFile != "" || l.TLS.KeyFile != "" || l.TLS.CAFile != "" ||
		(l.TLS.ClientAuth != "" && l.TLS.ClientAuth != ClientAuthNone) || l.TLS.UseCNAsUsername
	switch l.Type {
	case TypeTLS, TypeWSS:
		if l.TLS.CertFile == "" || l.TLS.KeyFile == "" {
			errs = append(errs, fmt.Sprintf("%s: tls.cert_file and tls.key_file are required", where))
		} else {
			errs = append(errs, requireFile(where, "tls.cert_file", l.TLS.CertFile)...)
			errs = append(errs, requireFile(where, "tls.key_file", l.TLS.KeyFile)...)
		}
		switch l.TLS.ClientAuth {
		case "", ClientAuthNone:
			if l.TLS.UseCNAsUsername {
				errs = append(errs, fmt.Sprintf("%s: tls.use_cn_as_username requires tls.client_auth: require_and_verify", where))
			}
		case ClientAuthRequireAndVerify:
			if l.TLS.CAFile == "" {
				errs = append(errs, fmt.Sprintf("%s: tls.ca_file is required with client_auth: require_and_verify", where))
			} else {
				errs = append(errs, requireFile(where, "tls.ca_file", l.TLS.CAFile)...)
			}
		default:
			errs = append(errs, fmt.Sprintf("%s: tls.client_auth must be none or require_and_verify, got %q", where, l.TLS.ClientAuth))
		}
	case TypeTCP, TypeWS:
		if hasTLS {
			errs = append(errs, fmt.Sprintf("%s: the tls block only applies to tls/wss listeners", where))
		}
	}
	if l.Type == TypeWS || l.Type == TypeWSS {
		if l.Path != "" && !strings.HasPrefix(l.Path, "/") {
			errs = append(errs, fmt.Sprintf("%s: path must start with /", where))
		}
	} else {
		if l.Path != "" {
			errs = append(errs, fmt.Sprintf("%s: path only applies to ws/wss listeners", where))
		}
		if len(l.AllowedOrigins) > 0 {
			errs = append(errs, fmt.Sprintf("%s: allowed_origins only applies to ws/wss listeners", where))
		}
	}
	return errs
}

// requireFile reports an error when path does not name an existing regular
// file.
func requireFile(where, field, path string) []string {
	info, err := os.Stat(path)
	if err != nil {
		return []string{fmt.Sprintf("%s: %s: %v", where, field, err)}
	}
	if info.IsDir() {
		return []string{fmt.Sprintf("%s: %s: %s is a directory", where, field, path)}
	}
	return nil
}

// validateAddress accepts "host:port" with a port in 0-65535; port 0 selects
// an ephemeral port (useful for tests and supervised deployments).
func validateAddress(addr string) error {
	if addr == "" {
		return fmt.Errorf("address is empty (want host:port, e.g. :1883)")
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q is not a host:port address: %v", addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("port %q out of range 0-65535", port)
	}
	return nil
}

func (b *BrokerConfig) validate() []string {
	var errs []string
	if b.MaxConnections < 0 {
		errs = append(errs, "broker.max_connections must be >= 0")
	}
	if b.MaxPacketSize < 0 || b.MaxPacketSize > maxRemainingLength {
		errs = append(errs, fmt.Sprintf("broker.max_packet_size must be between 0 and %d (the MQTT protocol limit)", maxRemainingLength))
	}
	if b.OutboundQueueDepth < 0 {
		errs = append(errs, "broker.outbound_queue_depth must be >= 0")
	}
	return errs
}

func (s *StoreConfig) validate() []string {
	if s.DSN == "" {
		return nil
	}
	scheme, _, _ := strings.Cut(s.DSN, ":")
	if scheme != "sqlite" && scheme != "mysql" {
		return []string{fmt.Sprintf("store.dsn %q lacks a sqlite:/mysql: scheme", s.DSN)}
	}
	return nil
}

func (a *AuthConfig) validate() []string {
	var errs []string
	for _, chain := range []struct {
		name    string
		sources []AuthSourceConfig
	}{{"auth.authentication", a.Authentication}, {"auth.authorization", a.Authorization}} {
		dbCount := 0
		for i, sc := range chain.sources {
			where := fmt.Sprintf("%s[%d]", chain.name, i)
			enabled := sc.Enabled == nil || *sc.Enabled
			switch sc.Type {
			case "file":
				if enabled && sc.Path == "" {
					errs = append(errs, fmt.Sprintf("%s: file source requires a path", where))
				}
			case "db":
				if enabled {
					if sc.DSN == "" {
						errs = append(errs, fmt.Sprintf("%s: db source requires a dsn", where))
					}
					dbCount++
				}
			default:
				errs = append(errs, fmt.Sprintf("%s: unknown type %q (want file or db)", where, sc.Type))
			}
		}
		if dbCount > 1 {
			errs = append(errs, fmt.Sprintf("%s allows at most one enabled db source, got %d", chain.name, dbCount))
		}
	}
	return errs
}

func (l *LogConfig) validate() []string {
	var errs []string
	switch l.Level {
	case "", "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("log.level must be debug, info, warn or error, got %q", l.Level))
	}
	switch l.Format {
	case "", "text", "json":
	default:
		errs = append(errs, fmt.Sprintf("log.format must be text or json, got %q", l.Format))
	}
	return errs
}
