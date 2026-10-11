// Package listener normalizes the transports the broker serves (TCP, TLS,
// WS and WSS) to net.Conn (docs/design/listeners.md section 1).
package listener

import (
	"context"
	"fmt"
	"net"
	"time"
)

// Transport type names shared with the configuration model.
const (
	TypeTCP = "tcp"
	TypeTLS = "tls"
	TypeWS  = "ws"
	TypeWSS = "wss"
)

// EstablishFunc is called once per accepted connection, in its own
// goroutine. The id is a unique connection identifier ("<listener>-<seq>");
// the broker installs its read/write loop pair there.
type EstablishFunc func(id string, c net.Conn)

// Listener is one network entry point of the broker.
type Listener interface {
	// Serve accepts connections until ctx is cancelled or Close is called,
	// then waits for the connection goroutines to wind down before
	// returning. It returns nil on a clean shutdown.
	Serve(ctx context.Context, establish EstablishFunc) error
	// Close stops accepting and force-closes the accepted connections.
	Close() error
	// ID is the configured listener name ("tcp", "tls", ...).
	ID() string
	// Addr is the bound address.
	Addr() string
}

// Conn wraps an accepted connection with its listener metadata.
type Conn struct {
	net.Conn
	// ListenerID identifies the listener the connection arrived on.
	ListenerID string
	// CertUsername is the Common Name of the verified client certificate on
	// mTLS listeners configured with use_cn_as_username; empty otherwise.
	// The broker presents it as the authentication username.
	CertUsername string
}

// drainTimeout bounds how long Close waits for in-flight connections to
// finish before forcing them shut.
const drainTimeout = 5 * time.Second

// errBind formats a listen failure with the listener address.
func errBind(addr string, err error) error {
	return fmt.Errorf("listener: bind %s: %w", addr, err)
}
