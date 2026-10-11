// Package listener normalizes the transports the broker serves (TCP now,
// TLS/WS/WSS later) to net.Conn (docs/design/listeners.md section 1).
package listener

import (
	"context"
	"errors"
	"net"
	"time"
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

// ErrMaxConnections is returned by TCP.Serve's accept loop when a connection
// is rejected because the listener-level connection limit is reached.
var ErrMaxConnections = errors.New("listener: max connections reached")

// Conn wraps an accepted connection with its listener metadata.
type Conn struct {
	net.Conn
	// ListenerID identifies the listener the connection arrived on.
	ListenerID string
}

// drainTimeout bounds how long Close waits for in-flight connections to
// finish before forcing them shut.
const drainTimeout = 5 * time.Second
