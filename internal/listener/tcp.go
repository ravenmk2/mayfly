package listener

import (
	"context"
	"net"
)

// TCPConfig configures a TCP listener.
type TCPConfig struct {
	// ID names the listener ("tcp" when empty).
	ID string
	// Address is the listen address, e.g. ":1883" or "127.0.0.1:0".
	Address string
	// MaxConnections caps concurrently accepted connections on this
	// listener; zero means unlimited. The global broker-level limit applies
	// on top.
	MaxConnections int
}

// TCP is a plain TCP listener (docs/design/listeners.md section 2).
type TCP struct {
	base *base
}

// NewTCP binds address and returns a listener ready to Serve.
func NewTCP(cfg TCPConfig) (*TCP, error) {
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, errBind(cfg.Address, err)
	}
	id := cfg.ID
	if id == "" {
		id = TypeTCP
	}
	return &TCP{base: newBase(id, ln, cfg.MaxConnections, nil)}, nil
}

// ID implements Listener.
func (t *TCP) ID() string { return t.base.id }

// Addr implements Listener, returning the effective bound address.
func (t *TCP) Addr() string { return t.base.ln.Addr().String() }

// Serve implements Listener. Accepted connections are handed to establish in
// their own goroutine; when the listener-level connection cap is reached the
// connection is closed immediately. Serve blocks until ctx is cancelled or
// Close is called, then drains connection goroutines (bounded by
// drainTimeout) and returns nil.
func (t *TCP) Serve(ctx context.Context, establish EstablishFunc) error {
	return t.base.serve(ctx, establish)
}

// Close implements Listener: stops accepting, closes every accepted
// connection and waits for Serve to return.
func (t *TCP) Close() error { return t.base.close() }

// AcceptCount reports the number of currently accepted connections.
func (t *TCP) AcceptCount() int64 { return t.base.acceptCount() }
