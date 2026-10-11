package listener

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TCPConfig configures a TCP listener.
type TCPConfig struct {
	// Address is the listen address, e.g. ":1883" or "127.0.0.1:0".
	Address string
	// MaxConnections caps concurrently accepted connections on this
	// listener; zero means unlimited. The global broker-level limit applies
	// on top.
	MaxConnections int
}

// TCP is a plain TCP listener (docs/design/listeners.md section 2).
type TCP struct {
	cfg TCPConfig

	ln       net.Listener
	seq      atomic.Uint64
	accepted atomic.Int64
	conns    sync.Map // net.Conn -> struct{}, for forced close
	wg       sync.WaitGroup

	closeOnce sync.Once
	serving   atomic.Bool
	closed    atomic.Bool
	done      chan struct{} // closed by Serve on return
}

// NewTCP binds address and returns a listener ready to Serve.
func NewTCP(cfg TCPConfig) (*TCP, error) {
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("listener: bind %s: %w", cfg.Address, err)
	}
	return &TCP{cfg: cfg, ln: ln, done: make(chan struct{})}, nil
}

// ID implements Listener.
func (t *TCP) ID() string { return "tcp" }

// Addr implements Listener, returning the effective bound address.
func (t *TCP) Addr() string { return t.ln.Addr().String() }

// Serve implements Listener. Accepted connections are handed to establish in
// their own goroutine; when the listener-level connection cap is reached the
// connection is closed immediately. Serve blocks until ctx is cancelled or
// Close is called, then drains connection goroutines (bounded by
// drainTimeout) and returns nil.
func (t *TCP) Serve(ctx context.Context, establish EstablishFunc) error {
	t.serving.Store(true)
	defer close(t.done)

	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = t.ln.Close()
		case <-stop:
		}
	}()
	defer close(stop)

	for {
		c, err := t.ln.Accept()
		if err != nil {
			// Accept fails when the socket is closed by ctx or Close.
			t.drain()
			if ctx.Err() != nil || t.closed.Load() {
				return nil
			}
			return err
		}
		if t.cfg.MaxConnections > 0 && t.accepted.Load() >= int64(t.cfg.MaxConnections) {
			// Reject at the listener level: the connection is closed
			// without a CONNACK (docs/design/listeners.md section 2).
			_ = c.Close()
			continue
		}
		t.accepted.Add(1)
		t.conns.Store(c, struct{}{})
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.accepted.Add(-1)
			defer t.conns.Delete(c)
			establish(fmt.Sprintf("%s-%d", t.ID(), t.seq.Add(1)), &Conn{Conn: c, ListenerID: t.ID()})
		}()
	}
}

// Close implements Listener: stops accepting, closes every accepted
// connection and waits for Serve to return.
func (t *TCP) Close() error {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		_ = t.ln.Close()
		t.conns.Range(func(k, _ any) bool {
			_ = k.(net.Conn).Close()
			return true
		})
	})
	if t.serving.Load() {
		<-t.done
	}
	return nil
}

// drain waits for connection goroutines to finish, bounded by drainTimeout;
// after the timeout it force-closes the accepted connections.
func (t *TCP) drain() {
	finished := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(drainTimeout):
		t.conns.Range(func(k, _ any) bool {
			_ = k.(net.Conn).Close()
			return true
		})
		<-finished
	}
}

// AcceptCount reports the number of currently accepted connections.
func (t *TCP) AcceptCount() int64 { return t.accepted.Load() }
