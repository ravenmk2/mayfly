package listener

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// connPreparer transforms a freshly accepted connection before the establish
// callback sees it (the TLS handshake, for example). A nil preparer passes
// the connection through; a preparer error closes the connection without
// establishing it.
type connPreparer func(net.Conn) (net.Conn, error)

// base is the shared accept/drain machinery of the stream listeners (TCP and
// TLS): accept, cap, prepare, hand to establish in its own goroutine, and
// drain with a bounded wait on shutdown (docs/design/listeners.md section 2).
type base struct {
	id             string
	maxConnections int
	ln             net.Listener
	prepare        connPreparer

	seq      atomic.Uint64
	accepted atomic.Int64
	conns    sync.Map // net.Conn -> struct{}, for forced close
	wg       sync.WaitGroup

	closeOnce sync.Once
	serving   atomic.Bool
	closed    atomic.Bool
	done      chan struct{} // closed by serve on return
}

// newBase wraps the bound ln with the shared accept machinery.
func newBase(id string, ln net.Listener, maxConnections int, prepare connPreparer) *base {
	return &base{
		id:             id,
		maxConnections: maxConnections,
		ln:             ln,
		prepare:        prepare,
		done:           make(chan struct{}),
	}
}

// serve accepts connections until ctx is cancelled or close is called, then
// drains connection goroutines (bounded by drainTimeout) and returns nil.
func (b *base) serve(ctx context.Context, establish EstablishFunc) error {
	b.serving.Store(true)
	defer close(b.done)

	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = b.ln.Close()
		case <-stop:
		}
	}()
	defer close(stop)

	for {
		c, err := b.ln.Accept()
		if err != nil {
			// Accept fails when the socket is closed by ctx or close.
			b.drain()
			if ctx.Err() != nil || b.closed.Load() {
				return nil
			}
			return err
		}
		if b.maxConnections > 0 && b.accepted.Load() >= int64(b.maxConnections) {
			// Reject at the listener level: the connection is closed
			// without a CONNACK (docs/design/listeners.md section 2).
			_ = c.Close()
			continue
		}
		b.accepted.Add(1)
		b.conns.Store(c, struct{}{})
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer b.accepted.Add(-1)
			defer b.conns.Delete(c)
			nc := net.Conn(&Conn{Conn: c, ListenerID: b.id})
			if b.prepare != nil {
				prepared, err := b.prepare(c)
				if err != nil {
					_ = c.Close()
					return
				}
				nc = prepared
			}
			establish(fmt.Sprintf("%s-%d", b.id, b.seq.Add(1)), nc)
		}()
	}
}

// close stops accepting, closes every accepted connection and waits for
// serve to return.
func (b *base) close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		_ = b.ln.Close()
		b.conns.Range(func(k, _ any) bool {
			_ = k.(net.Conn).Close()
			return true
		})
	})
	if b.serving.Load() {
		<-b.done
	}
	return nil
}

// drain waits for connection goroutines to finish, bounded by drainTimeout;
// after the timeout it force-closes the accepted connections.
func (b *base) drain() {
	finished := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(drainTimeout):
		b.conns.Range(func(k, _ any) bool {
			_ = k.(net.Conn).Close()
			return true
		})
		<-finished
	}
}

// acceptCount reports the number of currently accepted connections.
func (b *base) acceptCount() int64 { return b.accepted.Load() }
