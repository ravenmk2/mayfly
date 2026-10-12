// Package broker assembles the MQTT server: listener wiring, connection
// lifecycle, the per-connection read/write goroutine pair with a single
// bounded outbound queue (architecture decision A2), the delivery engine and
// the housekeeping loop driving the expiry min-heaps (decision A9).
package broker

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/metrics"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
	"github.com/ravenmk2/mayfly/internal/store"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// Defaults for the broker quotas (architecture section 5).
const (
	// DefaultMaxPacketSize bounds one MQTT control packet [MQTT-3.1.2-11].
	DefaultMaxPacketSize = 1 << 20 // 1 MiB
	// DefaultOutboundQueueDepth bounds the per-connection write queue.
	DefaultOutboundQueueDepth = 1024
	// DefaultReceiveMaximum is the inflight window advertised in CONNACK.
	DefaultReceiveMaximum = 64
	// DefaultTopicAliasMaximum is the inbound alias count advertised in
	// CONNACK.
	DefaultTopicAliasMaximum = 16
	// DefaultReadBufferSize is the per-connection read buffer.
	DefaultReadBufferSize = 4096
	// DefaultFlushInterval is how long the write loop waits to batch
	// packets before flushing.
	DefaultFlushInterval = 2 * time.Millisecond
	// DefaultConnectTimeout bounds the wait for the CONNECT packet on a new
	// connection.
	DefaultConnectTimeout = 10 * time.Second
	// DefaultHousekeepingInterval is the tick of the expiry sweeps.
	DefaultHousekeepingInterval = time.Second
)

// Options carries the broker configuration. Zero values select the
// defaults; the configuration system of a later milestone fills these from
// file and environment.
type Options struct {
	// MaxConnections caps the global number of concurrent connections;
	// zero is unlimited.
	MaxConnections int
	// MaxPacketSize bounds one control packet in bytes.
	MaxPacketSize int
	// OutboundQueueDepth bounds the per-connection write queue. QoS 0
	// messages are dropped when it is full; QoS 1/2 wait in inflight.
	OutboundQueueDepth int
	// ReceiveMaximum bounds concurrent unacknowledged inbound QoS 1/2
	// PUBLISH packets per client, advertised in CONNACK.
	ReceiveMaximum uint16
	// TopicAliasMaximum bounds inbound topic aliases, advertised in
	// CONNACK.
	TopicAliasMaximum uint16
	// ReadBufferSize is the per-connection read buffer size.
	ReadBufferSize int
	// FlushInterval is the write loop batching window.
	FlushInterval time.Duration
	// ConnectTimeout bounds the wait for CONNECT on a new connection.
	ConnectTimeout time.Duration
	// HousekeepingInterval is the expiry sweep tick.
	HousekeepingInterval time.Duration
	// Authenticator is the authentication chain; nil selects the allow-all
	// stub.
	Authenticator hook.Authenticator
	// Authorizer is the authorization chain; nil selects the allow-all
	// stub.
	Authorizer hook.Gate
	// Store persists sessions, subscriptions, inflight messages and retained
	// messages (docs/design/persistence.md); nil runs memory-only. Wrap the
	// store in a store.BatchWriter for asynchronous batched writes. When set,
	// New synchronously recovers the persisted state before any listener
	// accepts connections; a store attached later via SetStore (hot activation
	// after the setup wizard) is recovered the same way.
	Store store.Store
	// Metrics receives the broker counters (docs/design/observability.md);
	// nil selects a fresh zero-value set.
	Metrics *metrics.Metrics
	// Logger receives broker log records; nil selects slog.Default().
	Logger *slog.Logger
}

func (o *Options) normalize() {
	if o.MaxPacketSize <= 0 {
		o.MaxPacketSize = DefaultMaxPacketSize
	}
	if o.OutboundQueueDepth <= 0 {
		o.OutboundQueueDepth = DefaultOutboundQueueDepth
	}
	if o.ReceiveMaximum == 0 {
		o.ReceiveMaximum = DefaultReceiveMaximum
	}
	if o.TopicAliasMaximum == 0 {
		o.TopicAliasMaximum = DefaultTopicAliasMaximum
	}
	if o.ReadBufferSize <= 0 {
		o.ReadBufferSize = DefaultReadBufferSize
	}
	if o.FlushInterval < 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.HousekeepingInterval <= 0 {
		o.HousekeepingInterval = DefaultHousekeepingInterval
	}
	if o.Authenticator == nil {
		o.Authenticator = hook.NewAuthChain(hook.AllowAll{})
	}
	if o.Authorizer == nil {
		o.Authorizer = hook.NewAuthzChain(hook.AllowAll{})
	}
	if o.Metrics == nil {
		o.Metrics = metrics.New("")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// sessionExpiryItem is one node of the disconnected-session expiry min-heap
// (decision A9).
type sessionExpiryItem struct {
	clientID string
	deadline time.Time
}

type sessionExpiryHeap []sessionExpiryItem

func (h sessionExpiryHeap) Len() int           { return len(h) }
func (h sessionExpiryHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h sessionExpiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sessionExpiryHeap) Push(x any)        { *h = append(*h, x.(sessionExpiryItem)) }
func (h *sessionExpiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Server is the MQTT broker: listeners, client registry, subscription index
// and housekeeping.
type Server struct {
	opts   Options
	log    *slog.Logger
	topics *topics.Index
	authn  hook.Authenticator
	authz  hook.Gate
	wills  session.WillScheduler
	store  store.Store

	// Stats groups the exported counters (docs/design/observability.md).
	Stats *metrics.Metrics

	mu        sync.RWMutex
	clients   map[string]*conn // client ID -> connection (offline sessions included)
	expiries  sessionExpiryHeap
	listeners []listener.Listener

	closing   atomic.Bool
	wg        sync.WaitGroup // connection goroutines
	runCtx    context.Context
	cancel    context.CancelFunc
	listenErr []error
	runDone   chan struct{}
	closeOnce sync.Once

	// now is the clock; tests substitute it to control expiry.
	now func() time.Time
}

// New builds a Server with normalized options. When a Store is configured
// the persisted state is recovered synchronously, so sessions, subscriptions,
// inflight messages and retained messages are complete before the first
// listener accepts a connection.
func New(opts Options) *Server {
	opts.normalize()
	s := &Server{
		opts:    opts,
		log:     opts.Logger,
		topics:  topics.New(),
		authn:   opts.Authenticator,
		authz:   opts.Authorizer,
		store:   opts.Store,
		Stats:   opts.Metrics,
		clients: make(map[string]*conn),
		runDone: make(chan struct{}),
		now:     time.Now,
	}
	if s.store != nil {
		s.recoverStore()
	} else {
		s.log.Info("no store configured; session state is memory-only and lost on restart")
	}
	return s
}

// AddListener registers a listener. When the broker is already running
// (Start or Run was called, e.g. the data plane activated after the setup
// wizard) the listener is served immediately; otherwise Start or Run serves
// it.
func (s *Server) AddListener(l listener.Listener) {
	s.mu.Lock()
	s.listeners = append(s.listeners, l)
	ctx := s.runCtx
	s.mu.Unlock()
	if ctx != nil {
		s.serveAsync(ctx, l)
	}
}

// SetStore attaches the persistence store at runtime (hot activation after
// the setup wizard) and synchronously recovers the persisted state, like New
// does for a store passed via Options. It is a no-op when a store is already
// attached and reports whether st was attached.
func (s *Server) SetStore(st store.Store) bool {
	if st == nil {
		return false
	}
	s.mu.Lock()
	if s.store != nil {
		s.mu.Unlock()
		return false
	}
	s.store = st
	s.mu.Unlock()
	s.recoverStore()
	return true
}

// getStore returns the attached persistence store, nil when memory-only.
func (s *Server) getStore() store.Store {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store
}

// Topics exposes the subscription index.
func (s *Server) Topics() *topics.Index { return s.topics }

// ClientCount reports the number of registered sessions, connected or
// persisted.
func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// ErrNoListeners is returned by Run when no listener was registered.
var ErrNoListeners = errors.New("broker: no listeners configured")

// Start begins the broker runtime without blocking: housekeeping, the
// shutdown watcher and every registered listener. It is idempotent and may
// be called with zero listeners (pre-setup), in which case AddListener
// serves listeners as they are registered later. Close or ctx cancellation
// shuts the broker down gracefully (see Run).
func (s *Server) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.runCtx = ctx
	s.cancel = cancel
	listeners := append([]listener.Listener(nil), s.listeners...)
	s.mu.Unlock()

	go s.housekeeping(ctx)
	go func() {
		<-ctx.Done()
		s.shutdown()
		close(s.runDone)
	}()
	for _, l := range listeners {
		s.serveAsync(ctx, l)
	}
}

// serveAsync serves one listener in its own goroutine; a serve error is
// recorded for Run and logged.
func (s *Server) serveAsync(ctx context.Context, l listener.Listener) {
	s.log.Info("listening", "listener", l.ID(), "addr", l.Addr())
	go func() {
		if err := l.Serve(ctx, s.ServeConn); err != nil && !s.closing.Load() {
			s.mu.Lock()
			s.listenErr = append(s.listenErr, err)
			s.mu.Unlock()
			s.log.Error("listener serve failed", "listener", l.ID(), "error", err)
		}
	}()
}

// Run serves the registered listeners until ctx is cancelled or Close is
// called, then shuts down gracefully: listeners stop accepting, connected v5
// clients are notified with DISCONNECT 0x8B (Server shutting down), and
// connection goroutines are waited on. Listeners registered via AddListener
// while Run is active are served immediately.
func (s *Server) Run(ctx context.Context) error {
	s.mu.RLock()
	empty := len(s.listeners) == 0
	s.mu.RUnlock()
	if empty {
		return ErrNoListeners
	}
	s.Start(ctx)
	<-s.runDone
	s.mu.RLock()
	defer s.mu.RUnlock()
	return errors.Join(s.listenErr...)
}

// Close initiates a graceful shutdown and waits for Run to return. It is a
// no-op when Run was never started.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.RLock()
		cancel := s.cancel
		s.mu.RUnlock()
		if cancel == nil {
			return
		}
		cancel()
		<-s.runDone
	})
	return nil
}

// shutdown stops accepting, notifies connected clients and closes all
// connections, then waits for their goroutines.
func (s *Server) shutdown() {
	if !s.closing.CompareAndSwap(false, true) {
		return
	}
	s.mu.RLock()
	listeners := append([]listener.Listener(nil), s.listeners...)
	s.mu.RUnlock()
	for _, l := range listeners {
		_ = l.Close()
	}
	s.mu.RLock()
	conns := make([]*conn, 0, len(s.clients))
	for _, c := range s.clients {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	// Notify connected v5 clients before closing [MQTT-3.14.2-3 allows the
	// server to send DISCONNECT 0x8B]; v3 has no server DISCONNECT.
	for _, c := range conns {
		if cl := c.sessionClient(); cl != nil && cl.ProtocolVersion == packet.Version5 && !c.isStopped() {
			c.enqueue(&packet.Packet{
				FixedHeader:     packet.FixedHeader{Type: packet.Disconnect},
				ProtocolVersion: packet.Version5,
				ReasonCode:      packet.CodeServerShuttingDown,
			})
		}
	}
	for _, c := range conns {
		c.stop()
	}
	s.wg.Wait()
}

// ServeConn installs the read/write goroutine pair on an accepted
// connection. It is the listener.EstablishFunc entry point and is exported
// so tests can drive the broker over net.Pipe.
func (s *Server) ServeConn(id string, nc net.Conn) {
	if s.closing.Load() {
		_ = nc.Close()
		return
	}
	if max := s.opts.MaxConnections; max > 0 &&
		s.Stats.ConnectionsCurrent.Load() >= int64(max) {
		// The global connection cap is reached: refuse the connection
		// [MQTT-3.1.4-2 permits closing without a CONNACK].
		_ = nc.Close()
		return
	}
	c := newConn(s, id, nc)
	s.Stats.ConnectionsTotal.Add(1)
	s.Stats.ConnectionsCurrent.Add(1)
	s.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
}

// housekeeping runs the periodic expiry sweeps (decision A9): delayed Will
// Messages, inflight message expiry and disconnected-session expiry.
func (s *Server) housekeeping(ctx context.Context) {
	t := time.NewTicker(s.opts.HousekeepingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.sweep(now)
		}
	}
}

// sweep performs one housekeeping pass of the expiry min-heaps.
func (s *Server) sweep(now time.Time) {
	for _, w := range s.wills.Due(now) {
		s.fireWill(&w.Message)
	}

	s.mu.RLock()
	conns := make([]*conn, 0, len(s.clients))
	for _, c := range s.clients {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	for _, c := range conns {
		cl := c.sessionClient()
		if cl == nil {
			continue
		}
		for _, msg := range cl.DropExpiredOutbound(now) {
			s.storeDeleteInflight(cl.ID, store.Outbound, msg.Packet.PacketID)
		}
		for _, pk := range cl.DropExpiredInbound(now) {
			s.storeDeleteInflight(cl.ID, store.Inbound, pk.PacketID)
		}
	}

	for {
		s.mu.Lock()
		if len(s.expiries) == 0 || s.expiries[0].deadline.After(now) {
			s.mu.Unlock()
			return
		}
		item := heap.Pop(&s.expiries).(sessionExpiryItem)
		c, ok := s.clients[item.clientID]
		if !ok || c.sessionClient() == nil || c.sessionClient().TakenOver() {
			s.mu.Unlock()
			continue
		}
		cl := c.sessionClient()
		// A stale entry (the client reconnected or the expiry changed) is
		// recognized by a mismatching deadline.
		if deadline, ok := cl.ExpiryDeadline(); !ok || !deadline.Equal(item.deadline) {
			s.mu.Unlock()
			continue
		}
		delete(s.clients, item.clientID)
		s.removeSubscriptionsLocked(cl)
		s.wills.Cancel(item.clientID)
		s.mu.Unlock()
		c.suppressWill.Store(true)
		c.stop()
		s.storeDeleteSessionState(item.clientID)
		s.log.Debug("session expired", "client", item.clientID)
	}
}

// scheduleSessionExpiryLocked pushes a disconnected session onto the expiry
// heap. Callers hold s.mu.
func (s *Server) scheduleSessionExpiryLocked(cl *session.Client) {
	deadline, ok := cl.ExpiryDeadline()
	if !ok {
		return
	}
	heap.Push(&s.expiries, sessionExpiryItem{clientID: cl.ID, deadline: deadline})
}

// removeSubscriptionsLocked drops every subscription of the session from the
// trie. Callers hold s.mu; the trie has its own locking.
func (s *Server) removeSubscriptionsLocked(cl *session.Client) {
	for _, sub := range cl.Subscriptions() {
		if err := s.topics.Unsubscribe(cl.ID, fullFilter(sub)); err != nil {
			s.log.Warn("removing subscription failed", "client", cl.ID,
				"filter", sub.Filter, "error", err)
		}
	}
}

// fullFilter reconstructs the on-the-wire filter of a stored subscription,
// re-attaching the shared subscription prefix.
func fullFilter(sub topics.Subscription) string {
	if sub.Shared != "" {
		return "$share/" + sub.Shared + "/" + sub.Filter
	}
	return sub.Filter
}

// lookupConn returns the connection registered for clientID, or nil.
func (s *Server) lookupConn(clientID string) *conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clients[clientID]
}
