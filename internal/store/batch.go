package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for the asynchronous batch writer (docs/design/persistence.md
// section 3).
const (
	// DefaultQueueSize bounds the pending write queue; a full queue blocks
	// the caller (backpressure) rather than dropping writes.
	DefaultQueueSize = 4096
	// DefaultFlushInterval is how long the writer waits to fill a batch.
	DefaultFlushInterval = 100 * time.Millisecond
	// DefaultBatchSize flushes as soon as this many ops are pending.
	DefaultBatchSize = 500
	// writeRetries is how many times a failed batch is retried before it is
	// dropped and counted in WriteErrors.
	writeRetries = 3
	// retryBackoff is the base delay between batch retries.
	retryBackoff = 50 * time.Millisecond
)

// opKind enumerates the write operations the BatchWriter batches.
type opKind int

const (
	opPutSession opKind = iota
	opDeleteSession
	opPutSubscription
	opDeleteSubscription
	opDeleteSubscriptionsByClient
	opPutInflight
	opDeleteInflight
	opDeleteInflightByClient
	opPutRetained
	opDeleteRetained
)

// String names the op for error messages.
func (k opKind) String() string {
	switch k {
	case opPutSession:
		return "put session"
	case opDeleteSession:
		return "delete session"
	case opPutSubscription:
		return "put subscription"
	case opDeleteSubscription:
		return "delete subscription"
	case opDeleteSubscriptionsByClient:
		return "delete subscriptions by client"
	case opPutInflight:
		return "put inflight"
	case opDeleteInflight:
		return "delete inflight"
	case opDeleteInflightByClient:
		return "delete inflight by client"
	case opPutRetained:
		return "put retained"
	case opDeleteRetained:
		return "delete retained"
	default:
		return "unknown op"
	}
}

// op is one queued write. Only the field matching kind is populated.
type op struct {
	kind     opKind
	session  Session
	sub      Subscription
	inflight InflightMessage
	retained RetainedMessage
}

// batchStore is implemented by stores able to apply a batch of write ops
// atomically (one transaction for the SQL store).
type batchStore interface {
	applyOps(ctx context.Context, ops []op) error
}

// BatchOptions tunes the BatchWriter; zero values select the defaults.
type BatchOptions struct {
	// QueueSize bounds the pending write queue (default DefaultQueueSize).
	QueueSize int
	// FlushInterval is the time-based flush trigger (default 100ms).
	FlushInterval time.Duration
	// BatchSize is the size-based flush trigger (default 500).
	BatchSize int
	// Logger receives write failure records; nil selects slog.Default().
	Logger *slog.Logger
}

// BatchWriter fronts a Store with an asynchronous bounded write queue
// (docs/design/persistence.md section 3): Put/Delete enqueue and return,
// flush happens on interval or batch size, and each batch commits in one
// transaction. A full queue blocks the caller, forming backpressure. A batch
// that still fails after the retries is dropped and counted in WriteErrors;
// the broker is never blocked by persistence failures. Reads (Get/Iterate)
// pass through to the inner store and do not reflect queued writes. Ops
// flowing through the single queue stay ordered, so writes touching one
// entity are applied in order.
type BatchWriter struct {
	inner Store
	bs    batchStore
	log   *slog.Logger

	queue   chan op
	done    chan struct{}
	stopped chan struct{}

	batchSize     int
	flushInterval time.Duration

	closeOnce sync.Once
	closed    atomic.Bool

	// WriteErrors counts ops dropped after the batch retries ran out.
	WriteErrors atomic.Int64
}

// NewBatchWriter wraps inner and starts the flush goroutine. inner must
// support batched application (MemoryStore and SQLStore do).
func NewBatchWriter(inner Store, opts BatchOptions) (*BatchWriter, error) {
	bs, ok := inner.(batchStore)
	if !ok {
		return nil, errors.New("store: inner store does not support batched writes")
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	w := &BatchWriter{
		inner:         inner,
		bs:            bs,
		log:           opts.Logger,
		queue:         make(chan op, opts.QueueSize),
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
		batchSize:     opts.BatchSize,
		flushInterval: opts.FlushInterval,
	}
	go w.run()
	return w, nil
}

// Sessions returns the asynchronous session store view.
func (w *BatchWriter) Sessions() SessionStore { return batchSessions{w} }

// Subscriptions returns the asynchronous subscription store view.
func (w *BatchWriter) Subscriptions() SubscriptionStore { return batchSubs{w} }

// Inflight returns the asynchronous inflight store view.
func (w *BatchWriter) Inflight() InflightStore { return batchInflight{w} }

// Retained returns the asynchronous retained store view.
func (w *BatchWriter) Retained() RetainedStore { return batchRetained{w} }

// Close drains the queue, applies the final batch and closes the inner
// store. Ops enqueued after the writer stopped are dropped.
func (w *BatchWriter) Close() error {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.done)
		<-w.stopped
	})
	return w.inner.Close()
}

// enqueue queues one write, blocking while the queue is full (backpressure).
// After Close the op is rejected instead of blocking forever.
func (w *BatchWriter) enqueue(o op) error {
	if w.closed.Load() {
		return errors.New("store: batch writer closed")
	}
	select {
	case w.queue <- o:
		return nil
	case <-w.stopped:
		return errors.New("store: batch writer closed")
	}
}

// run collects ops and flushes them on batch size, interval or Close.
func (w *BatchWriter) run() {
	defer close(w.stopped)
	var batch []op
	var timer *time.Timer
	var timerC <-chan time.Time

	flush := func() {
		if timer != nil {
			timer.Stop()
		}
		timerC = nil
		if len(batch) == 0 {
			return
		}
		w.apply(batch)
		batch = nil
	}

	for {
		select {
		case o := <-w.queue:
			batch = append(batch, o)
			if len(batch) >= w.batchSize {
				flush()
			} else if timerC == nil {
				if timer == nil {
					timer = time.NewTimer(w.flushInterval)
				} else {
					timer.Reset(w.flushInterval)
				}
				timerC = timer.C
			}
		case <-timerC:
			flush()
		case <-w.done:
			for {
				select {
				case o := <-w.queue:
					batch = append(batch, o)
				default:
					flush()
					return
				}
			}
		}
	}
}

// apply commits one batch, retrying failed attempts before giving up. The
// broker is never blocked: failures surface only as WriteErrors and a log
// record (docs/design/persistence.md section 3).
func (w *BatchWriter) apply(batch []op) {
	var err error
	for attempt := 0; ; attempt++ {
		if err = w.bs.applyOps(context.Background(), batch); err == nil {
			return
		}
		if attempt >= writeRetries {
			break
		}
		time.Sleep(retryBackoff * time.Duration(attempt+1))
	}
	w.WriteErrors.Add(int64(len(batch)))
	w.log.Error("store batch write failed, dropping batch", "ops", len(batch), "error", err)
}

type batchSessions struct{ w *BatchWriter }

func (b batchSessions) Put(_ context.Context, s Session) error {
	return b.w.enqueue(op{kind: opPutSession, session: s})
}

func (b batchSessions) Delete(_ context.Context, clientID string) error {
	return b.w.enqueue(op{kind: opDeleteSession, session: Session{ClientID: clientID}})
}

func (b batchSessions) Get(ctx context.Context, clientID string) (Session, bool, error) {
	return b.w.inner.Sessions().Get(ctx, clientID)
}

func (b batchSessions) Iterate(ctx context.Context, fn func(Session) bool) error {
	return b.w.inner.Sessions().Iterate(ctx, fn)
}

type batchSubs struct{ w *BatchWriter }

func (b batchSubs) Put(_ context.Context, sub Subscription) error {
	return b.w.enqueue(op{kind: opPutSubscription, sub: sub})
}

func (b batchSubs) Delete(_ context.Context, clientID, filter string) error {
	return b.w.enqueue(op{kind: opDeleteSubscription, sub: Subscription{ClientID: clientID, Filter: filter}})
}

func (b batchSubs) DeleteByClient(_ context.Context, clientID string) error {
	return b.w.enqueue(op{kind: opDeleteSubscriptionsByClient, sub: Subscription{ClientID: clientID}})
}

func (b batchSubs) Iterate(ctx context.Context, fn func(Subscription) bool) error {
	return b.w.inner.Subscriptions().Iterate(ctx, fn)
}

type batchInflight struct{ w *BatchWriter }

func (b batchInflight) Put(_ context.Context, msg InflightMessage) error {
	return b.w.enqueue(op{kind: opPutInflight, inflight: msg})
}

func (b batchInflight) Delete(_ context.Context, clientID string, direction Direction, packetID uint16) error {
	return b.w.enqueue(op{kind: opDeleteInflight,
		inflight: InflightMessage{ClientID: clientID, Direction: direction, PacketID: packetID}})
}

func (b batchInflight) DeleteByClient(_ context.Context, clientID string) error {
	return b.w.enqueue(op{kind: opDeleteInflightByClient, inflight: InflightMessage{ClientID: clientID}})
}

func (b batchInflight) Iterate(ctx context.Context, fn func(InflightMessage) bool) error {
	return b.w.inner.Inflight().Iterate(ctx, fn)
}

type batchRetained struct{ w *BatchWriter }

func (b batchRetained) Put(_ context.Context, msg RetainedMessage) error {
	return b.w.enqueue(op{kind: opPutRetained, retained: msg})
}

func (b batchRetained) Delete(_ context.Context, topic string) error {
	return b.w.enqueue(op{kind: opDeleteRetained, retained: RetainedMessage{Topic: topic}})
}

func (b batchRetained) Iterate(ctx context.Context, fn func(RetainedMessage) bool) error {
	return b.w.inner.Retained().Iterate(ctx, fn)
}
