package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingStore wraps MemoryStore and records every applied batch.
type recordingStore struct {
	*MemoryStore
	mu      sync.Mutex
	batches [][]op
	block   chan struct{} // when non-nil, applyOps waits for it to close
	entered atomic.Int32  // applyOps invocations
	fail    atomic.Int32  // remaining applyOps calls that fail
}

func (r *recordingStore) applyOps(ctx context.Context, ops []op) error {
	r.entered.Add(1)
	if r.block != nil {
		<-r.block
	}
	if r.fail.Add(-1) >= 0 {
		return errors.New("injected failure")
	}
	r.mu.Lock()
	r.batches = append(r.batches, ops)
	r.mu.Unlock()
	return r.MemoryStore.applyOps(ctx, ops)
}

func (r *recordingStore) applied() [][]op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.batches
}

func newBatchWriter(t *testing.T, inner Store, opts BatchOptions) *BatchWriter {
	t.Helper()
	w, err := NewBatchWriter(inner, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestBatchWriterSizeTrigger(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	w := newBatchWriter(t, inner, BatchOptions{BatchSize: 3, FlushInterval: time.Hour})
	ctx := context.Background()

	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "a"}))
	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "b"}))
	require.NoError(t, w.Retained().Delete(ctx, "t/x"))

	waitFor(t, "size-triggered flush", func() bool { return len(inner.applied()) == 1 })
	assert.Len(t, inner.applied()[0], 3)

	// Pass-through reads see committed state.
	_, ok, err := w.Sessions().Get(ctx, "a")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestBatchWriterIntervalTrigger(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	w := newBatchWriter(t, inner, BatchOptions{BatchSize: 1000, FlushInterval: 20 * time.Millisecond})
	ctx := context.Background()

	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "a"}))
	waitFor(t, "interval-triggered flush", func() bool { return len(inner.applied()) == 1 })
	assert.Len(t, inner.applied()[0], 1)
}

func TestBatchWriterRetry(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	inner.fail.Store(2) // first two attempts fail, third succeeds
	w := newBatchWriter(t, inner, BatchOptions{BatchSize: 1, FlushInterval: time.Hour})
	ctx := context.Background()

	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "a"}))
	waitFor(t, "retry to succeed", func() bool { return len(inner.applied()) == 1 })
	assert.EqualValues(t, 0, w.WriteErrors.Load())
	_, ok, err := w.Sessions().Get(ctx, "a")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestBatchWriterFailureCounted(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	inner.fail.Store(100) // always fails
	w := newBatchWriter(t, inner, BatchOptions{BatchSize: 2, FlushInterval: time.Hour})
	ctx := context.Background()

	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "a"}))
	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "b"}))
	waitFor(t, "WriteErrors after retries run out", func() bool { return w.WriteErrors.Load() == 2 })
	// The broker path is not blocked: the writer keeps accepting work.
	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "c"}))
}

func TestBatchWriterBackpressure(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory(), block: make(chan struct{})}
	w := newBatchWriter(t, inner, BatchOptions{QueueSize: 1, BatchSize: 1, FlushInterval: time.Hour})
	ctx := context.Background()

	// Op 1 fills a batch immediately and blocks the run loop inside applyOps;
	// op 2 then fills the queue, so op 3 must block until op 1 unblocks.
	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "one"}))
	waitFor(t, "run loop to block in applyOps", func() bool { return inner.entered.Load() == 1 })
	require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: "two"}))

	enqueued := make(chan error, 1)
	go func() { enqueued <- w.Sessions().Put(ctx, Session{ClientID: "three"}) }()
	select {
	case <-enqueued:
		t.Fatal("enqueue on a full queue must block (backpressure)")
	case <-time.After(50 * time.Millisecond):
	}

	close(inner.block) // let the blocked apply through
	select {
	case err := <-enqueued:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("enqueue did not unblock after the flush")
	}
}

func TestBatchWriterCloseDrains(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	w, err := NewBatchWriter(inner, BatchOptions{BatchSize: 1000, FlushInterval: time.Hour})
	require.NoError(t, err)
	ctx := context.Background()

	const n = 5
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, w.Sessions().Put(ctx, Session{ClientID: id}))
	}
	require.NoError(t, w.Close())

	inner.mu.Lock()
	defer inner.mu.Unlock()
	assert.Len(t, inner.sessions, n)
}

func TestBatchWriterEnqueueAfterClose(t *testing.T) {
	inner := &recordingStore{MemoryStore: NewMemory()}
	w, err := NewBatchWriter(inner, BatchOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	assert.Error(t, w.Sessions().Put(context.Background(), Session{ClientID: "late"}))
}

func TestBatchWriterRejectsNonBatchStore(t *testing.T) {
	_, err := NewBatchWriter(nil, BatchOptions{})
	assert.Error(t, err)
}
