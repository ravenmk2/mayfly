package metrics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectPublishes records every published topic/payload pair.
type collectPublishes struct {
	mu   sync.Mutex
	seen map[string][][]byte
}

func (c *collectPublishes) publish(topic string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = map[string][][]byte{}
	}
	c.seen[topic] = append(c.seen[topic], payload)
	return nil
}

func (c *collectPublishes) rounds(topic string) [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[topic]
}

func TestSysPublisherRound(t *testing.T) {
	m := New("1.0.0")
	m.ConnectionsCurrent.Store(2)
	m.ConnectionsTotal.Store(9)
	m.BytesReceived.Add(1234)
	m.Subscriptions.Store(5)

	rec := &collectPublishes{}
	refreshed := 0
	p := NewSysPublisher(m, SysOptions{
		Publish: rec.publish,
		Refresh: func() { refreshed++ },
	})
	p.PublishNow()

	assert.Equal(t, 1, refreshed)
	for _, topic := range p.Topics() {
		require.Len(t, rec.rounds(topic), 1, "topic %s not published", topic)
	}
	assert.Equal(t, "1.0.0", string(rec.rounds(SysPrefix + "version")[0]))
	assert.Equal(t, "2", string(rec.rounds(SysPrefix + "clients/connected")[0]))
	assert.Equal(t, "9", string(rec.rounds(SysPrefix + "clients/total")[0]))
	assert.Equal(t, "1234", string(rec.rounds(SysPrefix + "bytes/received")[0]))
	assert.Equal(t, "5", string(rec.rounds(SysPrefix + "subscriptions/count")[0]))

	// Runtime sampling happens on the publish cadence.
	assert.Positive(t, m.Goroutines.Load())
	assert.NotEmpty(t, rec.rounds(SysPrefix + "runtime/heap_alloc")[0])
}

func TestSysPublisherPeriodic(t *testing.T) {
	m := New("test")
	rec := &collectPublishes{}
	p := NewSysPublisher(m, SysOptions{Interval: 20 * time.Millisecond, Publish: rec.publish})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()

	require.Eventually(t, func() bool {
		return len(rec.rounds(SysPrefix+"uptime")) >= 2
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

func TestSysPublisherDisabled(t *testing.T) {
	m := New("test")
	rec := &collectPublishes{}
	p := NewSysPublisher(m, SysOptions{Interval: -1, Publish: rec.publish})
	assert.True(t, p.Disabled())
	p.Run(context.Background()) // returns immediately
	assert.Empty(t, rec.rounds(SysPrefix+"uptime"))
}

func TestSysPublisherLogsFailures(t *testing.T) {
	m := New("test")
	p := NewSysPublisher(m, SysOptions{
		Publish: func(string, []byte) error { return errors.New("broker closed") },
	})
	// Failures are logged, not fatal.
	assert.NotPanics(t, p.PublishNow)
}
