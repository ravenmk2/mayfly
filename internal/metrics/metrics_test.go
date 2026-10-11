package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountersByQoS(t *testing.T) {
	m := New("test")
	m.AddReceived(0)
	m.AddReceived(1)
	m.AddReceived(1)
	m.AddReceived(2)
	m.AddSent(2)
	m.AddSent(2)
	m.AddSent(1)

	assert.EqualValues(t, 1, m.Received(0))
	assert.EqualValues(t, 2, m.Received(1))
	assert.EqualValues(t, 1, m.Received(2))
	assert.EqualValues(t, 0, m.Sent(0))
	assert.EqualValues(t, 1, m.Sent(1))
	assert.EqualValues(t, 2, m.Sent(2))

	snap := m.Snapshot()
	assert.EqualValues(t, 4, snap.MessagesReceived)
	assert.EqualValues(t, 3, snap.MessagesSent)
	assert.Equal(t, [3]int64{1, 2, 1}, snap.ReceivedByQoS)
	assert.Equal(t, [3]int64{0, 1, 2}, snap.SentByQoS)
}

func TestClampQoS(t *testing.T) {
	m := New("test")
	m.AddReceived(7) // corrupt QoS folds into level 2 instead of panicking
	assert.EqualValues(t, 1, m.Received(2))
}

func TestSnapshotScalars(t *testing.T) {
	m := New("1.2.3")
	m.ConnectionsTotal.Add(10)
	m.ConnectionsCurrent.Add(3)
	m.MessagesDropped.Add(2)
	m.MessagesOversized.Add(1)
	m.BytesReceived.Add(100)
	m.BytesSent.Add(200)
	m.AuthSucceeded.Add(8)
	m.AuthFailures.Add(2)
	m.ACLAllowed.Add(30)
	m.ACLDenied.Add(4)
	m.StoreWriteErrors.Add(5)
	m.Subscriptions.Store(11)
	m.Retained.Store(6)
	m.Inflight.Store(7)

	snap := m.Snapshot()
	assert.Equal(t, "1.2.3", snap.Version)
	assert.GreaterOrEqual(t, snap.Uptime, time.Duration(0))
	assert.EqualValues(t, 10, snap.ConnectionsTotal)
	assert.EqualValues(t, 3, snap.ConnectionsCurrent)
	assert.EqualValues(t, 2, snap.MessagesDropped)
	assert.EqualValues(t, 1, snap.MessagesOversized)
	assert.EqualValues(t, 100, snap.BytesReceived)
	assert.EqualValues(t, 200, snap.BytesSent)
	assert.EqualValues(t, 8, snap.AuthSucceeded)
	assert.EqualValues(t, 2, snap.AuthFailures)
	assert.EqualValues(t, 30, snap.ACLAllowed)
	assert.EqualValues(t, 4, snap.ACLDenied)
	assert.EqualValues(t, 5, snap.StoreWriteErrors)
	assert.EqualValues(t, 11, snap.Subscriptions)
	assert.EqualValues(t, 6, snap.Retained)
	assert.EqualValues(t, 7, snap.Inflight)
}

func TestSampleRuntime(t *testing.T) {
	m := New("test")
	m.SampleRuntime()
	assert.Greater(t, m.Goroutines.Load(), int64(0))
	assert.Greater(t, m.HeapAlloc.Load(), int64(0))
}

func TestUptimeMonotonic(t *testing.T) {
	m := New("test")
	require.GreaterOrEqual(t, m.Uptime(time.Now()), time.Duration(0))
	assert.Positive(t, m.Uptime(time.Now().Add(time.Second)))
}
