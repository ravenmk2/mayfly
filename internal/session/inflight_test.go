package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func publishPacket(id uint16, qos byte, topic string) *packet.Packet {
	return &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: qos},
		PacketID:    id,
		TopicName:   topic,
		Payload:     []byte("data"),
	}
}

func TestOutboundQuotaAndNextImmediate(t *testing.T) {
	o := newOutboundInflight(2)

	// Two messages fit the window and are sent immediately.
	m1 := &InflightMessage{Packet: *publishPacket(1, 1, "a")}
	m2 := &InflightMessage{Packet: *publishPacket(2, 1, "b")}
	assert.True(t, o.store(m1))
	assert.True(t, o.store(m2))
	assert.True(t, m1.Sent)
	assert.True(t, m2.Sent)
	assert.Equal(t, 2, o.sent)

	// The window is full: further messages queue in FIFO order, unsent.
	m3 := &InflightMessage{Packet: *publishPacket(3, 1, "c")}
	m4 := &InflightMessage{Packet: *publishPacket(4, 1, "d")}
	assert.False(t, o.store(m3))
	assert.False(t, o.store(m4))
	assert.False(t, m3.Sent)
	assert.False(t, m4.Sent)
	assert.Equal(t, []uint16{3, 4}, o.queue)

	// Acknowledging frees one slot; the head of the queue is promoted.
	require.NotNil(t, o.remove(1))
	promoted := o.nextImmediate()
	require.NotNil(t, promoted)
	assert.Equal(t, uint16(3), promoted.Packet.PacketID)
	assert.True(t, promoted.Sent)
	assert.Equal(t, []uint16{4}, o.queue)

	// No further promotion while the window is full again.
	assert.Nil(t, o.nextImmediate())

	// Completing the window drains the queue in order.
	o.remove(2)
	o.remove(3)
	promoted = o.nextImmediate()
	require.NotNil(t, promoted)
	assert.Equal(t, uint16(4), promoted.Packet.PacketID)
	assert.Equal(t, 1, o.sent)
	assert.Empty(t, o.queue)
	assert.Nil(t, o.nextImmediate())
}

func TestOutboundRemoveQueuedMessage(t *testing.T) {
	o := newOutboundInflight(1)
	o.store(&InflightMessage{Packet: *publishPacket(1, 1, "a")})
	o.store(&InflightMessage{Packet: *publishPacket(2, 1, "b")})
	o.store(&InflightMessage{Packet: *publishPacket(3, 1, "c")})

	// Removing a queued message frees no window slot.
	removed := o.remove(2)
	require.NotNil(t, removed)
	assert.Equal(t, uint16(2), removed.Packet.PacketID)
	assert.Equal(t, []uint16{3}, o.queue)
	assert.Equal(t, 1, o.sent)

	// Removing an unknown identifier is a no-op.
	assert.Nil(t, o.remove(99))
}

func TestOutboundExpiryHeapOrder(t *testing.T) {
	base := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	o := newOutboundInflight(10)
	store := func(id uint16, after time.Duration) {
		pk := publishPacket(id, 1, "t")
		o.store(&InflightMessage{Packet: *pk, Sent: true, Expiry: base.Add(after)})
		o.sent++
	}
	store(3, 30*time.Second)
	store(1, 10*time.Second)
	store(2, 20*time.Second)
	// Equal deadlines fall back to identifier order.
	store(5, 20*time.Second)
	// A message without expiry never enters the heap.
	o.store(&InflightMessage{Packet: *publishPacket(4, 1, "t"), Sent: true})
	o.sent++

	// Not yet expired.
	assert.Nil(t, o.peekExpired(base))
	assert.Empty(t, o.dropExpired(base))

	// Expiry drops messages in deadline order, regardless of insertion order.
	expired := o.dropExpired(base.Add(25 * time.Second))
	require.Len(t, expired, 3)
	assert.Equal(t, uint16(1), expired[0].Packet.PacketID)
	assert.Equal(t, uint16(2), expired[1].Packet.PacketID)
	assert.Equal(t, uint16(5), expired[2].Packet.PacketID)
	assert.Len(t, o.messages, 2)

	// Peek reports the next expiry without dropping it.
	assert.Equal(t, uint16(3), o.peekExpired(base.Add(30*time.Second)).Packet.PacketID)
	assert.Len(t, o.messages, 2)
	assert.Equal(t, uint16(3), o.dropExpired(base.Add(31 * time.Second))[0].Packet.PacketID)
	assert.Empty(t, o.dropExpired(base.Add(time.Hour)))
}

func TestOutboundExpiryLazyPurge(t *testing.T) {
	base := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	o := newOutboundInflight(10)
	msg := &InflightMessage{Packet: *publishPacket(1, 1, "t"), Expiry: base.Add(time.Second)}
	o.store(msg)

	// Acknowledging the message leaves a stale heap node behind; the next
	// peek/drop purges it instead of reporting a ghost expiry.
	o.remove(1)
	assert.Nil(t, o.peekExpired(base.Add(time.Hour)))
	assert.Empty(t, o.dropExpired(base.Add(time.Hour)))
	assert.Empty(t, o.expiries)
}

func TestInboundInflight(t *testing.T) {
	i := newInboundInflight(2)
	i.store(publishPacket(10, 2, "a"))
	assert.True(t, i.has(10))
	assert.False(t, i.full())
	i.store(publishPacket(11, 2, "b"))
	assert.True(t, i.full())

	pk := i.take(10)
	require.NotNil(t, pk)
	assert.Equal(t, "a", pk.TopicName)
	assert.False(t, i.has(10))
	assert.Nil(t, i.take(10))
}

func TestInboundExpiry(t *testing.T) {
	base := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	i := newInboundInflight(5)
	soon := publishPacket(1, 2, "a")
	soon.Expiry = base.Add(time.Second)
	late := publishPacket(2, 2, "b")
	late.Expiry = base.Add(time.Hour)
	i.store(late)
	i.store(soon)

	assert.Equal(t, "a", i.peekExpired(base.Add(2*time.Second)).TopicName)
	expired := i.dropExpired(base.Add(2 * time.Second))
	require.Len(t, expired, 1)
	assert.Equal(t, uint16(1), expired[0].PacketID)
	assert.False(t, i.has(1))
	assert.True(t, i.has(2))
}

func TestMessageExpiry(t *testing.T) {
	now := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)

	// No property: no expiry.
	assert.True(t, messageExpiry(now, publishPacket(1, 1, "a")).IsZero())

	// The property sets now + interval.
	pk := publishPacket(1, 1, "a")
	pk.Properties.MessageExpiryFlag = true
	pk.Properties.MessageExpiry = 30
	assert.Equal(t, now.Add(30*time.Second), messageExpiry(now, pk))

	// A caller-set Expiry wins over the property.
	preset := now.Add(time.Minute)
	pk.Expiry = preset
	assert.Equal(t, preset, messageExpiry(now, pk))
}
