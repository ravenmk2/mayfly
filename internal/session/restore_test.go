package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func qos1Publish(topic string) *packet.Packet {
	return &packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Publish, Qos: 1},
		ProtocolVersion: packet.Version5,
		TopicName:       topic,
		Payload:         []byte("p"),
	}
}

func TestNewRestoredClient(t *testing.T) {
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	stopped := time.Now().Truncate(time.Second)
	c := NewRestoredClient("c1", 300, created, stopped, Config{ReceiveMaximum: 10})
	assert.Equal(t, "c1", c.ID)
	assert.Equal(t, uint32(300), c.Expiry())
	assert.Equal(t, created, c.CreatedAt)
	assert.Equal(t, stopped, c.StoppedAt())
	deadline, ok := c.ExpiryDeadline()
	require.True(t, ok)
	assert.Equal(t, stopped.Add(300*time.Second), deadline)
}

func TestRestoreOutboundResendAndPromote(t *testing.T) {
	now := time.Now()
	c := NewRestoredClient("c1", 300, now, now, Config{})

	sent := InflightMessage{
		Packet: *qos1Publish("a/sent"), Sent: true, State: InflightPublish,
	}
	sent.Packet.PacketID = 5
	queued := InflightMessage{
		Packet: *qos1Publish("a/queued"), Sent: false, State: InflightPublish,
	}
	queued.Packet.PacketID = 6
	c.RestoreOutbound(sent)
	c.RestoreOutbound(queued)

	assert.Equal(t, 2, c.OutboundLen())
	assert.Equal(t, 1, c.OutboundQueued())
	assert.Equal(t, 2, c.PacketIDsInUse())

	// A sent message resends with DUP=1 [MQTT-4.4.0-1].
	resend := c.ResendInflight()
	require.Len(t, resend, 1)
	assert.Equal(t, uint16(5), resend[0].PacketID)
	assert.True(t, resend[0].FixedHeader.Dup)

	// A queued message promotes with DUP=0 on first transmission.
	promoted := c.PromoteQueued()
	require.Len(t, promoted, 1)
	assert.Equal(t, uint16(6), promoted[0].PacketID)
	assert.False(t, promoted[0].FixedHeader.Dup)
	assert.Equal(t, 0, c.OutboundQueued())

	// A new outbound publish must not reuse a restored identifier.
	out, err := c.SendPublish(now, qos1Publish("a/new"))
	require.NoError(t, err)
	assert.NotContains(t, []uint16{5, 6}, out.PacketID)
}

func TestRestoreOutboundPubrelStage(t *testing.T) {
	c := NewRestoredClient("c1", 300, time.Now(), time.Now(), Config{})
	msg := InflightMessage{
		Packet: *qos1Publish("a/q2"), Sent: true, State: InflightPubrel,
	}
	msg.Packet.FixedHeader.Qos = 2
	msg.Packet.PacketID = 9
	c.RestoreOutbound(msg)

	// Resend continues the handshake with a DUP PUBREL.
	resend := c.ResendInflight()
	require.Len(t, resend, 1)
	assert.Equal(t, packet.Pubrel, resend[0].FixedHeader.Type)
	assert.True(t, resend[0].FixedHeader.Dup)

	got, ok := c.OutboundMessage(9)
	require.True(t, ok)
	assert.Equal(t, InflightPubrel, got.State)

	assert.Nil(t, c.ReceivePubcomp(9))
	assert.Equal(t, 0, c.OutboundLen())
}

func TestRestoreInbound(t *testing.T) {
	c := NewRestoredClient("c1", 300, time.Now(), time.Now(), Config{})
	pk := qos1Publish("in/q2")
	pk.FixedHeader.Qos = 2
	pk.PacketID = 42
	c.RestoreInbound(pk)
	assert.Equal(t, 1, c.InboundLen())

	// A duplicate PUBLISH re-sends PUBREC without delivering.
	deliver, ack, err := c.ReceivePublish(time.Now(), pk)
	require.NoError(t, err)
	assert.Nil(t, deliver)
	require.NotNil(t, ack)
	assert.Equal(t, packet.Pubrec, ack.FixedHeader.Type)

	// PUBREL completes the stored handshake exactly once.
	deliver, pubcomp := c.ReceivePubrel(42)
	require.NotNil(t, deliver)
	assert.Equal(t, "in/q2", deliver.TopicName)
	assert.Equal(t, packet.Pubcomp, pubcomp.FixedHeader.Type)
	assert.Equal(t, 0, c.InboundLen())
}

func TestQueueOffline(t *testing.T) {
	now := time.Now()
	c := NewClient(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, ClientIdentifier: "c1"},
	}, Config{})
	c.MarkCreated(now)
	assert.Equal(t, now, c.CreatedAt)
	// MarkCreated is idempotent.
	c.MarkCreated(now.Add(time.Hour))
	assert.Equal(t, now, c.CreatedAt)

	// QoS 0 is never queued offline.
	_, err := c.QueueOffline(now, &packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Publish}})
	require.Error(t, err)

	msg, err := c.QueueOffline(now, qos1Publish("off/a"))
	require.NoError(t, err)
	assert.False(t, msg.Sent)
	assert.Equal(t, InflightPublish, msg.State)
	assert.NotZero(t, msg.Packet.PacketID)
	assert.Equal(t, 1, c.OutboundQueued())

	// Message Expiry is captured on the queued copy.
	expiring := qos1Publish("off/b")
	expiring.Properties = packet.Properties{MessageExpiry: 60, MessageExpiryFlag: true}
	msg2, err := c.QueueOffline(now, expiring)
	require.NoError(t, err)
	assert.Equal(t, now.Add(60*time.Second), msg2.Expiry)

	promoted := c.PromoteQueued()
	require.Len(t, promoted, 2)
	assert.Equal(t, msg.Packet.PacketID, promoted[0].PacketID)
	assert.Equal(t, msg2.Packet.PacketID, promoted[1].PacketID)
	assert.Equal(t, 0, c.OutboundQueued())
}

func TestQueueOfflineRespectsReceiveMaximum(t *testing.T) {
	// The client's Receive Maximum of 1 bounds the broker -> client window.
	c := NewClient(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, ClientIdentifier: "c1"},
		Properties:  packet.Properties{ReceiveMaximum: 1, ReceiveMaximumFlag: true},
	}, Config{})
	now := time.Now()
	m1, err := c.QueueOffline(now, qos1Publish("w/1"))
	require.NoError(t, err)
	_, err = c.QueueOffline(now, qos1Publish("w/2"))
	require.NoError(t, err)

	promoted := c.PromoteQueued()
	require.Len(t, promoted, 1, "only the free window slot is filled")
	assert.Equal(t, m1.Packet.PacketID, promoted[0].PacketID)

	// Acknowledging the first promotes the second.
	next := c.ReceivePuback(m1.Packet.PacketID)
	require.NotNil(t, next)
	assert.Equal(t, 0, c.OutboundQueued())
}

func TestMigrateFromCarriesCreatedAt(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	old := NewRestoredClient("c1", 300, created, time.Now(), Config{})
	c := NewClient(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, ClientIdentifier: "c1"},
	}, Config{})
	c.MigrateFrom(old)
	assert.Equal(t, created, c.CreatedAt)
}
