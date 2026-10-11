package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

func migrateClient(t *testing.T, id string) *Client {
	t.Helper()
	return NewClient(&packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Connect},
		ProtocolVersion: packet.Version5,
		Connect:         packet.ConnectParams{ClientIdentifier: id, ProtocolVersion: packet.Version5},
	}, Config{ReceiveMaximum: 2, TopicAliasMaximum: 4})
}

func TestMigrateFromTransfersDurableState(t *testing.T) {
	old := migrateClient(t, "c1")
	old.AddSubscription(topics.Subscription{Filter: "a/b", Qos: 1})

	// Park an outbound QoS 1 message in the inflight window.
	out, err := old.SendPublish(time.Now(), &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1},
		TopicName:   "a/b",
		Payload:     []byte("x"),
	})
	require.NoError(t, err)
	require.NotNil(t, out)

	// Park an inbound QoS 2 PUBLISH awaiting PUBREL.
	_, ack, err := old.ReceivePublish(time.Now(), &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 2},
		TopicName:   "up/c1",
		PacketID:    7,
	})
	require.NoError(t, err)
	require.NotNil(t, ack)

	fresh := migrateClient(t, "c1")
	fresh.MigrateFrom(old)

	subs := fresh.Subscriptions()
	require.Len(t, subs, 1)
	assert.Equal(t, "a/b", subs[0].Filter)
	assert.Equal(t, 1, fresh.OutboundLen())
	assert.Equal(t, 1, fresh.InboundLen())
	// Only the outbound message holds an allocated Packet Identifier; the
	// inbound QoS 2 window tracks the peer's identifiers separately.
	assert.Equal(t, 1, fresh.PacketIDsInUse())

	// ResendInflight replays the migrated message with DUP=1.
	resend := fresh.ResendInflight()
	require.Len(t, resend, 1)
	assert.True(t, resend[0].FixedHeader.Dup)

	// The migrated inbound QoS 2 handshake completes on the new session.
	deliver, comp := fresh.ReceivePubrel(7)
	require.NotNil(t, deliver)
	assert.Equal(t, packet.Pubcomp, comp.FixedHeader.Type)
}

func TestMigrateFromKeepsNewFlowControlLimits(t *testing.T) {
	old := migrateClient(t, "c1")
	fresh := NewClient(&packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Connect},
		ProtocolVersion: packet.Version5,
		Connect:         packet.ConnectParams{ClientIdentifier: "c1", ProtocolVersion: packet.Version5},
		Properties:      packet.Properties{ReceiveMaximum: 1, ReceiveMaximumFlag: true},
	}, Config{ReceiveMaximum: 3, TopicAliasMaximum: 4})
	fresh.MigrateFrom(old)

	// The new connection's Receive Maximum (1) bounds the migrated window:
	// the first message sends, the second queues.
	now := time.Now()
	first, err := fresh.SendPublish(now, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1},
		TopicName:   "a/b",
	})
	require.NoError(t, err)
	require.NotNil(t, first)
	second, err := fresh.SendPublish(now, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1},
		TopicName:   "a/b",
	})
	require.NoError(t, err)
	assert.Nil(t, second)
	assert.Equal(t, 1, fresh.OutboundQueued())
}

func TestSetSessionExpiry(t *testing.T) {
	c := migrateClient(t, "c1")
	assert.Equal(t, uint32(0), c.Expiry())
	c.SetSessionExpiry(60)
	assert.Equal(t, uint32(60), c.Expiry())
}
