package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

var fixedNow = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

func TestDowngradeQoS(t *testing.T) {
	cases := []struct {
		publish, subscription, want byte
	}{
		{0, 0, 0},
		{0, 2, 0},
		{1, 0, 0},
		{1, 2, 1},
		{2, 1, 1},
		{2, 2, 2},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, DowngradeQoS(tc.publish, tc.subscription),
			"publish=%d subscription=%d", tc.publish, tc.subscription)
	}
}

func TestInboundQoS0(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "in-q0"), Config{})
	pk := publishPacket(0, 0, "a/b")
	deliver, ack, err := c.ReceivePublish(fixedNow, pk)
	require.NoError(t, err)
	assert.Same(t, pk, deliver)
	assert.Nil(t, ack)
}

func TestInboundQoS1(t *testing.T) {
	for _, version := range []byte{packet.Version311, packet.Version5} {
		c := NewClient(connectPacket(version, true, "in-q1"), Config{})
		pk := publishPacket(7, 1, "a/b")
		deliver, ack, err := c.ReceivePublish(fixedNow, pk)
		require.NoError(t, err)
		assert.Same(t, pk, deliver)
		require.NotNil(t, ack)
		assert.Equal(t, packet.Puback, ack.FixedHeader.Type)
		assert.Equal(t, uint16(7), ack.PacketID)
		assert.Equal(t, packet.CodeSuccess, ack.ReasonCode)
		assert.Equal(t, version, ack.ProtocolVersion)
		// QoS 1 leaves nothing in the inbound window.
		assert.Equal(t, 0, c.InboundLen())
	}
}

// TestInboundQoS2StrictExactlyOnce pins the core guarantee: a QoS 2 PUBLISH
// is never delivered before its PUBREL arrives.
func TestInboundQoS2StrictExactlyOnce(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "in-q2"), Config{})
	pk := publishPacket(9, 2, "a/b")

	// First PUBLISH: stored, PUBREC produced, nothing delivered.
	deliver, ack, err := c.ReceivePublish(fixedNow, pk)
	require.NoError(t, err)
	assert.Nil(t, deliver)
	require.NotNil(t, ack)
	assert.Equal(t, packet.Pubrec, ack.FixedHeader.Type)
	assert.Equal(t, uint16(9), ack.PacketID)
	assert.Equal(t, 1, c.InboundLen())

	// Duplicate PUBLISH (DUP=1): PUBREC re-sent, still nothing delivered.
	dup := publishPacket(9, 2, "a/b")
	dup.FixedHeader.Dup = true
	deliver, ack, err = c.ReceivePublish(fixedNow, dup)
	require.NoError(t, err)
	assert.Nil(t, deliver)
	require.NotNil(t, ack)
	assert.Equal(t, packet.Pubrec, ack.FixedHeader.Type)
	assert.Equal(t, 1, c.InboundLen())

	// PUBREL: exactly now the message becomes deliverable, PUBCOMP produced.
	deliver, pubcomp := c.ReceivePubrel(9)
	require.NotNil(t, deliver)
	assert.Equal(t, "a/b", deliver.TopicName)
	assert.Equal(t, packet.Pubcomp, pubcomp.FixedHeader.Type)
	assert.Equal(t, packet.CodeSuccess, pubcomp.ReasonCode)
	assert.Equal(t, 0, c.InboundLen())

	// Repeated PUBREL (PUBCOMP lost on the wire): PUBCOMP re-sent with 0x92,
	// the message is not delivered a second time.
	deliver, pubcomp = c.ReceivePubrel(9)
	assert.Nil(t, deliver)
	assert.Equal(t, packet.Pubcomp, pubcomp.FixedHeader.Type)
	assert.Equal(t, packet.CodePacketIdentifierNotFound, pubcomp.ReasonCode)
}

func TestInboundQoS2UnknownPubrelV311(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, true, "in-q2-v3"), Config{})
	deliver, pubcomp := c.ReceivePubrel(55)
	assert.Nil(t, deliver)
	assert.Equal(t, packet.Pubcomp, pubcomp.FixedHeader.Type)
	// MQTT 3.1.1 PUBCOMP carries no reason code.
	assert.Equal(t, packet.CodeSuccess, pubcomp.ReasonCode)
	assert.Equal(t, packet.Version311, pubcomp.ProtocolVersion)
}

func TestInboundQoS2ReceiveMaximumExceeded(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "in-q2-max"), Config{ReceiveMaximum: 2})
	for _, id := range []uint16{1, 2} {
		_, _, err := c.ReceivePublish(fixedNow, publishPacket(id, 2, "a"))
		require.NoError(t, err)
	}
	// The third concurrent QoS 2 PUBLISH exceeds the advertised Receive
	// Maximum; the caller disconnects with 0x93 [MQTT-3.3.4-9].
	_, _, err := c.ReceivePublish(fixedNow, publishPacket(3, 2, "a"))
	require.Error(t, err)
	assert.Equal(t, packet.CodeReceiveMaximumExceeded, err.(packet.Code).Code)

	// A duplicate of a stored id is a retransmission, not a new message, and
	// is still answered.
	_, ack, err := c.ReceivePublish(fixedNow, publishPacket(1, 2, "a"))
	require.NoError(t, err)
	assert.Equal(t, packet.Pubrec, ack.FixedHeader.Type)
}

func TestInboundQoS3Rejected(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "in-q3"), Config{})
	pk := publishPacket(1, 0, "a")
	pk.FixedHeader.Qos = 3 // bypassing codec validation
	_, _, err := c.ReceivePublish(fixedNow, pk)
	require.Error(t, err)
	assert.Equal(t, packet.CodeProtocolError, err.(packet.Code).Code)
}

func TestOutboundQoS1(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-q1"), Config{})
	send, err := c.SendPublish(fixedNow, publishPacket(0, 1, "a/b"))
	require.NoError(t, err)
	require.NotNil(t, send)
	assert.Equal(t, uint16(1), send.PacketID)
	assert.False(t, send.FixedHeader.Dup)
	assert.Equal(t, 1, c.OutboundLen())

	// PUBACK completes the message and frees the identifier.
	assert.Nil(t, c.ReceivePuback(send.PacketID))
	assert.Equal(t, 0, c.OutboundLen())
	assert.Equal(t, 0, c.PacketIDsInUse())

	// Unknown PUBACK is ignored.
	assert.Nil(t, c.ReceivePuback(1234))
}

func TestOutboundQoS1QuotaPromotion(t *testing.T) {
	cp := connectPacket(packet.Version5, true, "out-q1-quota")
	cp.Properties.ReceiveMaximumFlag = true
	cp.Properties.ReceiveMaximum = 1
	c := NewClient(cp, Config{})

	first, err := c.SendPublish(fixedNow, publishPacket(0, 1, "a"))
	require.NoError(t, err)
	require.NotNil(t, first)

	// Window full: the second message is queued, not sent.
	second, err := c.SendPublish(fixedNow, publishPacket(0, 1, "b"))
	require.NoError(t, err)
	assert.Nil(t, second)
	assert.Equal(t, 1, c.OutboundQueued())

	// PUBACK releases the slot and returns the queued message to send.
	promoted := c.ReceivePuback(first.PacketID)
	require.NotNil(t, promoted)
	assert.Equal(t, "b", promoted.TopicName)
	assert.Equal(t, 0, c.OutboundQueued())
}

func TestOutboundQoS2FullHandshake(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-q2"), Config{})
	send, err := c.SendPublish(fixedNow, publishPacket(0, 2, "a/b"))
	require.NoError(t, err)
	require.NotNil(t, send)

	// PUBREC advances to the PUBREL stage.
	pubrel, err := c.ReceivePubrec(send.PacketID)
	require.NoError(t, err)
	assert.Equal(t, packet.Pubrel, pubrel.FixedHeader.Type)
	assert.Equal(t, send.PacketID, pubrel.PacketID)
	assert.False(t, pubrel.FixedHeader.Dup)
	assert.Equal(t, 1, c.OutboundLen())

	// Duplicate PUBREC (PUBREL lost): retransmit PUBREL with DUP=1.
	pubrel, err = c.ReceivePubrec(send.PacketID)
	require.NoError(t, err)
	assert.True(t, pubrel.FixedHeader.Dup)

	// PUBCOMP completes the message.
	assert.Nil(t, c.ReceivePubcomp(send.PacketID))
	assert.Equal(t, 0, c.OutboundLen())
	assert.Equal(t, 0, c.PacketIDsInUse())
}

func TestOutboundPubrecUnknownID(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-q2-unk"), Config{})
	pubrel, err := c.ReceivePubrec(77)
	require.NoError(t, err)
	assert.Equal(t, packet.Pubrel, pubrel.FixedHeader.Type)
	assert.Equal(t, packet.CodePacketIdentifierNotFound, pubrel.ReasonCode)
}

func TestOutboundPubrecForQoS1IsProtocolError(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-q1-rec"), Config{})
	send, err := c.SendPublish(fixedNow, publishPacket(0, 1, "a"))
	require.NoError(t, err)
	_, err = c.ReceivePubrec(send.PacketID)
	require.Error(t, err)
	assert.Equal(t, packet.CodeProtocolError, err.(packet.Code).Code)
}

func TestOutboundQoS0BypassesInflight(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-q0"), Config{})
	send, err := c.SendPublish(fixedNow, publishPacket(0, 0, "a"))
	require.NoError(t, err)
	require.NotNil(t, send)
	assert.Equal(t, 0, c.OutboundLen())
	assert.Equal(t, 0, c.PacketIDsInUse())
}

func TestResendInflightDupSnapshot(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "resume"), Config{})

	q1, err := c.SendPublish(fixedNow, publishPacket(0, 1, "qos1"))
	require.NoError(t, err)
	q2pub, err := c.SendPublish(fixedNow, publishPacket(0, 2, "qos2-pub"))
	require.NoError(t, err)
	q2rel, err := c.SendPublish(fixedNow, publishPacket(0, 2, "qos2-rel"))
	require.NoError(t, err)
	_, err = c.ReceivePubrec(q2rel.PacketID) // advance to PUBREL stage
	require.NoError(t, err)

	resend := c.ResendInflight()
	require.Len(t, resend, 3)

	byID := make(map[uint16]packet.Packet, len(resend))
	for _, pk := range resend {
		byID[pk.PacketID] = pk
	}
	// QoS 1: PUBLISH re-sent with DUP=1 [MQTT-4.4.0-1].
	pk := byID[q1.PacketID]
	assert.Equal(t, packet.Publish, pk.FixedHeader.Type)
	assert.True(t, pk.FixedHeader.Dup)
	assert.Equal(t, byte(1), pk.FixedHeader.Qos)
	// QoS 2 awaiting PUBREC: PUBLISH re-sent with DUP=1.
	pk = byID[q2pub.PacketID]
	assert.Equal(t, packet.Publish, pk.FixedHeader.Type)
	assert.True(t, pk.FixedHeader.Dup)
	// QoS 2 awaiting PUBCOMP: PUBREL re-sent with DUP=1.
	pk = byID[q2rel.PacketID]
	assert.Equal(t, packet.Pubrel, pk.FixedHeader.Type)
	assert.True(t, pk.FixedHeader.Dup)
}

func TestResendInflightSkipsQueued(t *testing.T) {
	cp := connectPacket(packet.Version5, true, "resume-q")
	cp.Properties.ReceiveMaximumFlag = true
	cp.Properties.ReceiveMaximum = 1
	c := NewClient(cp, Config{})

	_, err := c.SendPublish(fixedNow, publishPacket(0, 1, "sent"))
	require.NoError(t, err)
	_, err = c.SendPublish(fixedNow, publishPacket(0, 1, "queued"))
	require.NoError(t, err)

	// The queued message was never sent, so it is not retransmitted with DUP;
	// it flows through normal window promotion after reconnect.
	resend := c.ResendInflight()
	require.Len(t, resend, 1)
	assert.Equal(t, "sent", resend[0].TopicName)
}

func TestOutboundExpiryFreesPacketID(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "out-exp"), Config{})
	pk := publishPacket(0, 1, "a")
	pk.Properties.MessageExpiryFlag = true
	pk.Properties.MessageExpiry = 10
	send, err := c.SendPublish(fixedNow, pk)
	require.NoError(t, err)
	require.NotNil(t, send)

	peeked := c.PeekExpiredOutbound(fixedNow.Add(11 * time.Second))
	require.NotNil(t, peeked)
	assert.Equal(t, send.PacketID, peeked.Packet.PacketID)
	assert.Equal(t, 1, c.OutboundLen(), "peek must not remove")

	dropped := c.DropExpiredOutbound(fixedNow.Add(11 * time.Second))
	require.Len(t, dropped, 1)
	assert.Equal(t, 0, c.OutboundLen())
	assert.Equal(t, 0, c.PacketIDsInUse())
}

func TestInboundExpiryDropsPendingQoS2(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "in-exp"), Config{})
	pk := publishPacket(5, 2, "a")
	pk.Properties.MessageExpiryFlag = true
	pk.Properties.MessageExpiry = 5
	_, ack, err := c.ReceivePublish(fixedNow, pk)
	require.NoError(t, err)
	require.NotNil(t, ack)

	dropped := c.DropExpiredInbound(fixedNow.Add(6 * time.Second))
	require.Len(t, dropped, 1)
	assert.Equal(t, uint16(5), dropped[0].PacketID)
	assert.Equal(t, 0, c.InboundLen())
}
