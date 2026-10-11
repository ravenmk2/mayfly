package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestOfflineQueue(t *testing.T) {
	s := newTestServer(t, nil)

	// Persistent session subscribes at QoS 1 and disconnects cleanly.
	sub := dial(t, s).v5()
	sub.connect("sub", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	ack := sub.subscribe(1, packet.Subscription{Filter: "off/#", Qos: 1})
	require.Equal(t, []byte{packet.CodeGrantedQos1}, ack.ReasonCodes)
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	sub.expectClosed(time.Second)
	require.Equal(t, 1, s.ClientCount())

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	// QoS 1 is queued for the offline session; QoS 0 is dropped.
	pub.publish(1, "off/one", []byte("queued"), 1, packet.Properties{})
	puback := pub.read()
	require.Equal(t, packet.Puback, puback.FixedHeader.Type)
	pub.publish(0, "off/zero", []byte("dropped"), 0, packet.Properties{})

	// Reconnect: the queued message is delivered (first transmission, DUP=0)
	// and survives acknowledgement through the normal inflight window.
	sub2 := dial(t, s).v5()
	ack2 := sub2.connect("sub", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.True(t, ack2.SessionPresent)
	msg := sub2.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, "off/one", msg.TopicName)
	assert.Equal(t, []byte("queued"), msg.Payload)
	assert.Equal(t, byte(1), msg.FixedHeader.Qos)
	assert.False(t, msg.FixedHeader.Dup)
	assert.NotZero(t, msg.PacketID)
	sub2.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Puback}, PacketID: msg.PacketID})

	assert.Nil(t, sub2.readMaybe(100*time.Millisecond), "QoS 0 messages are not queued offline")
}

func TestOfflineQueueReceiveMaximum(t *testing.T) {
	// The client's Receive Maximum of 1 bounds the window: on resume one
	// queued message is promoted, the next follows after its PUBACK.
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", false, packet.Properties{
		SessionExpiry: 300, SessionExpiryFlag: true,
		ReceiveMaximum: 1, ReceiveMaximumFlag: true,
	})
	sub.subscribe(1, packet.Subscription{Filter: "w/#", Qos: 1})
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	sub.expectClosed(time.Second)

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "w/1", []byte("a"), 1, packet.Properties{})
	pub.read()
	pub.publish(1, "w/2", []byte("b"), 2, packet.Properties{})
	pub.read()

	sub2 := dial(t, s).v5()
	sub2.connect("sub", false, packet.Properties{
		SessionExpiry: 300, SessionExpiryFlag: true,
		ReceiveMaximum: 1, ReceiveMaximumFlag: true,
	})
	m1 := sub2.read()
	require.Equal(t, packet.Publish, m1.FixedHeader.Type)
	assert.Equal(t, "w/1", m1.TopicName)
	assert.Nil(t, sub2.readMaybe(50*time.Millisecond), "window of 1 holds the second message")
	sub2.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Puback}, PacketID: m1.PacketID})
	m2 := sub2.read()
	require.Equal(t, packet.Publish, m2.FixedHeader.Type)
	assert.Equal(t, "w/2", m2.TopicName)
}
