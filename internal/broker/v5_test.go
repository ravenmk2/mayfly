package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestConnackAdvertisesMaximumPacketSize(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.MaxPacketSize = 4096 })
	c := dial(t, s).v5()
	ack := c.connect("c", true, packet.Properties{})
	assert.True(t, ack.Properties.MaximumPacketSizeFlag)
	assert.Equal(t, uint32(4096), ack.Properties.MaximumPacketSize)
}

func TestPeerMaxPacketSizeDropsOversized(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	// The client accepts at most 64 bytes per packet.
	sub.connect("sub", true, packet.Properties{MaximumPacketSize: 64, MaximumPacketSizeFlag: true})
	sub.subscribe(1, packet.Subscription{Filter: "cap", Qos: 1})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})

	// A payload that fits is delivered.
	pub.publish(0, "cap", []byte("small"), 0, packet.Properties{})
	msg := sub.read()
	assert.Equal(t, []byte("small"), msg.Payload)

	// An oversized QoS 0 message is dropped, not queued.
	pub.publish(0, "cap", make([]byte, 512), 0, packet.Properties{})
	assert.Nil(t, sub.readMaybe(100*time.Millisecond))
	assert.EqualValues(t, 1, s.Stats.MessagesOversized.Load())

	// An oversized QoS 1 message is dropped without entering the inflight
	// window, so it cannot be retransmitted forever.
	pub.publish(1, "cap", make([]byte, 512), 9, packet.Properties{})
	ack := pub.read()
	require.Equal(t, packet.Puback, ack.FixedHeader.Type) // publisher side unaffected
	assert.Nil(t, sub.readMaybe(100*time.Millisecond))
	assert.EqualValues(t, 2, s.Stats.MessagesOversized.Load())

	client := s.lookupConn("sub").sessionClient()
	assert.Equal(t, 0, client.OutboundLen(), "oversized message must not enter inflight")
	assert.Equal(t, 0, client.OutboundQueued())
}

func TestConnectWithAuthMethodRejected(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	// Enhanced authentication is not supported: an Authentication Method on
	// CONNECT is rejected with 0x8C [MQTT-3.1.2-22].
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: "c"},
		Properties:  packet.Properties{AuthMethod: "SCRAM", AuthMethodFlag: true},
	})
	ack := c.read()
	require.Equal(t, packet.Connack, ack.FixedHeader.Type)
	assert.Equal(t, packet.CodeBadAuthenticationMethod, ack.ReasonCode)
	c.expectClosed(time.Second)
}

func TestAuthPacketRejected(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("c", true, packet.Properties{})

	// AUTH outside an enhanced authentication exchange is a Protocol Error
	// [MQTT-3.15.2-2]; 0x8C is not a valid DISCONNECT reason code.
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Auth},
		ReasonCode:  packet.CodeSuccess,
		Properties:  packet.Properties{AuthMethod: "x", AuthMethodFlag: true},
	})
	disc := c.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeProtocolError, disc.ReasonCode)
	c.expectClosed(time.Second)
}

func TestDisconnectSessionExpiryOverride(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("dev", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	c.subscribe(1, packet.Subscription{Filter: "a/b", Qos: 0})

	// The client shortens its session to one second on DISCONNECT
	// [MQTT-3.1.2-11].
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Disconnect},
		Properties:  packet.Properties{SessionExpiry: 1, SessionExpiryFlag: true},
	})
	c.expectClosed(time.Second)
	require.Equal(t, 1, s.ClientCount())

	s.sweep(s.now().Add(2 * time.Second))
	assert.Equal(t, 0, s.ClientCount(), "the DISCONNECT override must shorten the session")
	assert.EqualValues(t, 0, s.Topics().SubscriptionCount())
}

func TestDisconnectSessionExpiryExtendRejected(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	// No Session Expiry on CONNECT: the session ends at disconnect.
	c.connect("dev", true, packet.Properties{})

	// Raising the interval from zero is a Protocol Error [MQTT-3.1.2-11];
	// the server notifies with 0x82 before closing.
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Disconnect},
		Properties:  packet.Properties{SessionExpiry: 10, SessionExpiryFlag: true},
	})
	disc := c.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeProtocolError, disc.ReasonCode)
	c.expectClosed(time.Second)
	assert.Equal(t, 0, s.ClientCount())
}

func TestRequestProblemInfoOmitsReasonString(t *testing.T) {
	s := newTestServer(t, nil)

	// Client opting out: Request Problem Information = 0 [MQTT-3.1.2-29].
	quiet := dial(t, s).v5()
	quiet.connect("quiet", true, packet.Properties{RequestProblemInfo: 0, RequestProblemInfoFlag: true})
	// Trigger a protocol error: unknown topic alias.
	quiet.publish(0, "", []byte("x"), 0, packet.Properties{TopicAlias: 3, TopicAliasFlag: true})
	disc := quiet.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeProtocolError, disc.ReasonCode)
	assert.False(t, disc.Properties.ReasonStringFlag, "Reason String must be omitted")

	// Default clients still receive the Reason String.
	loud := dial(t, s).v5()
	loud.connect("loud", true, packet.Properties{})
	loud.publish(0, "", []byte("x"), 0, packet.Properties{TopicAlias: 3, TopicAliasFlag: true})
	disc2 := loud.read()
	require.Equal(t, packet.Disconnect, disc2.FixedHeader.Type)
	assert.True(t, disc2.Properties.ReasonStringFlag)
	assert.NotEmpty(t, disc2.Properties.ReasonString)
}
