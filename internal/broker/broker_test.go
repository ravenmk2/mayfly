package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// denyAuth rejects every CONNECT.
type denyAuth struct{}

func (denyAuth) Authenticate(context.Context, hook.ConnectInfo) (hook.Result, error) {
	return hook.Result{OK: false, Reason: "denied"}, nil
}

// failAuth simulates a broken authentication backend.
type failAuth struct{}

func (failAuth) Authenticate(context.Context, hook.ConnectInfo) (hook.Result, error) {
	return hook.Result{}, errors.New("backend down")
}

func TestConnectV3(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v3()
	ack := c.connect("c1", true, packet.Properties{})
	assert.False(t, ack.SessionPresent)
	assert.Equal(t, packet.V3ConnackAccepted, ack.ReasonCode)
	c.close()
}

func TestConnectV5AdvertisedLimits(t *testing.T) {
	s := newTestServer(t, func(o *Options) {
		o.ReceiveMaximum = 10
		o.TopicAliasMaximum = 4
	})
	c := dial(t, s).v5()
	ack := c.connect("c1", true, packet.Properties{})
	assert.True(t, ack.Properties.ReceiveMaximumFlag)
	assert.Equal(t, uint16(10), ack.Properties.ReceiveMaximum)
	assert.True(t, ack.Properties.TopicAliasMaximumFlag)
	assert.Equal(t, uint16(4), ack.Properties.TopicAliasMaximum)
	c.close()
}

func TestConnectRejected(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.Authenticator = hook.NewAuthChain(denyAuth{}) })

	// v5 without credentials: 0x87 Not authorized.
	c5 := dial(t, s).v5()
	c5.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: "c5"},
	})
	ack := c5.read()
	assert.Equal(t, packet.Connack, ack.FixedHeader.Type)
	assert.Equal(t, packet.CodeNotAuthorized, ack.ReasonCode)
	c5.expectClosed(time.Second)

	// v3 with credentials: mapped to return code 4 (bad username/password).
	c3 := dial(t, s).v3()
	c3.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion: packet.Version311, Clean: true, ClientIdentifier: "c3",
			UsernameFlag: true, Username: "u", PasswordFlag: true, Password: []byte("p"),
		},
	})
	ack3 := c3.read()
	assert.Equal(t, packet.V3ConnackBadUsernameOrPassword, ack3.ReasonCode)
	c3.expectClosed(time.Second)
	assert.EqualValues(t, 2, s.Stats.AuthFailures.Load())
}

func TestConnectAuthBackendFailure(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.Authenticator = hook.NewAuthChain(failAuth{}) })
	c := dial(t, s).v5()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: "c"},
	})
	ack := c.read()
	assert.Equal(t, packet.CodeNotAuthorized, ack.ReasonCode) // fail-closed
	c.expectClosed(time.Second)
}

func TestConnectEmptyClientID(t *testing.T) {
	s := newTestServer(t, nil)

	// v5 clean start: the broker assigns an identifier [MQTT-3.1.3-6].
	c5 := dial(t, s).v5()
	ack := c5.connect("", true, packet.Properties{})
	assert.True(t, ack.Properties.AssignedClientIDFlag)
	assert.NotEmpty(t, ack.Properties.AssignedClientID)
	c5.close()

	// v3 without clean session: rejected [MQTT-3.1.3-8].
	c3 := dial(t, s).v3()
	c3.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version311, Clean: false},
	})
	ack3 := c3.read()
	assert.Equal(t, packet.V3ConnackIdentifierRejected, ack3.ReasonCode)
	c3.expectClosed(time.Second)
}

func TestFirstPacketMustBeConnect(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pingreq}})
	c.expectClosed(time.Second)
	assert.EqualValues(t, 0, s.ClientCount())
}

func TestSessionPresentAndResume(t *testing.T) {
	s := newTestServer(t, nil)

	// First connection: persistent session with a subscription.
	c1 := dial(t, s).v5()
	ack := c1.connect("dev", true, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.False(t, ack.SessionPresent)
	sub := c1.subscribe(1, packet.Subscription{Filter: "sensors/+", Qos: 1})
	require.Equal(t, []byte{packet.CodeGrantedQos1}, sub.ReasonCodes)

	// Normal DISCONNECT with expiry kept.
	c1.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	c1.expectClosed(time.Second)
	require.Equal(t, 1, s.ClientCount()) // session persists

	// Reconnect without clean start: session present, subscription live.
	c2 := dial(t, s).v5()
	ack2 := c2.connect("dev", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.True(t, ack2.SessionPresent)

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "sensors/temp", []byte("21"), 5, packet.Properties{})
	puback := pub.read()
	require.Equal(t, packet.Puback, puback.FixedHeader.Type)

	msg := c2.read()
	assert.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, "sensors/temp", msg.TopicName)
	assert.Equal(t, []byte("21"), msg.Payload)
	assert.Equal(t, byte(1), msg.FixedHeader.Qos) // subscription QoS upgrade cap
	assert.NotZero(t, msg.PacketID)
	c2.readMaybe(50 * time.Millisecond) // no duplicates
}

func TestSessionExpirySweep(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("dev", false, packet.Properties{SessionExpiry: 60, SessionExpiryFlag: true})
	c.subscribe(1, packet.Subscription{Filter: "a/b", Qos: 0})
	c.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	c.expectClosed(time.Second)
	require.Equal(t, 1, s.ClientCount())
	require.EqualValues(t, 1, s.Topics().SubscriptionCount())

	// Move the clock past the expiry and sweep.
	s.sweep(s.now().Add(61 * time.Second))
	assert.Equal(t, 0, s.ClientCount())
	assert.EqualValues(t, 0, s.Topics().SubscriptionCount())
}

func TestSessionExpiryNotDue(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("dev", false, packet.Properties{SessionExpiry: 60, SessionExpiryFlag: true})
	c.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	c.expectClosed(time.Second)
	s.sweep(s.now().Add(30 * time.Second))
	assert.Equal(t, 1, s.ClientCount())
}

func TestPublishQoS0Flow(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v3()
	sub.connect("sub", true, packet.Properties{})
	ack := sub.subscribe(1, packet.Subscription{Filter: "a/#", Qos: 0})
	require.Equal(t, []byte{byte(0)}, ack.ReasonCodes)

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(0, "a/b", []byte("hello"), 0, packet.Properties{})

	msg := sub.read()
	assert.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, byte(0), msg.FixedHeader.Qos)
	assert.False(t, msg.FixedHeader.Retain)
	assert.Equal(t, "a/b", msg.TopicName)
	assert.Equal(t, []byte("hello"), msg.Payload)
}

func TestPublishQoS1Flow(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "a/b", Qos: 1})

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "a/b", []byte("m"), 42, packet.Properties{})

	ack := pub.read()
	assert.Equal(t, packet.Puback, ack.FixedHeader.Type)
	assert.Equal(t, uint16(42), ack.PacketID)
	assert.Equal(t, packet.CodeSuccess, ack.ReasonCode)

	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, byte(1), msg.FixedHeader.Qos)
	// Acknowledge the outbound QoS 1 message.
	sub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Puback},
		PacketID:    msg.PacketID,
	})
}

func TestPubackNoMatchingSubscribers(t *testing.T) {
	s := newTestServer(t, nil)
	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "nobody/listens", []byte("m"), 7, packet.Properties{})
	ack := pub.read()
	require.Equal(t, packet.Puback, ack.FixedHeader.Type)
	assert.Equal(t, packet.CodeNoMatchingSubscribers, ack.ReasonCode) // 0x10
}

func TestInboundQoS2StrictExactlyOnce(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v3()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "exact/#", Qos: 2})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})

	// PUBLISH -> PUBREC; nothing delivered yet [MQTT-4.3.3-1].
	pub.publish(2, "exact/once", []byte("payload"), 100, packet.Properties{})
	rec := pub.read()
	require.Equal(t, packet.Pubrec, rec.FixedHeader.Type)
	assert.Equal(t, uint16(100), rec.PacketID)
	assert.Nil(t, sub.readMaybe(100*time.Millisecond), "no delivery before PUBREL")

	// Duplicate PUBLISH: another PUBREC, still no delivery.
	pub.publish(2, "exact/once", []byte("payload"), 100, packet.Properties{})
	rec2 := pub.read()
	assert.Equal(t, packet.Pubrec, rec2.FixedHeader.Type)
	assert.Nil(t, sub.readMaybe(100*time.Millisecond))

	// PUBREL -> delivered once, PUBCOMP returned.
	pub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrel}, PacketID: 100})
	comp := pub.read()
	assert.Equal(t, packet.Pubcomp, comp.FixedHeader.Type)
	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, byte(2), msg.FixedHeader.Qos)
	assert.Equal(t, []byte("payload"), msg.Payload)

	// Duplicate PUBREL: PUBCOMP again, no second delivery.
	pub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrel}, PacketID: 100})
	comp2 := pub.read()
	assert.Equal(t, packet.Pubcomp, comp2.FixedHeader.Type)
	assert.Nil(t, sub.readMaybe(100*time.Millisecond), "exactly-once violated")
}

func TestOutboundQoS2Flow(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "q2", Qos: 2})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(2, "q2", []byte("v"), 9, packet.Properties{})
	pub.read() // PUBREC
	pub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrel}, PacketID: 9})
	pub.read() // PUBCOMP

	// Subscriber side handshake: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.
	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, byte(2), msg.FixedHeader.Qos)
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrec}, PacketID: msg.PacketID})
	rel := sub.read()
	require.Equal(t, packet.Pubrel, rel.FixedHeader.Type)
	assert.Equal(t, msg.PacketID, rel.PacketID)
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubcomp}, PacketID: msg.PacketID})
}

func TestReceiveMaximumExceeded(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.ReceiveMaximum = 1 })
	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})

	pub.publish(2, "t", []byte("1"), 1, packet.Properties{})
	rec := pub.read()
	require.Equal(t, packet.Pubrec, rec.FixedHeader.Type)

	// A second unacknowledged QoS 2 PUBLISH exceeds the advertised window:
	// the broker disconnects with 0x93 [MQTT-3.3.4-9].
	pub.publish(2, "t", []byte("2"), 2, packet.Properties{})
	disc := pub.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeReceiveMaximumExceeded, disc.ReasonCode)
	pub.expectClosed(time.Second)
}

func TestTopicAliasInbound(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v3()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "aliased/topic", Qos: 0})

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})

	// Register alias 1, then publish with the alias only [MQTT-3.3.2-9].
	pub.publish(0, "aliased/topic", []byte("one"), 0, packet.Properties{TopicAlias: 1, TopicAliasFlag: true})
	msg := sub.read()
	assert.Equal(t, "aliased/topic", msg.TopicName)
	assert.Equal(t, []byte("one"), msg.Payload)

	pub.publish(0, "", []byte("two"), 0, packet.Properties{TopicAlias: 1, TopicAliasFlag: true})
	msg2 := sub.read()
	assert.Equal(t, "aliased/topic", msg2.TopicName)
	assert.Equal(t, []byte("two"), msg2.Payload)

	// An unknown alias is a Protocol Error [MQTT-3.3.2-11].
	pub.publish(0, "", []byte("three"), 0, packet.Properties{TopicAlias: 9, TopicAliasFlag: true})
	disc := pub.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeProtocolError, disc.ReasonCode)
}

func TestTopicAliasOutbound(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{TopicAliasMaximum: 5, TopicAliasMaximumFlag: true})
	sub.subscribe(1, packet.Subscription{Filter: "out/alias", Qos: 0})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(0, "out/alias", []byte("x"), 0, packet.Properties{})

	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.True(t, msg.Properties.TopicAliasFlag)
	assert.NotZero(t, msg.Properties.TopicAlias)
}

func TestSubscriptionIdentifierForwarded(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "sid/#", Qos: 0, Identifier: 77})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(0, "sid/x", []byte("y"), 0, packet.Properties{})

	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, []uint32{77}, msg.Properties.SubscriptionID)
}

func TestNoLocal(t *testing.T) {
	s := newTestServer(t, nil)
	self := dial(t, s).v5()
	self.connect("self", true, packet.Properties{})
	self.subscribe(1, packet.Subscription{Filter: "nl", Qos: 0, NoLocal: true})

	other := dial(t, s).v5()
	other.connect("other", true, packet.Properties{})
	other.subscribe(1, packet.Subscription{Filter: "nl", Qos: 0})

	self.publish(0, "nl", []byte("x"), 0, packet.Properties{})
	assert.Nil(t, self.readMaybe(100*time.Millisecond), "NoLocal must suppress self-delivery")
	msg := other.read()
	assert.Equal(t, []byte("x"), msg.Payload)
}

func TestUnsubscribe(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "u/a", Qos: 0})
	sub.subscribe(2, packet.Subscription{Filter: "u/b", Qos: 0})

	sub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Unsubscribe},
		PacketID:    3,
		Filters:     []packet.Subscription{{Filter: "u/a"}, {Filter: "u/never"}},
	})
	ack := sub.read()
	require.Equal(t, packet.Unsuback, ack.FixedHeader.Type)
	assert.Equal(t, []byte{packet.CodeSuccess, packet.CodeNoSubscriptionExisted}, ack.ReasonCodes)
	assert.EqualValues(t, 1, s.Topics().SubscriptionCount())

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(0, "u/a", []byte("gone"), 0, packet.Properties{})
	pub.publish(0, "u/b", []byte("here"), 0, packet.Properties{})
	msg := sub.read()
	assert.Equal(t, "u/b", msg.TopicName)
}

func TestPingreq(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v3()
	c.connect("ping", true, packet.Properties{})
	c.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pingreq}})
	resp := c.read()
	assert.Equal(t, packet.Pingresp, resp.FixedHeader.Type)
}

func TestMaxPacketSize(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.MaxPacketSize = 64 })
	c := dial(t, s).v5()
	c.connect("big", true, packet.Properties{})

	// Announce a PUBLISH far beyond the limit; the broker disconnects with
	// 0x95 without reading the body. The write runs in a goroutine because
	// the in-memory pipe is synchronous and the broker never reads the
	// body.
	go c.writeBestEffort(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0},
		TopicName:   "big/topic",
		Payload:     make([]byte, 4096),
	})
	disc := c.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodePacketTooLarge, disc.ReasonCode)
	c.expectClosed(time.Second)
}
