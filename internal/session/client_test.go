package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// connectPacket builds a CONNECT packet for tests.
func connectPacket(version byte, clean bool, clientID string) *packet.Packet {
	return &packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Connect},
		ProtocolVersion: version,
		Connect: packet.ConnectParams{
			ProtocolVersion:  version,
			Clean:            clean,
			ClientIdentifier: clientID,
		},
	}
}

func TestNewClientV311(t *testing.T) {
	cp := connectPacket(packet.Version311, false, "dev-1")
	cp.Connect.Username = "u"
	cp.Connect.Keepalive = 30

	c := NewClient(cp, Config{})
	assert.Equal(t, "dev-1", c.ID)
	assert.Equal(t, "u", c.Username)
	assert.Equal(t, packet.Version311, c.ProtocolVersion)
	assert.False(t, c.CleanStart)
	// MQTT 3.1.1 without Clean Session never expires [MQTT-3.1.2-6].
	assert.Equal(t, uint32(SessionExpiryNever), c.SessionExpiry)
	assert.Equal(t, 45*time.Second, c.KeepaliveTimeout())
	assert.Nil(t, c.Will)
	assert.False(t, c.TakenOver())
}

func TestNewClientV5Properties(t *testing.T) {
	cp := connectPacket(packet.Version5, true, "dev-2")
	cp.Properties.SessionExpiryFlag = true
	cp.Properties.SessionExpiry = 60
	cp.Properties.ReceiveMaximumFlag = true
	cp.Properties.ReceiveMaximum = 10
	cp.Properties.TopicAliasMaximumFlag = true
	cp.Properties.TopicAliasMaximum = 4
	cp.Connect.WillFlag = true
	cp.Connect.WillTopic = "dev/2/status"
	cp.Connect.WillPayload = []byte("off")
	cp.Connect.WillQos = 1
	cp.Connect.WillRetain = true
	cp.Connect.WillProperties.WillDelayFlag = true
	cp.Connect.WillProperties.WillDelay = 5

	c := NewClient(cp, Config{ReceiveMaximum: 20, TopicAliasMaximum: 3})
	assert.True(t, c.CleanStart)
	assert.Equal(t, uint32(60), c.SessionExpiry)
	assert.Equal(t, packet.Version5, c.ProtocolVersion)
	assert.Equal(t, uint32(5), c.Will.Properties.WillDelay)
	assert.Equal(t, "dev/2/status", c.Will.Topic)
	assert.Equal(t, byte(1), c.Will.Qos)
	assert.True(t, c.Will.Retain)

	// Negotiated limits drive the inflight windows and alias tables.
	assert.Equal(t, uint16(10), c.outbound.max)
	assert.Equal(t, uint16(20), c.inbound.max)
	assert.Equal(t, uint16(3), c.inboundAliasMax)
	assert.Equal(t, uint16(4), c.outboundAliasMax)
}

func TestNewClientDefaults(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, false, "dev-3"), Config{})
	// Absent v5 Receive Maximum means 65535 [MQTT-3.1.2-11].
	assert.Equal(t, uint16(DefaultReceiveMaximum), c.outbound.max)
	assert.Equal(t, uint16(DefaultReceiveMaximum), c.inbound.max)
	// Absent v5 Session Expiry means the session ends at disconnect.
	assert.Equal(t, uint32(0), c.SessionExpiry)
	// Zero protocol version is normalized to MQTT 3.1.1.
	c0 := NewClient(connectPacket(0, true, "dev-4"), Config{})
	assert.Equal(t, packet.Version311, c0.ProtocolVersion)
}

func TestKeepaliveTimeoutDisabled(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, true, "dev-5"), Config{})
	assert.Equal(t, time.Duration(0), c.KeepaliveTimeout())
}

func TestTakeOver(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, true, "dev-6"), Config{})
	assert.False(t, c.TakenOver())
	c.TakeOver()
	assert.True(t, c.TakenOver())
}

func TestMarkStoppedAndExpiryDeadline(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, false, "dev-7"), Config{})
	c.SessionExpiry = 30
	stop := time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)
	c.MarkStopped(stop)
	assert.Equal(t, stop, c.StoppedAt())
	deadline, ok := c.ExpiryDeadline()
	require.True(t, ok)
	assert.Equal(t, stop.Add(30*time.Second), deadline)

	c.SessionExpiry = SessionExpiryNever
	_, ok = c.ExpiryDeadline()
	assert.False(t, ok)
}

func TestSubscriptions(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, false, "dev-8"), Config{})
	subs := []topics.Subscription{
		{Filter: "a/b", Qos: 1},
		{Filter: "c/#", Qos: 0, Shared: "g1"},
		{Filter: "a/b", Qos: 2}, // replaces the first entry [MQTT-3.8.4-2]
	}
	for _, s := range subs {
		c.AddSubscription(s)
	}

	got := c.Subscriptions()
	require.Len(t, got, 2)
	// Sorted by session key: plain "a/b" precedes shared "g1\x00c/#".
	assert.Equal(t, "a/b", got[0].Filter)
	assert.Equal(t, byte(2), got[0].Qos)
	assert.Equal(t, "g1", got[1].Shared)

	key := SubscriptionKey(topics.Subscription{Filter: "c/#", Shared: "g1"})
	assert.True(t, c.RemoveSubscription(key))
	assert.False(t, c.RemoveSubscription(key))
	assert.Len(t, c.Subscriptions(), 1)
}
