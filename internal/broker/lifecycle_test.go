package broker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/store"
)

func TestRetainedSemantics(t *testing.T) {
	s := newTestServer(t, nil)

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	// Store a retained message.
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0, Retain: true},
		TopicName:   "retain/a",
		Payload:     []byte("kept"),
	})

	// New subscriber receives it with RETAIN set [MQTT-3.3.1-9].
	sub := dial(t, s).v3()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "retain/+", Qos: 0})
	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.True(t, msg.FixedHeader.Retain)
	assert.Equal(t, []byte("kept"), msg.Payload)

	// An empty retained payload deletes the retained message
	// [MQTT-3.3.1-6] but is still forwarded to current subscribers; as live
	// delivery without RetainAsPublished the RETAIN flag is cleared
	// [MQTT-3.3.1-9].
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0, Retain: true},
		TopicName:   "retain/a",
		Payload:     []byte{},
	})
	clr := sub.read()
	require.Equal(t, packet.Publish, clr.FixedHeader.Type)
	assert.False(t, clr.FixedHeader.Retain)
	assert.Empty(t, clr.Payload)
	assert.Equal(t, 0, s.Topics().RetainedCount())

	// A later subscriber receives nothing.
	sub2 := dial(t, s).v3()
	sub2.connect("sub2", true, packet.Properties{})
	sub2.subscribe(1, packet.Subscription{Filter: "retain/+", Qos: 0})
	assert.Nil(t, sub2.readMaybe(100*time.Millisecond))
}

func TestRetainedRetainHandling(t *testing.T) {
	s := newTestServer(t, nil)
	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0, Retain: true},
		TopicName:   "rh/topic",
		Payload:     []byte("v"),
	})

	// Retain Handling 2: never send retained [MQTT-3.8.3-1].
	never := dial(t, s).v5()
	never.connect("never", true, packet.Properties{})
	never.subscribe(1, packet.Subscription{Filter: "rh/topic", Qos: 0, RetainHandling: 2})
	assert.Nil(t, never.readMaybe(100*time.Millisecond))

	// Retain Handling 1: send only for a new subscription.
	once := dial(t, s).v5()
	once.connect("once", true, packet.Properties{})
	once.subscribe(1, packet.Subscription{Filter: "rh/topic", Qos: 0, RetainHandling: 1})
	msg := once.read()
	assert.Equal(t, []byte("v"), msg.Payload)
	// Re-subscribing the same filter sends nothing.
	once.subscribe(2, packet.Subscription{Filter: "rh/topic", Qos: 0, RetainHandling: 1})
	assert.Nil(t, once.readMaybe(100*time.Millisecond))

	// Retain Handling 0: always send.
	always := dial(t, s).v5()
	always.connect("always", true, packet.Properties{})
	always.subscribe(1, packet.Subscription{Filter: "rh/topic", Qos: 0, RetainHandling: 0})
	msg2 := always.read()
	assert.Equal(t, []byte("v"), msg2.Payload)
}

func TestRetainedRetainAsPublished(t *testing.T) {
	s := newTestServer(t, nil)
	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0, Retain: true},
		TopicName:   "rap/topic",
		Payload:     []byte("v"),
	})

	// RetainAsPublished clears the forced retain flag and passes the
	// stored one through.
	rap := dial(t, s).v5()
	rap.connect("rap", true, packet.Properties{})
	rap.subscribe(1, packet.Subscription{Filter: "rap/topic", Qos: 0, RetainAsPublished: true})
	msg := rap.read()
	assert.True(t, msg.FixedHeader.Retain)
}

func TestSharedSubscriptionRoundRobin(t *testing.T) {
	s := newTestServer(t, nil)
	m1 := dial(t, s).v3()
	m1.connect("m1", true, packet.Properties{})
	m1.subscribe(1, packet.Subscription{Filter: "$share/workers/jobs", Qos: 0})
	m2 := dial(t, s).v3()
	m2.connect("m2", true, packet.Properties{})
	m2.subscribe(1, packet.Subscription{Filter: "$share/workers/jobs", Qos: 0})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	for i := 0; i < 4; i++ {
		pub.publish(0, "jobs", []byte{byte('0' + i)}, 0, packet.Properties{})
	}

	got1 := map[byte]bool{}
	got2 := map[byte]bool{}
	for i := 0; i < 2; i++ {
		got1[m1.read().Payload[0]] = true
		got2[m2.read().Payload[0]] = true
	}
	// Strict round-robin: each member received every other message.
	assert.True(t, got1['0'] && got1['2'] || got1['1'] && got1['3'])
	assert.True(t, got2['0'] && got2['2'] || got2['1'] && got2['3'])
	assert.Empty(t, m1.readMaybe(50*time.Millisecond))
	assert.Empty(t, m2.readMaybe(50*time.Millisecond))
}

func TestWillOnAbnormalDisconnect(t *testing.T) {
	s := newTestServer(t, nil)
	watcher := dial(t, s).v3()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "status/#", Qos: 0})

	dev := dial(t, s).v3()
	dev.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion:  packet.Version311,
			Clean:            true,
			ClientIdentifier: "dev",
			WillFlag:         true,
			WillTopic:        "status/dev",
			WillPayload:      []byte("offline"),
		},
	})
	require.Equal(t, packet.Connack, dev.read().FixedHeader.Type)

	dev.close() // abnormal: no DISCONNECT [MQTT-3.1.2-8]
	will := watcher.read()
	require.Equal(t, packet.Publish, will.FixedHeader.Type)
	assert.Equal(t, "status/dev", will.TopicName)
	assert.Equal(t, []byte("offline"), will.Payload)
}

func TestWillSuppressedOnNormalDisconnect(t *testing.T) {
	s := newTestServer(t, nil)
	watcher := dial(t, s).v3()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "status/#", Qos: 0})

	dev := dial(t, s).v5()
	dev.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion:  packet.Version5,
			Clean:            true,
			ClientIdentifier: "dev",
			WillFlag:         true,
			WillTopic:        "status/dev",
			WillPayload:      []byte("offline"),
		},
	})
	require.Equal(t, packet.Connack, dev.read().FixedHeader.Type)

	dev.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	dev.expectClosed(time.Second)
	assert.Nil(t, watcher.readMaybe(150*time.Millisecond), "normal DISCONNECT suppresses the Will")
}

func TestWillDelayAndCancelOnResume(t *testing.T) {
	s := newTestServer(t, nil)
	watcher := dial(t, s).v3()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "status/#", Qos: 0})

	connectWithDelayedWill := func(c *testClient) {
		c.write(&packet.Packet{
			FixedHeader: packet.FixedHeader{Type: packet.Connect},
			Connect: packet.ConnectParams{
				ProtocolVersion:  packet.Version5,
				Clean:            false,
				ClientIdentifier: "dev",
				WillFlag:         true,
				WillTopic:        "status/dev",
				WillPayload:      []byte("offline"),
				WillProperties:   packet.Properties{WillDelay: 60, WillDelayFlag: true},
			},
			Properties: packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true},
		})
		require.Equal(t, packet.Connack, c.read().FixedHeader.Type)
	}

	dev := dial(t, s).v5()
	connectWithDelayedWill(dev)
	dev.close()
	assert.Nil(t, watcher.readMaybe(100*time.Millisecond), "will must wait for its delay")

	// Reconnecting before the delay elapses cancels the Will
	// [MQTT-3.1.3-9].
	dev2 := dial(t, s).v5()
	connectWithDelayedWill(dev2)
	s.sweep(s.now().Add(61 * time.Second))
	assert.Nil(t, watcher.readMaybe(100*time.Millisecond), "resumed session cancels the delayed Will")
}

func TestWillDelayFires(t *testing.T) {
	s := newTestServer(t, nil)
	watcher := dial(t, s).v3()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "status/#", Qos: 1})

	dev := dial(t, s).v5()
	dev.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion:  packet.Version5,
			Clean:            false,
			ClientIdentifier: "dev",
			WillFlag:         true,
			WillQos:          1,
			WillTopic:        "status/dev",
			WillPayload:      []byte("gone"),
			WillProperties:   packet.Properties{WillDelay: 30, WillDelayFlag: true},
		},
		Properties: packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true},
	})
	require.Equal(t, packet.Connack, dev.read().FixedHeader.Type)
	dev.close()
	assert.Nil(t, watcher.readMaybe(100*time.Millisecond))

	s.sweep(s.now().Add(31 * time.Second))
	will := watcher.read()
	require.Equal(t, packet.Publish, will.FixedHeader.Type)
	assert.Equal(t, byte(1), will.FixedHeader.Qos)
	assert.Equal(t, []byte("gone"), will.Payload)
}

func TestKeepaliveTimeout(t *testing.T) {
	s := newTestServer(t, nil)
	watcher := dial(t, s).v3()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "dead/#", Qos: 0})

	c := dial(t, s).v5()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion:  packet.Version5,
			Clean:            true,
			ClientIdentifier: "slow",
			Keepalive:        1, // 1.5s timeout [MQTT-3.1.2-24]
			WillFlag:         true,
			WillTopic:        "dead/slow",
			WillPayload:      []byte("timeout"),
		},
	})
	require.Equal(t, packet.Connack, c.read().FixedHeader.Type)

	// No packets follow: the broker disconnects with 0x8D and the Will
	// fires.
	disc := c.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeKeepAliveTimeout, disc.ReasonCode)
	c.expectClosed(2 * time.Second)

	will := watcher.read()
	assert.Equal(t, "dead/slow", will.TopicName)
}

func TestSessionTakeover(t *testing.T) {
	s := newTestServer(t, nil)
	expiry := packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true}

	first := dial(t, s).v5()
	ack := first.connect("dev", false, expiry)
	assert.False(t, ack.SessionPresent)
	first.subscribe(1, packet.Subscription{Filter: "takeover/t", Qos: 1})

	// A second connection with the same ID takes over: the old connection
	// receives DISCONNECT 0x8E [MQTT-3.1.4-3] and the session migrates.
	second := dial(t, s).v5()
	ack2 := second.connect("dev", false, expiry)
	assert.True(t, ack2.SessionPresent)

	disc := first.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeSessionTakenOver, disc.ReasonCode)
	first.expectClosed(time.Second)
	assert.Equal(t, 1, s.ClientCount())

	// The subscription survived on the new connection.
	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(0, "takeover/t", []byte("x"), 0, packet.Properties{})
	msg := second.read()
	assert.Equal(t, []byte("x"), msg.Payload)
}

func TestResendInflightOnReconnect(t *testing.T) {
	s := newTestServer(t, nil)

	sub := dial(t, s).v3()
	sub.connect("sub", false, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "inflight/t", Qos: 1})

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "inflight/t", []byte("unacked"), 3, packet.Properties{})
	pub.read() // PUBACK

	// The subscriber receives the QoS 1 message but never acknowledges it.
	msg := sub.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	savedID := msg.PacketID
	sub.close()

	// Reconnecting with the persisted session replays the message with
	// DUP=1 and the same Packet Identifier [MQTT-4.4.0-1].
	sub2 := dial(t, s).v3()
	ack := sub2.connect("sub", false, packet.Properties{})
	assert.True(t, ack.SessionPresent)
	resent := sub2.read()
	require.Equal(t, packet.Publish, resent.FixedHeader.Type)
	assert.True(t, resent.FixedHeader.Dup)
	assert.Equal(t, savedID, resent.PacketID)
	assert.Equal(t, []byte("unacked"), resent.Payload)
}

func TestOutboundBackpressureDropsQoS0(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.OutboundQueueDepth = 4 })
	sub := dial(t, s).v3()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "flood", Qos: 0})
	// The subscriber never reads again: its pipe blocks the write loop.

	pub := dial(t, s).v3()
	pub.connect("pub", true, packet.Properties{})
	for i := 0; i < 20; i++ {
		pub.publish(0, "flood", []byte("x"), 0, packet.Properties{})
	}

	// Four queue slots plus at most one in-flight write; the rest of the
	// QoS 0 messages are dropped and counted (architecture section 3).
	require.Eventually(t, func() bool {
		return s.Stats.MessagesDropped.Load() >= 15
	}, 2*time.Second, 10*time.Millisecond)
}

func TestGracefulShutdown(t *testing.T) {
	l, err := listener.NewTCP(listener.TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	s := newTestServer(t, nil)
	s.AddListener(l)

	ctx := context.Background()
	runDone := make(chan error, 1)
	go func() { runDone <- s.Run(ctx) }()

	// Connect over real TCP.
	c := dialTCP(t, l.Addr()).v5()
	c.connect("grace", true, packet.Properties{})
	require.Eventually(t, func() bool { return s.ClientCount() == 1 },
		2*time.Second, 10*time.Millisecond)

	require.NoError(t, s.Close())
	require.NoError(t, <-runDone)

	// The shutdown drained the queue first: the client saw DISCONNECT 0x8B
	// (Server shutting down) before the connection closed.
	disc := c.read()
	require.Equal(t, packet.Disconnect, disc.FixedHeader.Type)
	assert.Equal(t, packet.CodeServerShuttingDown, disc.ReasonCode)
	c.expectClosed(2 * time.Second)
}

func TestServerRequiresListener(t *testing.T) {
	s := newTestServer(t, nil)
	require.ErrorIs(t, s.Run(context.Background()), ErrNoListeners)
}

// TestRuntimeListenerActivation covers the hot activation path (setup
// wizard): a broker started without listeners serves listeners registered
// later via AddListener, Start is idempotent, and shutdown still drains the
// runtime-added listener.
func TestRuntimeListenerActivation(t *testing.T) {
	s := newTestServer(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	s.Start(ctx) // idempotent

	l, err := listener.NewTCP(listener.TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	s.AddListener(l)

	c := dialTCP(t, l.Addr()).v3()
	c.connect("late", true, packet.Properties{})
	require.Eventually(t, func() bool { return s.ClientCount() == 1 },
		2*time.Second, 10*time.Millisecond)

	require.NoError(t, s.Close())
	c.expectClosed(2 * time.Second)
}

// TestSetStoreRecoversRetained covers the store hot attach: SetStore
// recovers the persisted state like a store passed to New, and a second
// SetStore is a no-op.
func TestSetStoreRecoversRetained(t *testing.T) {
	seed := store.NewMemory()
	require.NoError(t, seed.Retained().Put(context.Background(), store.RetainedMessage{
		Topic: "seed/topic",
		Packet: packet.Packet{
			FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
			TopicName:   "seed/topic", Payload: []byte("persisted"),
		},
	}))

	s := newTestServer(t, nil)
	require.True(t, s.SetStore(seed))
	assert.Equal(t, 1, s.Topics().RetainedCount(), "SetStore recovers persisted state")

	require.False(t, s.SetStore(store.NewMemory()), "a store is attached at most once")

	// Writes mirror into the attached store from now on.
	require.NoError(t, s.Publish("live/topic", []byte("x"), true))
	topics := map[string]bool{}
	require.NoError(t, seed.Retained().Iterate(context.Background(), func(r store.RetainedMessage) bool {
		topics[r.Topic] = true
		return true
	}))
	assert.Contains(t, topics, "live/topic")
}
