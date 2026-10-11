package broker

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/metrics"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// newSysServer builds a broker with a versioned metrics instance so the
// $SYS/broker/version payload is non-empty (an empty retained payload is a
// deletion [MQTT-3.3.1-6]).
func newSysServer(t *testing.T) *Server {
	t.Helper()
	return newTestServer(t, func(o *Options) { o.Metrics = metrics.New("test-version") })
}

// startSys runs the $SYS publisher against the broker on a fast cadence and
// waits for the first retained round to land.
func startSys(t *testing.T, s *Server, interval time.Duration) {
	t.Helper()
	pub := metrics.NewSysPublisher(s.Stats, metrics.SysOptions{
		Interval: interval,
		Publish:  func(topic string, payload []byte) error { return s.Publish(topic, payload, true) },
		Refresh:  s.RefreshMetrics,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go pub.Run(ctx)
	// Wait for the full first round, not just its first topic.
	require.Eventually(t, func() bool { return s.Topics().RetainedCount() >= len(pub.Topics()) },
		2*time.Second, 5*time.Millisecond, "first $SYS round never landed")
}

func TestSysRetainedDelivery(t *testing.T) {
	s := newSysServer(t)
	startSys(t, s, time.Minute) // no periodic rounds during this test

	// A subscriber arriving after the round reads the retained values.
	c := dial(t, s).v5()
	c.connect("watcher", true, packet.Properties{})
	ack := c.subscribe(1, packet.Subscription{Filter: "$SYS/broker/#", Qos: 0})
	assert.Equal(t, []byte{0}, ack.ReasonCodes)

	got := map[string]*packet.Packet{}
	want := []string{
		"$SYS/broker/uptime", "$SYS/broker/version",
		"$SYS/broker/clients/connected", "$SYS/broker/clients/total",
		"$SYS/broker/messages/received", "$SYS/broker/messages/sent",
		"$SYS/broker/messages/dropped", "$SYS/broker/subscriptions/count",
		"$SYS/broker/retained/count", "$SYS/broker/bytes/received",
		"$SYS/broker/bytes/sent",
	}
	missing := func() bool {
		for _, topic := range want {
			if got[topic] == nil {
				return true
			}
		}
		return false
	}
	for missing() {
		pk := c.read()
		require.Equal(t, packet.Publish, pk.FixedHeader.Type)
		assert.True(t, pk.FixedHeader.Retain, "%s must be retained", pk.TopicName)
		got[pk.TopicName] = pk
	}
	// Values are decimal integers. The snapshot predates this client (and
	// the round's own retained publishes), so only parseability is asserted.
	for _, topic := range want {
		if topic == "$SYS/broker/version" {
			assert.Equal(t, "test-version", string(got[topic].Payload))
			continue
		}
		_, err := strconv.Atoi(string(got[topic].Payload))
		assert.NoError(t, err, "%s payload must be a decimal integer", topic)
	}
	c.close()
}

func TestSysPeriodicUpdates(t *testing.T) {
	s := newSysServer(t)
	startSys(t, s, 20*time.Millisecond)

	c := dial(t, s).v5()
	c.connect("watcher", true, packet.Properties{})
	c.subscribe(1, packet.Subscription{Filter: "$SYS/broker/uptime", Qos: 0})

	// The retained replay plus live republish rounds keep arriving.
	seen := 0
	deadline := time.Now().Add(2 * time.Second)
	for seen < 3 && time.Now().Before(deadline) {
		pk := c.readMaybe(time.Second)
		if pk != nil && pk.TopicName == "$SYS/broker/uptime" {
			seen++
		}
	}
	assert.GreaterOrEqual(t, seen, 3, "expected retained replay plus periodic republishes")
	c.close()
}

// TestSysNotMatchedByTopLevelWildcard confirms [MQTT-4.7.2-1]: a '#' or '+'
// subscription at the root never receives $SYS traffic.
func TestSysNotMatchedByTopLevelWildcard(t *testing.T) {
	s := newSysServer(t)
	startSys(t, s, 20*time.Millisecond)

	all := dial(t, s).v5()
	all.connect("all", true, packet.Properties{})
	all.subscribe(1, packet.Subscription{Filter: "#", Qos: 0})
	plus := dial(t, s).v5()
	plus.connect("plus", true, packet.Properties{})
	plus.subscribe(1, packet.Subscription{Filter: "+/+", Qos: 0})

	// The wildcard subscriptions work — a normal retained message arrives.
	require.NoError(t, s.Publish("normal/topic", []byte("x"), true))
	for _, c := range []*testClient{all, plus} {
		pk := c.read()
		assert.Equal(t, "normal/topic", pk.TopicName)
	}
	// Several $SYS rounds elapse; nothing $SYS reaches the wildcard clients.
	assert.Nil(t, all.readMaybe(300*time.Millisecond))
	assert.Nil(t, plus.readMaybe(300*time.Millisecond))

	all.close()
	plus.close()
}
