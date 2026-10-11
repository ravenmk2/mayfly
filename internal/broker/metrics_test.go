package broker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/metrics"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// denyAuthz rejects every publish/subscribe authorization.
type denyAuthz struct{}

func (denyAuthz) Authorize(context.Context, hook.ClientInfo, hook.Op, string) hook.Verdict {
	return hook.VerdictDeny
}

func TestMetricsMessageAndByteCounters(t *testing.T) {
	s := newTestServer(t, nil)
	sub := dial(t, s).v5()
	sub.connect("sub", true, packet.Properties{})
	sub.subscribe(1, packet.Subscription{Filter: "m/#", Qos: 1})

	pub := dial(t, s).v5()
	pub.connect("pub", true, packet.Properties{})
	pub.publish(1, "m/topic", []byte("hello"), 7, packet.Properties{})
	ack := pub.read()
	require.Equal(t, packet.Puback, ack.FixedHeader.Type)

	delivered := sub.read()
	require.Equal(t, packet.Publish, delivered.FixedHeader.Type)
	assert.Equal(t, "m/topic", delivered.TopicName)

	assert.EqualValues(t, 1, s.Stats.Received(1))
	assert.EqualValues(t, 1, s.Stats.Sent(1))
	assert.EqualValues(t, 0, s.Stats.Received(0))
	assert.Positive(t, s.Stats.BytesReceived.Load())
	assert.Positive(t, s.Stats.BytesSent.Load())
	// Two CONNECTs authenticated; one subscribe and one publish authorized.
	assert.EqualValues(t, 2, s.Stats.AuthSucceeded.Load())
	assert.EqualValues(t, 2, s.Stats.ACLAllowed.Load())
	assert.Zero(t, s.Stats.ACLDenied.Load())

	sub.close()
	pub.close()
}

func TestMetricsACLDenied(t *testing.T) {
	s := newTestServer(t, func(o *Options) { o.Authorizer = hook.NewAuthzChain(denyAuthz{}) })
	c := dial(t, s).v5()
	c.connect("c", true, packet.Properties{})

	ack := c.subscribe(1, packet.Subscription{Filter: "a/#", Qos: 1})
	assert.Equal(t, []byte{packet.CodeNotAuthorized}, ack.ReasonCodes)

	c.publish(1, "a/b", []byte("x"), 2, packet.Properties{})
	puback := c.read()
	require.Equal(t, packet.Puback, puback.FixedHeader.Type)
	assert.Equal(t, packet.CodeNotAuthorized, puback.ReasonCode)

	assert.EqualValues(t, 2, s.Stats.ACLDenied.Load())
	assert.Zero(t, s.Stats.ACLAllowed.Load())
	assert.EqualValues(t, 1, s.Stats.MessagesDropped.Load())
	c.close()
}

func TestMetricsCustomInstance(t *testing.T) {
	m := metrics.New("9.9.9")
	s := newTestServer(t, func(o *Options) { o.Metrics = m })
	assert.Same(t, m, s.Stats)
	c := dial(t, s).v3()
	c.connect("c", true, packet.Properties{})
	assert.EqualValues(t, 1, m.ConnectionsCurrent.Load())
	c.close()
}

func TestRefreshMetricsGauges(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("sub", true, packet.Properties{})
	c.subscribe(1, packet.Subscription{Filter: "g/#", Qos: 0})

	require.NoError(t, s.Publish("g/one", []byte("1"), true))
	s.RefreshMetrics()

	assert.EqualValues(t, 1, s.Stats.Subscriptions.Load())
	assert.EqualValues(t, 1, s.Stats.Retained.Load())
	assert.EqualValues(t, s.Topics().RetainedCount(), int(s.Stats.Retained.Load()))
	c.close()
}

func TestPublishInternalValidation(t *testing.T) {
	s := newTestServer(t, nil)
	assert.Error(t, s.Publish("bad/#/topic", []byte("x"), false))
	assert.NoError(t, s.Publish("ok/topic", []byte("x"), false))
}

// TestPublishInternalDelivers ensures the internal publish entry fans out to
// live subscribers and stores retained messages for later ones.
func TestPublishInternalDelivers(t *testing.T) {
	s := newTestServer(t, nil)
	live := dial(t, s).v5()
	live.connect("live", true, packet.Properties{})
	live.subscribe(1, packet.Subscription{Filter: "internal/#", Qos: 0})

	require.NoError(t, s.Publish("internal/news", []byte("hello"), true))
	got := live.read()
	assert.Equal(t, "internal/news", got.TopicName)
	assert.Equal(t, []byte("hello"), got.Payload)

	late := dial(t, s).v5()
	late.connect("late", true, packet.Properties{})
	late.subscribe(1, packet.Subscription{Filter: "internal/#", Qos: 0})
	replayed := late.read()
	assert.Equal(t, "internal/news", replayed.TopicName)
	assert.True(t, replayed.FixedHeader.Retain)

	// Internal traffic does not count towards the client message counters.
	assert.Zero(t, s.Stats.Received(0))
	assert.Zero(t, s.Stats.MessagesDropped.Load())

	live.close()
	late.close()
}
