package topics

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func retained(topic, payload string) packet.Packet {
	return packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
		TopicName:   topic,
		Payload:     []byte(payload),
	}
}

func TestRetainSetAndDelete(t *testing.T) {
	x := New()
	require.NoError(t, x.Retain(retained("a/b", "one")))
	require.NoError(t, x.Retain(retained("a/c", "two")))
	assert.Equal(t, 2, x.RetainedCount())

	msgs := x.Messages(Subscription{Filter: "a/+"}, false)
	require.Len(t, msgs, 2)
	assert.Equal(t, "a/b", msgs[0].TopicName)
	assert.Equal(t, "one", string(msgs[0].Payload))
	assert.Equal(t, "a/c", msgs[1].TopicName)

	// An empty payload removes the retained message [MQTT-3.3.1-6].
	require.NoError(t, x.Retain(retained("a/b", "")))
	assert.Equal(t, 1, x.RetainedCount())
	msgs = x.Messages(Subscription{Filter: "a/+"}, false)
	require.Len(t, msgs, 1)
	assert.Equal(t, "a/c", msgs[0].TopicName)

	// Deleting again and deleting unknown topics are no-ops.
	require.NoError(t, x.Retain(retained("a/b", "")))
	require.NoError(t, x.Retain(retained("not/present", "")))
	assert.Equal(t, 1, x.RetainedCount())
}

func TestRetainRejectsInvalidTopic(t *testing.T) {
	x := New()
	require.ErrorIs(t, x.Retain(retained("", "x")), ErrInvalidTopicName)
	require.ErrorIs(t, x.Retain(retained("a/+", "x")), ErrInvalidTopicName)
	require.ErrorIs(t, x.Retain(retained("a/#", "x")), ErrInvalidTopicName)
	assert.Zero(t, x.RetainedCount())
}

func TestRetainNormalizesFlagAndClonesPayload(t *testing.T) {
	x := New()
	pk := retained("a/b", "data")
	pk.FixedHeader.Retain = false // stored form always carries the flag
	require.NoError(t, x.Retain(pk))

	pk.Payload[0] = 'X' // mutating the caller's buffer must not corrupt state
	msgs := x.Messages(Subscription{Filter: "a/b"}, false)
	require.Len(t, msgs, 1)
	assert.True(t, msgs[0].FixedHeader.Retain)
	assert.Equal(t, "data", string(msgs[0].Payload))
}

func TestRetainDeleteTrimsBranchButKeepsSubscriptions(t *testing.T) {
	x := New()
	_, err := x.Subscribe("c1", Subscription{Filter: "a/b"})
	require.NoError(t, err)
	require.NoError(t, x.Retain(retained("a/b/c/d", "x")))
	require.NoError(t, x.Retain(retained("a/b/c/d", "")))
	assert.Nil(t, x.root.find([]string{"a", "b", "c"}))
	assert.NotNil(t, x.root.find([]string{"a", "b"}))
}

func TestMessagesRetainHandling(t *testing.T) {
	setup := func() *Index {
		x := New()
		require.NoError(t, x.Retain(retained("a/b", "one")))
		return x
	}

	cases := []struct {
		name           string
		retainHandling byte
		existed        bool
		want           int
	}{
		{"handling 0 new subscription", 0, false, 1},
		{"handling 0 existing subscription", 0, true, 1},
		{"handling 1 new subscription", 1, false, 1},
		{"handling 1 existing subscription", 1, true, 0}, // [MQTT-3.8.3-1]
		{"handling 2 new subscription", 2, false, 0},
		{"handling 2 existing subscription", 2, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := setup()
			sub := Subscription{Filter: "a/+", RetainHandling: tc.retainHandling}
			assert.Len(t, x.Messages(sub, tc.existed), tc.want)
		})
	}
}

func TestMessagesSkipsSharedSubscriptions(t *testing.T) {
	x := New()
	require.NoError(t, x.Retain(retained("a/b", "one")))
	assert.Nil(t, x.Messages(Subscription{Filter: "$share/g/a/+"}, false))
}

func TestMessagesFilterMatching(t *testing.T) {
	x := New()
	for _, topic := range []string{"a/b", "a/c", "a/b/c", "d", "$SYS/broker"} {
		require.NoError(t, x.Retain(retained(topic, "x")))
	}

	cases := []struct {
		filter string
		want   []string
	}{
		{"a/b", []string{"a/b"}},
		{"a/+", []string{"a/b", "a/c"}},
		{"a/#", []string{"a/b", "a/b/c", "a/c"}},
		{"#", []string{"a/b", "a/b/c", "a/c", "d"}}, // no $SYS [MQTT-4.7.2-1]
		{"+/b", []string{"a/b"}},
		{"$SYS/#", []string{"$SYS/broker"}},
		{"d/#", []string{"d"}}, // filter/# matches filter itself [MQTT-4.7.1-2]
		{"not/present", nil},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			msgs := x.Messages(Subscription{Filter: tc.filter}, false)
			var got []string
			for _, m := range msgs {
				got = append(got, m.TopicName)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMessagesRetainAsPublished(t *testing.T) {
	x := New()
	require.NoError(t, x.Retain(retained("a/b", "one")))

	// RetainAsPublished unset: the RETAIN flag is forced for messages sent
	// on a new subscription [MQTT-3.3.1-9].
	msgs := x.Messages(Subscription{Filter: "a/b"}, false)
	require.Len(t, msgs, 1)
	assert.True(t, msgs[0].FixedHeader.Retain)

	// RetainAsPublished set: the stored flag passes through.
	msgs = x.Messages(Subscription{Filter: "a/b", RetainAsPublished: true}, false)
	require.Len(t, msgs, 1)
	assert.True(t, msgs[0].FixedHeader.Retain)
}

func TestConcurrentReadWrite(t *testing.T) {
	x := New()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("w%d-c%d", w, i%16)
				filter := fmt.Sprintf("a/%d/#", i%8)
				_, _ = x.Subscribe(id, Subscription{Filter: filter})
				_ = x.Unsubscribe(id, filter)
				_, _ = x.Subscribe(id, Subscription{Filter: fmt.Sprintf("$share/g%d/a/+", w)})
				_ = x.Retain(retained(fmt.Sprintf("a/%d/b", i%8), "x"))
				_ = x.Retain(retained(fmt.Sprintf("a/%d/b", i%8), ""))
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = x.Subscribers(fmt.Sprintf("a/%d/b", i%8))
				_ = x.Messages(Subscription{Filter: "a/#"}, false)
			}
		}()
	}
	wg.Wait()
}
