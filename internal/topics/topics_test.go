package topics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidTopicName(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		valid bool
	}{
		{"simple", "a/b/c", true},
		{"single level", "a", true},
		{"empty levels", "a//b", true},
		{"dollar prefix", "$SYS/broker/uptime", true},
		{"empty", "", false},
		{"contains hash", "a/#", false},
		{"contains plus", "a/+/c", false},
		{"bare hash", "#", false},
		{"bare plus", "+", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, IsValidTopicName(tc.topic))
		})
	}
}

func TestIsValidFilter(t *testing.T) {
	cases := []struct {
		name   string
		filter string
		valid  bool
	}{
		{"simple", "a/b/c", true},
		{"hash terminal", "a/#", true},
		{"bare hash", "#", true},
		{"plus level", "a/+/c", true},
		{"bare plus", "+", true},
		{"plus then hash", "+/+/#", true},
		{"empty levels", "a//b", true},
		{"dollar filter", "$SYS/#", true},
		{"empty", "", false},
		{"hash not alone", "a/b#", false},
		{"hash not terminal", "a/#/c", false},
		{"plus not alone", "a/b+", false},
		{"shared", "$share/g/a/b", true},
		{"shared with wildcards", "$share/g/+/+#", false},
		{"shared trailing hash", "$share/g/a/#", true},
		{"shared missing group", "$share//a/b", false},
		{"shared missing filter", "$share/g", false},
		{"shared empty filter", "$share/g/", false},
		{"shared wildcard group", "$share/g+/a", false},
		{"shared hash group", "$share/#/a", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.valid, IsValidFilter(tc.filter))
		})
	}
}

func TestSubscribeValidation(t *testing.T) {
	x := New()
	_, err := x.Subscribe("", Subscription{Filter: "a/b"})
	require.ErrorIs(t, err, ErrEmptyClientID)

	_, err = x.Subscribe("c1", Subscription{Filter: "a/#/c"})
	require.ErrorIs(t, err, ErrInvalidFilter)

	_, err = x.Subscribe("c1", Subscription{Filter: ""})
	require.ErrorIs(t, err, ErrInvalidFilter)

	_, err = x.Subscribe("c1", Subscription{Filter: "a/b", RetainHandling: 3})
	require.ErrorIs(t, err, ErrInvalidRetainHandling)

	assert.EqualValues(t, 0, x.SubscriptionCount())
}

func TestSubscribeUnsubscribe(t *testing.T) {
	x := New()

	existed, err := x.Subscribe("c1", Subscription{Filter: "a/b/c", Qos: 1})
	require.NoError(t, err)
	assert.False(t, existed)
	assert.EqualValues(t, 1, x.SubscriptionCount())

	// Replacing an existing subscription keeps the count and reports it.
	existed, err = x.Subscribe("c1", Subscription{Filter: "a/b/c", Qos: 2})
	require.NoError(t, err)
	assert.True(t, existed)
	assert.EqualValues(t, 1, x.SubscriptionCount())

	leaf := x.root.find([]string{"a", "b", "c"})
	require.NotNil(t, leaf)
	leaf.mu.RLock()
	sub, ok := leaf.subs["c1"]
	leaf.mu.RUnlock()
	require.True(t, ok)
	assert.EqualValues(t, 2, sub.Qos)

	_, err = x.Subscribe("c2", Subscription{Filter: "a/b/+", Qos: 0})
	require.NoError(t, err)
	assert.EqualValues(t, 2, x.SubscriptionCount())

	require.NoError(t, x.Unsubscribe("c1", "a/b/c"))
	assert.EqualValues(t, 1, x.SubscriptionCount())

	// Unsubscribing twice or for unknown clients/filters is a no-op.
	require.NoError(t, x.Unsubscribe("c1", "a/b/c"))
	require.NoError(t, x.Unsubscribe("c9", "a/b/+"))
	require.NoError(t, x.Unsubscribe("c1", "not/present"))
	assert.EqualValues(t, 1, x.SubscriptionCount())

	require.ErrorIs(t, x.Unsubscribe("c1", "a/#/c"), ErrInvalidFilter)
}

func TestTrimRemovesEmptyBranches(t *testing.T) {
	x := New()
	_, err := x.Subscribe("c1", Subscription{Filter: "a/b/c/d"})
	require.NoError(t, err)

	require.NoError(t, x.Unsubscribe("c1", "a/b/c/d"))

	// The whole branch must be trimmed back to the root.
	x.root.mu.RLock()
	remaining := len(x.root.children)
	x.root.mu.RUnlock()
	assert.Zero(t, remaining)
	assert.Nil(t, x.root.find([]string{"a"}))
}

func TestTrimKeepsNonEmptyNodes(t *testing.T) {
	x := New()
	_, err := x.Subscribe("c1", Subscription{Filter: "a/b/c"})
	require.NoError(t, err)
	_, err = x.Subscribe("c2", Subscription{Filter: "a/b"})
	require.NoError(t, err)

	require.NoError(t, x.Unsubscribe("c1", "a/b/c"))

	// a/b still carries c2's subscription, a still leads to it.
	leaf := x.root.find([]string{"a", "b"})
	require.NotNil(t, leaf)
	leaf.mu.RLock()
	_, ok := leaf.subs["c2"]
	leaf.mu.RUnlock()
	assert.True(t, ok)
	assert.Nil(t, x.root.find([]string{"a", "b", "c"}))
}

func TestSplitSharedFilter(t *testing.T) {
	cases := []struct {
		filter string
		group  string
		inner  string
	}{
		{"a/b", "", "a/b"},
		{"$share/g/a/b", "g", "a/b"},
		{"$share/g/#", "g", "#"},
		{"$share//a/b", "", "$share//a/b"},
		{"$share/g", "", "$share/g"},
		{"$share/g/", "", "$share/g/"},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			group, inner := splitSharedFilter(tc.filter)
			assert.Equal(t, tc.group, group)
			assert.Equal(t, tc.inner, inner)
		})
	}
}

func TestSubscriptionMerge(t *testing.T) {
	a := Subscription{Filter: "a/#", Qos: 1, Identifiers: []uint32{3, 1}}
	b := Subscription{Filter: "a/b", Qos: 2, Identifiers: []uint32{2, 3}}

	m := a.Merge(b)
	assert.EqualValues(t, 2, m.Qos)
	assert.Equal(t, []uint32{1, 2, 3}, m.Identifiers)

	// Merging with a lower QoS keeps the higher one.
	m = b.Merge(a)
	assert.EqualValues(t, 2, m.Qos)
	assert.Equal(t, []uint32{1, 2, 3}, m.Identifiers)

	// Empty identifier sides pass through.
	c := Subscription{Filter: "a/b", Qos: 0}
	assert.Nil(t, c.Merge(Subscription{Filter: "a/+", Qos: 0}).Identifiers)
	assert.Equal(t, []uint32{7}, c.Merge(Subscription{Identifiers: []uint32{7}}).Identifiers)
}
