package topics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedSelectsOneMemberPerGroup(t *testing.T) {
	x := New()
	for i := 0; i < 3; i++ {
		_, err := x.Subscribe(fmt.Sprintf("m%d", i), Subscription{Filter: "$share/g1/a/b", Qos: 1})
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		_, err := x.Subscribe(fmt.Sprintf("n%d", i), Subscription{Filter: "$share/g2/a/b", Qos: 1})
		require.NoError(t, err)
	}

	got := x.Subscribers("a/b")
	require.Len(t, got, 2, "each group contributes exactly one member")
	assert.Contains(t, []string{"m0", "m1", "m2"}, firstWithShared(got, "g1"))
	assert.Contains(t, []string{"n0", "n1"}, firstWithShared(got, "g2"))
}

func firstWithShared(subs Subscriptions, group string) string {
	for id, sub := range subs {
		if sub.Shared == group {
			return id
		}
	}
	return ""
}

func TestSharedRoundRobinUniformity(t *testing.T) {
	x := New()
	members := []string{"m0", "m1", "m2"}
	for _, id := range members {
		_, err := x.Subscribe(id, Subscription{Filter: "$share/g/a/#"})
		require.NoError(t, err)
	}

	const rounds = 3000
	counts := map[string]int{}
	for i := 0; i < rounds; i++ {
		got := x.Subscribers("a/b")
		require.Len(t, got, 1)
		for id := range got {
			counts[id]++
		}
	}
	// True round-robin: every member is selected exactly rounds/len times.
	for _, id := range members {
		assert.Equal(t, rounds/len(members), counts[id], "member %s", id)
	}
}

func TestSharedRotationAdvancesPerGroup(t *testing.T) {
	x := New()
	for _, id := range []string{"a1", "a2"} {
		_, err := x.Subscribe(id, Subscription{Filter: "$share/ga/t"})
		require.NoError(t, err)
	}
	for _, id := range []string{"b1", "b2"} {
		_, err := x.Subscribe(id, Subscription{Filter: "$share/gb/t"})
		require.NoError(t, err)
	}

	// Both groups rotate independently.
	for round := 0; round < 2; round++ {
		got := x.Subscribers("t")
		require.Len(t, got, 2)
		assert.Equal(t, fmt.Sprintf("a%d", round+1), firstWithShared(got, "ga"))
		assert.Equal(t, fmt.Sprintf("b%d", round+1), firstWithShared(got, "gb"))
	}
}

func TestSharedDeduplicatesWithRegularSubscription(t *testing.T) {
	x := New()
	// c1 is the only group member, so it is always selected; it also holds a
	// regular subscription on the same inner filter.
	_, err := x.Subscribe("c1", Subscription{Filter: "a/b", Qos: 1, Identifiers: []uint32{1}})
	require.NoError(t, err)
	_, err = x.Subscribe("c1", Subscription{Filter: "$share/g/a/b", Qos: 2, Identifiers: []uint32{2}})
	require.NoError(t, err)

	got := x.Subscribers("a/b")
	require.Len(t, got, 1, "shared and regular hits for one client must merge")
	sub := got["c1"]
	assert.EqualValues(t, 2, sub.Qos)
	assert.Equal(t, []uint32{1, 2}, sub.Identifiers)
}

func TestSharedUnsubscribeMembership(t *testing.T) {
	x := New()
	_, err := x.Subscribe("m0", Subscription{Filter: "$share/g/a/b"})
	require.NoError(t, err)
	_, err = x.Subscribe("m1", Subscription{Filter: "$share/g/a/b"})
	require.NoError(t, err)
	assert.EqualValues(t, 2, x.SubscriptionCount())

	require.NoError(t, x.Unsubscribe("m0", "$share/g/a/b"))
	assert.EqualValues(t, 1, x.SubscriptionCount())
	for i := 0; i < 10; i++ {
		got := x.Subscribers("a/b")
		require.Len(t, got, 1)
		assert.Contains(t, got, "m1")
	}

	// Removing the last member dissolves the group and trims the branch.
	require.NoError(t, x.Unsubscribe("m1", "$share/g/a/b"))
	assert.EqualValues(t, 0, x.SubscriptionCount())
	assert.Nil(t, x.Subscribers("a/b"))
	assert.Nil(t, x.root.find([]string{"a"}))
}

func TestSharedGroupIsolation(t *testing.T) {
	x := New()
	// Same group name on different filters are independent groups.
	_, err := x.Subscribe("m0", Subscription{Filter: "$share/g/a/b"})
	require.NoError(t, err)
	_, err = x.Subscribe("m1", Subscription{Filter: "$share/g/c/d"})
	require.NoError(t, err)

	got := x.Subscribers("a/b")
	require.Len(t, got, 1)
	assert.Contains(t, got, "m0")
	got = x.Subscribers("c/d")
	require.Len(t, got, 1)
	assert.Contains(t, got, "m1")
}
