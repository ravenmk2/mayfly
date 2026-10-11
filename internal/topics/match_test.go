package topics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribersMatching(t *testing.T) {
	cases := []struct {
		name    string
		filters []string
		topic   string
		want    []string // filter indexes expected to match
	}{
		{"exact", []string{"a/b/c"}, "a/b/c", []string{"a/b/c"}},
		{"exact miss", []string{"a/b/c"}, "a/b/d", nil},
		{"prefix is not a match", []string{"a/b"}, "a/b/c", nil},
		{"deeper is not a match", []string{"a/b/c"}, "a/b", nil},
		{"plus single level", []string{"a/+/c"}, "a/b/c", []string{"a/+/c"}},
		{"plus misses extra level", []string{"a/+/c"}, "a/b/b/c", nil},
		{"plus matches empty level", []string{"a/+/c"}, "a//c", []string{"a/+/c"}},
		{"leading plus", []string{"+/b/c"}, "a/b/c", []string{"+/b/c"}},
		{"hash multi level", []string{"a/#"}, "a/b/c/d", []string{"a/#"}},
		{"hash matches parent itself", []string{"a/#"}, "a", []string{"a/#"}}, // [MQTT-4.7.1-2]
		{"bare hash matches all", []string{"#"}, "a/b/c", []string{"#"}},
		{"hash matches single level", []string{"#"}, "a", []string{"#"}},
		{"plus and hash mix", []string{"a/+/#"}, "a/b/c/d", []string{"a/+/#"}},
		{"empty level exact", []string{"a//b"}, "a//b", []string{"a//b"}},
		{"trailing slash topic", []string{"a/+"}, "a/", []string{"a/+"}},
		{"filter hash own level only miss", []string{"a/b/#"}, "a", nil},
		// Top-level wildcards MUST NOT match '$' topics [MQTT-4.7.2-1].
		{"sys hidden from bare hash", []string{"#"}, "$SYS/broker", nil},
		{"sys hidden from plus", []string{"+/broker"}, "$SYS/broker", nil},
		{"sys explicit hash", []string{"$SYS/#"}, "$SYS/broker", []string{"$SYS/#"}},
		{"sys exact", []string{"$SYS/broker"}, "$SYS/broker", []string{"$SYS/broker"}},
		{"sys deeper wildcard ok", []string{"$share/g/$SYS/+"}, "$SYS/x", []string{"$share/g/$SYS/+"}},
		// Multiple filters, multiple subscribers.
		{"fanout", []string{"a/b", "a/+", "a/#", "b/b"}, "a/b", []string{"a/b", "a/+", "a/#"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := New()
			for i, f := range tc.filters {
				_, err := x.Subscribe(fmt.Sprintf("c%d", i), Subscription{Filter: f})
				require.NoError(t, err)
			}
			got := x.Subscribers(tc.topic)
			want := Subscriptions{}
			for _, f := range tc.want {
				for i, sub := range tc.filters {
					if sub == f {
						want[fmt.Sprintf("c%d", i)] = Subscription{Filter: f}
					}
				}
			}
			if len(want) == 0 {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			ids := make([]string, 0, len(got))
			for id := range got {
				ids = append(ids, id)
			}
			assert.ElementsMatch(t, subIDs(want), ids)
		})
	}
}

// subIDs returns the client ids of subs as a slice.
func subIDs(subs Subscriptions) []string {
	ids := make([]string, 0, len(subs))
	for id := range subs {
		ids = append(ids, id)
	}
	return ids
}

func TestSubscribersRejectsInvalidTopics(t *testing.T) {
	x := New()
	_, err := x.Subscribe("c1", Subscription{Filter: "#"})
	require.NoError(t, err)

	assert.Nil(t, x.Subscribers(""))
	assert.Nil(t, x.Subscribers("a/+/c"))
	assert.Nil(t, x.Subscribers("a/#"))
}

func TestSubscribersMergesSameClient(t *testing.T) {
	x := New()
	_, err := x.Subscribe("c1", Subscription{Filter: "a/#", Qos: 0, Identifiers: []uint32{1}})
	require.NoError(t, err)
	_, err = x.Subscribe("c1", Subscription{Filter: "a/+", Qos: 1, Identifiers: []uint32{2}})
	require.NoError(t, err)
	_, err = x.Subscribe("c1", Subscription{Filter: "a/b", Qos: 2, Identifiers: []uint32{3}})
	require.NoError(t, err)

	got := x.Subscribers("a/b")
	require.Len(t, got, 1)
	sub := got["c1"]
	// The highest QoS wins and all Subscription Identifiers are unioned.
	assert.EqualValues(t, 2, sub.Qos)
	assert.Equal(t, []uint32{1, 2, 3}, sub.Identifiers)
}
