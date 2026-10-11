package auth

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
)

func allow(seq int, user, perm, topic string) Rule {
	return Rule{Seq: seq, Username: user, Action: ActionAllow, Permission: mustPerm(perm), Topic: topic}
}

// global returns a rule with no subject constraint.
func global(seq int, action Action, perm Permission, topic string) Rule {
	return Rule{Seq: seq, Action: action, Permission: perm, Topic: topic}
}

func mustPerm(s string) Permission {
	switch s {
	case "publish":
		return PermissionPublish
	case "subscribe":
		return PermissionSubscribe
	case "pubsub":
		return PermissionPubsub
	default:
		panic("bad permission " + s)
	}
}

func TestRuleSetMatching(t *testing.T) {
	rules := []Rule{
		global(1, ActionAllow, PermissionPubsub, "lit/exact"),
		global(2, ActionAllow, PermissionPubsub, "plus/+/end"),
		global(3, ActionAllow, PermissionPubsub, "hash/#"),
		global(4, ActionAllow, PermissionPubsub, "up/{user}"),
		global(5, ActionAllow, PermissionPubsub, "dev/{clientid}/in"),
		global(6, ActionAllow, PermissionPubsub, "mix/{user}/+/{clientid}/#"),
		global(7, ActionDeny, PermissionPubsub, "$SYS/#"),
		global(8, ActionAllow, PermissionPubsub, "#"),
	}
	rs, err := NewRuleSet(rules, 1)
	require.NoError(t, err)

	cases := []struct {
		name     string
		username string
		clientID string
		op       hook.Op
		topic    string
		want     hook.Verdict
	}{
		{"literal hit", "alice", "c1", hook.OpPublish, "lit/exact", hook.VerdictAllow},
		{"literal miss falls to hash-all", "alice", "c1", hook.OpPublish, "lit/other", hook.VerdictAllow},
		{"plus matches one level", "alice", "c1", hook.OpPublish, "plus/x/end", hook.VerdictAllow},
		{"hash matches suffix", "alice", "c1", hook.OpPublish, "hash/a/b/c", hook.VerdictAllow},
		{"hash matches zero levels", "alice", "c1", hook.OpPublish, "hash", hook.VerdictAllow},
		{"varUser matches username", "alice", "c1", hook.OpPublish, "up/alice", hook.VerdictAllow},
		{"varUser rejects other user level", "alice", "c1", hook.OpPublish, "up/bob", hook.VerdictAllow}, // seq 8 catch-all
		{"varClient matches", "alice", "c1", hook.OpPublish, "dev/c1/in", hook.VerdictAllow},
		{"varClient other falls to catch-all", "alice", "c2", hook.OpPublish, "dev/c2/in", hook.VerdictAllow},
		{"mixed placeholders", "alice", "c1", hook.OpPublish, "mix/alice/x/c1/a/b", hook.VerdictAllow},
		{"dollar topic skipped by wildcard rules", "alice", "c1", hook.OpPublish, "$SYS/broker/uptime", hook.VerdictDeny},
		{"dollar topic other branch denied", "alice", "c1", hook.OpPublish, "$SYS/x", hook.VerdictDeny},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, rs.Decide(tc.username, tc.clientID, tc.op, tc.topic))
		})
	}
}

// TestRuleSetFirstHitOrder pins seq ordering: the first matching rule wins,
// so a deny placed before an allow denies.
func TestRuleSetFirstHitOrder(t *testing.T) {
	rs, err := NewRuleSet([]Rule{
		global(1, ActionDeny, PermissionPubsub, "a/b"),
		global(2, ActionAllow, PermissionPubsub, "a/b"),
	}, 1)
	require.NoError(t, err)
	assert.Equal(t, hook.VerdictDeny, rs.Decide("u", "c", hook.OpPublish, "a/b"))

	rs, err = NewRuleSet([]Rule{
		global(1, ActionAllow, PermissionPubsub, "a/b"),
		global(2, ActionDeny, PermissionPubsub, "a/b"),
	}, 1)
	require.NoError(t, err)
	assert.Equal(t, hook.VerdictAllow, rs.Decide("u", "c", hook.OpPublish, "a/b"))
}

// TestRuleSetDefaultIgnore: no matching rule yields VerdictIgnore; the chain
// turns that into a default deny.
func TestRuleSetDefaultIgnore(t *testing.T) {
	rs, err := NewRuleSet([]Rule{global(1, ActionAllow, PermissionPublish, "only/this")}, 1)
	require.NoError(t, err)
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("u", "c", hook.OpPublish, "other/topic"))
	// Permission does not cover the op.
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("u", "c", hook.OpSubscribe, "only/this"))
}

// TestRuleSetBucketIsolation: rules bucketed to one user must not affect
// another user's decisions.
func TestRuleSetBucketIsolation(t *testing.T) {
	rs, err := NewRuleSet([]Rule{
		allow(1, "alice", "pubsub", "devices/#"),
		{Seq: 2, ClientID: "dev1", Action: ActionAllow, Permission: PermissionPubsub, Topic: "c/#"},
	}, 1)
	require.NoError(t, err)

	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "x", hook.OpPublish, "devices/d1"))
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("bob", "x", hook.OpPublish, "devices/d1"))

	assert.Equal(t, hook.VerdictAllow, rs.Decide("bob", "dev1", hook.OpPublish, "c/x"))
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("bob", "dev2", hook.OpPublish, "c/x"))
}

// TestRuleSetBucketMergeOrder: user, clientid and global buckets merge by
// seq; the lowest seq across buckets wins.
func TestRuleSetBucketMergeOrder(t *testing.T) {
	rs, err := NewRuleSet([]Rule{
		global(5, ActionAllow, PermissionPubsub, "t/#"),
		allow(6, "alice", "pubsub", "t/#"),
		allow(2, "alice", "pubsub", "alice/#"),
		{Seq: 4, ClientID: "c1", Action: ActionDeny, Permission: PermissionPubsub, Topic: "t/secret"},
	}, 1)
	require.NoError(t, err)

	// seq 4 deny beats the seq 5 global and seq 6 user allows.
	assert.Equal(t, hook.VerdictDeny, rs.Decide("alice", "c1", hook.OpPublish, "t/secret"))
	// seq 5 global beats the seq 6 user rule for other topics.
	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "c1", hook.OpPublish, "t/open"))
	// Without the user bucket the clientid deny is still the first hit.
	assert.Equal(t, hook.VerdictDeny, rs.Decide("mallory", "c1", hook.OpPublish, "t/secret"))
	assert.Equal(t, hook.VerdictAllow, rs.Decide("mallory", "c1", hook.OpPublish, "t/open"))
	// The user bucket contributes its own rules.
	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "c1", hook.OpPublish, "alice/x"))
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("mallory", "c1", hook.OpPublish, "alice/x"))
}

// TestRuleSetPermissionGating: a publish-only rule does not authorize a
// subscribe even when the topic matches.
func TestRuleSetPermissionGating(t *testing.T) {
	rs, err := NewRuleSet([]Rule{global(1, ActionAllow, PermissionPublish, "up/#")}, 1)
	require.NoError(t, err)
	assert.Equal(t, hook.VerdictAllow, rs.Decide("u", "c", hook.OpPublish, "up/x"))
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("u", "c", hook.OpSubscribe, "up/x"))
}

// TestRuleSetSubscribeFilterMatching: subscribe authorization receives the
// requested filter verbatim; placeholders compare level-wise.
func TestRuleSetSubscribeFilterMatching(t *testing.T) {
	rs, err := NewRuleSet([]Rule{
		global(1, ActionAllow, PermissionSubscribe, "down/{user}"),
		global(2, ActionAllow, PermissionSubscribe, "broadcast/#"),
	}, 1)
	require.NoError(t, err)

	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "c", hook.OpSubscribe, "down/alice"))
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("alice", "c", hook.OpSubscribe, "down/bob"))
	// A wildcarded subscribe filter does not satisfy the placeholder level.
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("alice", "c", hook.OpSubscribe, "down/+"))
	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "c", hook.OpSubscribe, "broadcast/#"))
	assert.Equal(t, hook.VerdictAllow, rs.Decide("alice", "c", hook.OpSubscribe, "broadcast/a/b"))
}

// TestRuleSetPlaceholderRequiresSubject: anonymous clients (empty username)
// never match {user} placeholder levels.
func TestRuleSetPlaceholderRequiresSubject(t *testing.T) {
	rs, err := NewRuleSet([]Rule{global(1, ActionAllow, PermissionPubsub, "up/{user}")}, 1)
	require.NoError(t, err)
	assert.Equal(t, hook.VerdictIgnore, rs.Decide("", "c1", hook.OpPublish, "up/anything"))
}

func TestRuleSetInvalidFilters(t *testing.T) {
	for _, filter := range []string{"", "a/#/b", "a/b#", "a/+b"} {
		_, err := NewRuleSet([]Rule{global(1, ActionAllow, PermissionPubsub, filter)}, 1)
		assert.Error(t, err, "filter %q", filter)
	}
}

// TestRuleSetDecisionCache: repeated identical decisions are served from the
// cache, and a fresh RuleSet (new generation) starts with an empty cache.
func TestRuleSetDecisionCache(t *testing.T) {
	rs, err := NewRuleSet([]Rule{global(1, ActionAllow, PermissionPubsub, "a/#")}, 1)
	require.NoError(t, err)

	assert.Equal(t, hook.VerdictAllow, rs.Decide("u", "c", hook.OpPublish, "a/b"))
	rs.mu.Lock()
	assert.Len(t, rs.cache, 1)
	rs.mu.Unlock()

	// Same key hits the cache.
	assert.Equal(t, hook.VerdictAllow, rs.Decide("u", "c", hook.OpPublish, "a/b"))
	rs.mu.Lock()
	assert.Len(t, rs.cache, 1)
	rs.mu.Unlock()

	// A refresh builds a new set with a bumped generation and empty cache.
	rs2, err := NewRuleSet([]Rule{global(1, ActionDeny, PermissionPubsub, "a/#")}, rs.Generation()+1)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rs2.Generation())
	assert.Equal(t, hook.VerdictDeny, rs2.Decide("u", "c", hook.OpPublish, "a/b"))
}

// TestRuleSetConcurrentDecide hammers Decide from many goroutines to shake
// out cache races under -race.
func TestRuleSetConcurrentDecide(t *testing.T) {
	rs, err := NewRuleSet([]Rule{
		global(1, ActionDeny, PermissionPubsub, "$SYS/#"),
		global(2, ActionAllow, PermissionPublish, "up/{user}"),
		global(3, ActionAllow, PermissionSubscribe, "down/{user}"),
	}, 1)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			user := fmt.Sprintf("user%d", i%8)
			for j := 0; j < 200; j++ {
				assert.Equal(t, hook.VerdictAllow, rs.Decide(user, "c", hook.OpPublish, "up/"+user))
				assert.Equal(t, hook.VerdictIgnore, rs.Decide(user, "c", hook.OpPublish, "up/other"))
				assert.Equal(t, hook.VerdictDeny, rs.Decide(user, "c", hook.OpPublish, "$SYS/x"))
			}
		}(i)
	}
	wg.Wait()
}
