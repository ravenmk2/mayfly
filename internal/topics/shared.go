package topics

import (
	"maps"
	"slices"
	"sync/atomic"
)

// SelectStrategy picks the index of the shared group member receiving the
// next publication (architecture decision A4). members are the group's
// sorted member ids, seq a per-group monotonically increasing cursor.
// RoundRobin is the current implementation; sticky/hash are reserved.
type SelectStrategy interface {
	Select(members []string, seq uint64) int
}

// RoundRobin selects members in strict rotation.
type RoundRobin struct{}

// Select implements SelectStrategy.
func (RoundRobin) Select(members []string, seq uint64) int {
	return int(seq % uint64(len(members)))
}

// sharedGroup holds the members of one shared subscription group attached to
// a trie node. Membership changes run under the owning particle write lock
// plus the global write lock; selection runs under the particle read lock
// with an atomic cursor, giving true round-robin rotation.
type sharedGroup struct {
	cursor  atomic.Uint64
	order   []string                // sorted member ids, rebuilt on change
	members map[string]Subscription // clientID → subscription
}

func newSharedGroup() *sharedGroup {
	return &sharedGroup{members: make(map[string]Subscription)}
}

// rebuildOrder refreshes the sorted member id slice. Callers hold the owning
// particle write lock.
func (g *sharedGroup) rebuildOrder() {
	g.order = slices.Sorted(maps.Keys(g.members))
}

// selectMember picks the next member using strategy. Callers hold the owning
// particle read lock; groups attached to the trie are never empty.
func (g *sharedGroup) selectMember(s SelectStrategy) (string, Subscription) {
	seq := g.cursor.Add(1) - 1
	id := g.order[s.Select(g.order, seq)]
	return id, g.members[id]
}
