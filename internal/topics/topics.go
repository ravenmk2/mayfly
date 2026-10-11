// Package topics implements the subscription trie with wildcard matching,
// shared subscriptions and the retained message store (architecture
// decisions A3/A4).
//
// The write path (Subscribe/Unsubscribe/Retain) is serialized by a global
// mutex; the read path (Subscribers/Messages) descends the trie with
// per-level read locks only. Empty nodes are trimmed under the write lock so
// readers never race a deletion.
package topics

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ravenmk2/mayfly/internal/packet"
)

var (
	// ErrInvalidFilter is returned for structurally invalid topic filters.
	ErrInvalidFilter = errors.New("topics: invalid topic filter")
	// ErrInvalidTopicName is returned for topic names containing wildcards.
	ErrInvalidTopicName = errors.New("topics: invalid topic name")
	// ErrEmptyClientID is returned when subscribing without a client id.
	ErrEmptyClientID = errors.New("topics: empty client id")
	// ErrInvalidRetainHandling is returned for Retain Handling values above
	// 2 [MQTT-3.8.3-4].
	ErrInvalidRetainHandling = errors.New("topics: retain handling must be 0, 1 or 2")
)

// Index is the subscription trie plus retained message store.
type Index struct {
	mu        sync.Mutex // global write lock
	root      *particle
	strategy  SelectStrategy
	subsCount atomic.Int64

	retainedMu sync.RWMutex
	retained   map[string]packet.Packet
}

// New returns an empty Index using round-robin shared group selection.
func New() *Index {
	return &Index{
		root:     newParticle("", nil),
		strategy: RoundRobin{},
		retained: make(map[string]packet.Packet),
	}
}

// SetSharedStrategy overrides the shared group member selection strategy
// (round-robin by default). It must be called before the Index serves
// concurrent matching.
func (x *Index) SetSharedStrategy(s SelectStrategy) {
	x.strategy = s
}

// Subscribe adds or replaces the subscription of clientID and reports
// whether it already existed; Retain Handling 1 callers use that to decide
// retained delivery [MQTT-3.8.3-1]. Shared subscriptions are stored under
// their inner filter with the group recorded on the leaf.
func (x *Index) Subscribe(clientID string, sub Subscription) (existed bool, err error) {
	if clientID == "" {
		return false, ErrEmptyClientID
	}
	if !IsValidFilter(sub.Filter) {
		return false, fmt.Errorf("%w: %q", ErrInvalidFilter, sub.Filter)
	}
	if sub.RetainHandling > 2 {
		return false, fmt.Errorf("%w: %d", ErrInvalidRetainHandling, sub.RetainHandling)
	}
	group, inner := splitSharedFilter(sub.Filter)
	sub.Shared = group
	sub.Filter = inner

	x.mu.Lock()
	defer x.mu.Unlock()
	leaf := x.root.leaf(strings.Split(inner, "/"))
	leaf.mu.Lock()
	defer leaf.mu.Unlock()
	if group == "" {
		_, existed = leaf.subs[clientID]
		leaf.subs[clientID] = sub
	} else {
		g, ok := leaf.shared[group]
		if !ok {
			g = newSharedGroup()
			leaf.shared[group] = g
		}
		_, existed = g.members[clientID]
		g.members[clientID] = sub
		g.rebuildOrder()
	}
	if !existed {
		x.subsCount.Add(1)
	}
	return existed, nil
}

// Unsubscribe removes the subscription of clientID for filter. Removing a
// non-existent subscription is a no-op. Emptied trie nodes are trimmed under
// the global write lock.
func (x *Index) Unsubscribe(clientID, filter string) error {
	if !IsValidFilter(filter) {
		return fmt.Errorf("%w: %q", ErrInvalidFilter, filter)
	}
	group, inner := splitSharedFilter(filter)

	x.mu.Lock()
	defer x.mu.Unlock()
	leaf := x.root.find(strings.Split(inner, "/"))
	if leaf == nil {
		return nil
	}
	removed := false
	leaf.mu.Lock()
	if group == "" {
		if _, ok := leaf.subs[clientID]; ok {
			delete(leaf.subs, clientID)
			removed = true
		}
	} else if g, ok := leaf.shared[group]; ok {
		if _, ok := g.members[clientID]; ok {
			delete(g.members, clientID)
			removed = true
			if len(g.members) == 0 {
				delete(leaf.shared, group)
			} else {
				g.rebuildOrder()
			}
		}
	}
	leaf.mu.Unlock()
	if removed {
		x.subsCount.Add(-1)
		trim(leaf, x.root)
	}
	return nil
}

// SubscriptionCount returns the total number of stored subscriptions,
// counting each shared group membership individually.
func (x *Index) SubscriptionCount() int64 {
	return x.subsCount.Load()
}
