package topics

import (
	"slices"
	"strings"
)

// sharedPrefix prefixes shared subscription filters [MQTT-4.8.2].
const sharedPrefix = "$share/"

// Subscription describes a single client subscription stored in the trie.
type Subscription struct {
	// Filter is the topic filter without the $share/{group} prefix.
	Filter            string
	Qos               byte
	NoLocal           bool
	RetainAsPublished bool
	// RetainHandling controls retained delivery at subscribe time: 0 send,
	// 1 send only for new subscriptions, 2 never send [MQTT-3.8.3-1].
	RetainHandling byte
	// Identifiers carries the v5 Subscription Identifiers. When several
	// filters of one client match the same publication the identifiers are
	// unioned by Merge.
	Identifiers []uint32
	// Shared is the shared subscription group name, empty for regular
	// subscriptions [MQTT-4.8.2].
	Shared string
}

// Merge combines two subscriptions of the same client matching one
// publication: the highest QoS wins and the Subscription Identifiers are
// unioned. Other flags are kept from the receiver.
func (s Subscription) Merge(o Subscription) Subscription {
	if o.Qos > s.Qos {
		s.Qos = o.Qos
	}
	s.Identifiers = mergeIdentifiers(s.Identifiers, o.Identifiers)
	return s
}

// Subscriptions maps client IDs to their merged subscription.
type Subscriptions map[string]Subscription

// merge records sub for clientID, merging with an existing hit.
func (subs Subscriptions) merge(clientID string, sub Subscription) {
	if existing, ok := subs[clientID]; ok {
		subs[clientID] = existing.Merge(sub)
		return
	}
	subs[clientID] = sub
}

// mergeIdentifiers unions two identifier slices, returning a sorted
// duplicate-free slice when both are non-empty.
func mergeIdentifiers(a, b []uint32) []uint32 {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := append(slices.Clone(a), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

// splitSharedFilter splits "$share/{group}/{filter}" into its group name and
// inner filter. Regular filters return an empty group and the filter
// unchanged; malformed shared forms are returned unchanged for IsValidFilter
// to reject.
func splitSharedFilter(filter string) (group, inner string) {
	if !strings.HasPrefix(filter, sharedPrefix) {
		return "", filter
	}
	rest := filter[len(sharedPrefix):]
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", filter
	}
	return rest[:i], rest[i+1:]
}
