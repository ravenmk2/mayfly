package topics

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// Retain stores or removes the retained message for a topic. An empty
// payload removes the retained message [MQTT-3.3.1-6]. Trie nodes only
// record the topic name; payloads live in a dedicated map.
func (x *Index) Retain(pk packet.Packet) error {
	if !IsValidTopicName(pk.TopicName) {
		return fmt.Errorf("%w: %q", ErrInvalidTopicName, pk.TopicName)
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(pk.Payload) == 0 {
		x.retainedMu.Lock()
		delete(x.retained, pk.TopicName)
		x.retainedMu.Unlock()
		if leaf := x.root.find(strings.Split(pk.TopicName, "/")); leaf != nil {
			leaf.mu.Lock()
			leaf.retainPath = ""
			leaf.mu.Unlock()
			trim(leaf, x.root)
		}
		return nil
	}
	pk.FixedHeader.Retain = true
	pk.Payload = bytes.Clone(pk.Payload)
	x.retainedMu.Lock()
	x.retained[pk.TopicName] = pk
	x.retainedMu.Unlock()
	leaf := x.root.leaf(strings.Split(pk.TopicName, "/"))
	leaf.mu.Lock()
	leaf.retainPath = pk.TopicName
	leaf.mu.Unlock()
	return nil
}

// RetainedCount returns the number of stored retained messages.
func (x *Index) RetainedCount() int {
	x.retainedMu.RLock()
	defer x.retainedMu.RUnlock()
	return len(x.retained)
}

// RetainedMessages returns every stored retained message sorted by topic
// name. It serves the management API's retained listing.
func (x *Index) RetainedMessages() []packet.Packet {
	x.retainedMu.RLock()
	out := make([]packet.Packet, 0, len(x.retained))
	for _, pk := range x.retained {
		out = append(out, pk)
	}
	x.retainedMu.RUnlock()
	slices.SortFunc(out, func(a, b packet.Packet) int {
		return strings.Compare(a.TopicName, b.TopicName)
	})
	return out
}

// Messages returns the retained messages matching the subscription's filter
// for delivery at subscribe time, honouring Retain Handling [MQTT-3.8.3-1]:
// 0 always delivers, 1 delivers only when the subscription is new
// (existed == false), 2 never delivers. Shared subscriptions never receive
// retained messages. When RetainAsPublished is unset the RETAIN flag is
// forced to 1 as required for messages sent on a new subscription
// [MQTT-3.3.1-9]. Results are sorted by topic name for stable delivery.
func (x *Index) Messages(sub Subscription, existed bool) []packet.Packet {
	group, inner := splitSharedFilter(sub.Filter)
	if group != "" || !IsValidFilter(sub.Filter) {
		return nil
	}
	switch sub.RetainHandling {
	case 2:
		return nil
	case 1:
		if existed {
			return nil
		}
	}
	var paths []string
	x.root.collectRetained(strings.Split(inner, "/"), &paths, !strings.HasPrefix(inner, "$"))
	if len(paths) == 0 {
		return nil
	}
	x.retainedMu.RLock()
	out := make([]packet.Packet, 0, len(paths))
	for _, path := range paths {
		pk, ok := x.retained[path]
		if !ok {
			continue
		}
		if !sub.RetainAsPublished {
			pk.FixedHeader.Retain = true // [MQTT-3.3.1-9]
		}
		out = append(out, pk)
	}
	x.retainedMu.RUnlock()
	slices.SortFunc(out, func(a, b packet.Packet) int {
		return strings.Compare(a.TopicName, b.TopicName)
	})
	return out
}

// collectRetained walks the trie driven by the filter parts, appending the
// retainPath of every matching node. Unlike match, the trie here holds topic
// levels, so '+' selects every child and a trailing '#' collects the whole
// subtree including the current node [MQTT-4.7.1-2]. wildcards gates both
// wildcards and is false only at the root for '$' filters [MQTT-4.7.2-1].
func (p *particle) collectRetained(parts []string, out *[]string, wildcards bool) {
	if len(parts) > 0 && parts[0] == "#" {
		if wildcards {
			p.collectAllRetained(out, p.parent == nil)
		}
		return
	}
	p.mu.RLock()
	if len(parts) == 0 && p.retainPath != "" {
		*out = append(*out, p.retainPath)
	}
	var next []*particle
	if len(parts) > 0 {
		if parts[0] == "+" {
			if wildcards {
				next = make([]*particle, 0, len(p.children))
				for _, c := range p.children {
					next = append(next, c)
				}
			}
		} else if c := p.children[parts[0]]; c != nil {
			next = []*particle{c}
		}
	}
	p.mu.RUnlock()
	for _, c := range next {
		c.collectRetained(parts[1:], out, true)
	}
}

// collectAllRetained appends the retainPath of every node in the subtree
// rooted at p (the '#' terminal case). skipDollar excludes children whose
// key begins with '$' so a bare '#' filter never matches '$' topics
// [MQTT-4.7.2-1].
func (p *particle) collectAllRetained(out *[]string, skipDollar bool) {
	p.mu.RLock()
	if p.retainPath != "" {
		*out = append(*out, p.retainPath)
	}
	children := make([]*particle, 0, len(p.children))
	for key, c := range p.children {
		if skipDollar && strings.HasPrefix(key, "$") {
			continue
		}
		children = append(children, c)
	}
	p.mu.RUnlock()
	for _, c := range children {
		c.collectAllRetained(out, false)
	}
}
