package topics

import "strings"

// Subscribers returns the merged subscriptions matching a published topic,
// keyed by client ID. Shared groups contribute exactly one selected member
// each. Topics that are empty or contain wildcards [MQTT-4.7.3-1] yield nil.
func (x *Index) Subscribers(topic string) Subscriptions {
	if !IsValidTopicName(topic) {
		return nil
	}
	out := Subscriptions{}
	// Wildcards at the top level MUST NOT match topics beginning with '$'
	// [MQTT-4.7.2-1].
	x.root.match(strings.Split(topic, "/"), out, x.strategy, !strings.HasPrefix(topic, "$"))
	if len(out) == 0 {
		return nil
	}
	return out
}

// match walks the trie depth-first: each level descends into the exact-key
// and '+' branches, and '#' children are collected at every visited node
// because they match the remaining levels and the parent level itself
// [MQTT-4.7.1-2]. wildcards gates both branches and is false only at the
// root for '$' topics.
func (p *particle) match(parts []string, out Subscriptions, s SelectStrategy, wildcards bool) {
	p.mu.RLock()
	if len(parts) == 0 {
		for id, sub := range p.subs {
			out.merge(id, sub)
		}
		for _, g := range p.shared {
			id, sub := g.selectMember(s)
			out.merge(id, sub)
		}
	}
	var hash, plus, exact *particle
	if wildcards {
		hash = p.children["#"]
		if len(parts) > 0 {
			plus = p.children["+"]
		}
	}
	if len(parts) > 0 {
		exact = p.children[parts[0]]
	}
	p.mu.RUnlock()

	if hash != nil {
		hash.match(nil, out, s, true)
	}
	if exact != nil {
		exact.match(parts[1:], out, s, true)
	}
	if plus != nil {
		plus.match(parts[1:], out, s, true)
	}
}
