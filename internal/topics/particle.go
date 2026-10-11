package topics

import "sync"

// particle is a node of the subscription trie. Each particle owns an RWMutex
// guarding its children, subs and shared maps, so the read path descends one
// level at a time without a global lock while writers additionally hold the
// global write lock of Index (architecture decision A3).
type particle struct {
	mu       sync.RWMutex
	key      string
	parent   *particle
	children map[string]*particle
	subs     map[string]Subscription // clientID → subscription
	shared   map[string]*sharedGroup // group name → members
	// retainPath is the full topic name when a retained message exists for
	// this node, empty otherwise. Payloads live in the Index retained map.
	retainPath string
}

func newParticle(key string, parent *particle) *particle {
	return &particle{
		key:      key,
		parent:   parent,
		children: make(map[string]*particle),
		subs:     make(map[string]Subscription),
		shared:   make(map[string]*sharedGroup),
	}
}

// child returns the child for key, creating it when create is set.
func (p *particle) child(key string, create bool) *particle {
	if create {
		p.mu.Lock()
		defer p.mu.Unlock()
		c, ok := p.children[key]
		if !ok {
			c = newParticle(key, p)
			p.children[key] = c
		}
		return c
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.children[key]
}

// leaf descends the trie creating particles as needed and returns the node
// for the last part.
func (p *particle) leaf(parts []string) *particle {
	for _, part := range parts {
		p = p.child(part, true)
	}
	return p
}

// find descends the trie without creating nodes and returns nil when the
// path is absent.
func (p *particle) find(parts []string) *particle {
	for _, part := range parts {
		p = p.child(part, false)
		if p == nil {
			return nil
		}
	}
	return p
}

// trim removes p and its ancestors while they carry no subscriptions, shared
// groups, children or retained reference. Callers MUST hold the global write
// lock: pruning empty nodes only there keeps the lock-free read path free of
// read/delete races.
func trim(p, root *particle) {
	for p != root {
		p.mu.RLock()
		empty := len(p.children) == 0 && len(p.subs) == 0 && len(p.shared) == 0 && p.retainPath == ""
		p.mu.RUnlock()
		if !empty {
			return
		}
		parent := p.parent
		parent.mu.Lock()
		delete(parent.children, p.key)
		parent.mu.Unlock()
		p = parent
	}
}
