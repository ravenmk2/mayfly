package store

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"sync"
)

// MemoryStore is the in-memory Store implementation used by tests and by
// brokers running without a configured database. Puts replace existing
// entries; deletes are idempotent; iteration is ordered by key for
// determinism.
type MemoryStore struct {
	mu       sync.RWMutex
	sessions map[string]Session
	subs     map[subKey]Subscription
	inflight map[inflightKey]InflightMessage
	retained map[string]RetainedMessage
}

type subKey struct {
	clientID string
	filter   string
}

type inflightKey struct {
	clientID  string
	direction Direction
	packetID  uint16
}

// NewMemory returns an empty in-memory store.
func NewMemory() *MemoryStore {
	return &MemoryStore{
		sessions: make(map[string]Session),
		subs:     make(map[subKey]Subscription),
		inflight: make(map[inflightKey]InflightMessage),
		retained: make(map[string]RetainedMessage),
	}
}

func (m *MemoryStore) Sessions() SessionStore           { return memorySessions{m} }
func (m *MemoryStore) Subscriptions() SubscriptionStore { return memorySubs{m} }
func (m *MemoryStore) Inflight() InflightStore          { return memoryInflight{m} }
func (m *MemoryStore) Retained() RetainedStore          { return memoryRetained{m} }
func (m *MemoryStore) Close() error                     { return nil }

type memorySessions struct{ m *MemoryStore }

func (ms memorySessions) Put(_ context.Context, s Session) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	ms.m.sessions[s.ClientID] = s
	return nil
}

func (ms memorySessions) Delete(_ context.Context, clientID string) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	delete(ms.m.sessions, clientID)
	return nil
}

func (ms memorySessions) Get(_ context.Context, clientID string) (Session, bool, error) {
	ms.m.mu.RLock()
	defer ms.m.mu.RUnlock()
	s, ok := ms.m.sessions[clientID]
	return s, ok, nil
}

func (ms memorySessions) Iterate(_ context.Context, fn func(Session) bool) error {
	ms.m.mu.RLock()
	out := make([]Session, 0, len(ms.m.sessions))
	for _, s := range ms.m.sessions {
		out = append(out, s)
	}
	ms.m.mu.RUnlock()
	slices.SortFunc(out, func(a, b Session) int { return cmp.Compare(a.ClientID, b.ClientID) })
	for _, s := range out {
		if !fn(s) {
			return nil
		}
	}
	return nil
}

type memorySubs struct{ m *MemoryStore }

func (ms memorySubs) Put(_ context.Context, sub Subscription) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	ms.m.subs[subKey{sub.ClientID, sub.Filter}] = sub
	return nil
}

func (ms memorySubs) Delete(_ context.Context, clientID, filter string) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	delete(ms.m.subs, subKey{clientID, filter})
	return nil
}

func (ms memorySubs) DeleteByClient(_ context.Context, clientID string) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	for k := range ms.m.subs {
		if k.clientID == clientID {
			delete(ms.m.subs, k)
		}
	}
	return nil
}

func (ms memorySubs) Iterate(_ context.Context, fn func(Subscription) bool) error {
	ms.m.mu.RLock()
	out := make([]Subscription, 0, len(ms.m.subs))
	for _, s := range ms.m.subs {
		out = append(out, s)
	}
	ms.m.mu.RUnlock()
	slices.SortFunc(out, func(a, b Subscription) int {
		if c := strings.Compare(a.ClientID, b.ClientID); c != 0 {
			return c
		}
		return strings.Compare(a.Filter, b.Filter)
	})
	for _, s := range out {
		if !fn(s) {
			return nil
		}
	}
	return nil
}

type memoryInflight struct{ m *MemoryStore }

func (ms memoryInflight) Put(_ context.Context, msg InflightMessage) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	ms.m.inflight[inflightKey{msg.ClientID, msg.Direction, msg.PacketID}] = msg
	return nil
}

func (ms memoryInflight) Delete(_ context.Context, clientID string, direction Direction, packetID uint16) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	delete(ms.m.inflight, inflightKey{clientID, direction, packetID})
	return nil
}

func (ms memoryInflight) DeleteByClient(_ context.Context, clientID string) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	for k := range ms.m.inflight {
		if k.clientID == clientID {
			delete(ms.m.inflight, k)
		}
	}
	return nil
}

func (ms memoryInflight) Iterate(_ context.Context, fn func(InflightMessage) bool) error {
	ms.m.mu.RLock()
	out := make([]InflightMessage, 0, len(ms.m.inflight))
	for _, msg := range ms.m.inflight {
		out = append(out, msg)
	}
	ms.m.mu.RUnlock()
	slices.SortFunc(out, func(a, b InflightMessage) int {
		if c := strings.Compare(a.ClientID, b.ClientID); c != 0 {
			return c
		}
		if a.Direction != b.Direction {
			return cmp.Compare(a.Direction, b.Direction)
		}
		return cmp.Compare(a.PacketID, b.PacketID)
	})
	for _, msg := range out {
		if !fn(msg) {
			return nil
		}
	}
	return nil
}

type memoryRetained struct{ m *MemoryStore }

func (ms memoryRetained) Put(_ context.Context, msg RetainedMessage) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	ms.m.retained[msg.Topic] = msg
	return nil
}

func (ms memoryRetained) Delete(_ context.Context, topic string) error {
	ms.m.mu.Lock()
	defer ms.m.mu.Unlock()
	delete(ms.m.retained, topic)
	return nil
}

func (ms memoryRetained) Iterate(_ context.Context, fn func(RetainedMessage) bool) error {
	ms.m.mu.RLock()
	out := make([]RetainedMessage, 0, len(ms.m.retained))
	for _, msg := range ms.m.retained {
		out = append(out, msg)
	}
	ms.m.mu.RUnlock()
	slices.SortFunc(out, func(a, b RetainedMessage) int { return strings.Compare(a.Topic, b.Topic) })
	for _, msg := range out {
		if !fn(msg) {
			return nil
		}
	}
	return nil
}
