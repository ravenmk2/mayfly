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

// applyOps applies a batch in one critical section (batchStore).
func (m *MemoryStore) applyOps(_ context.Context, ops []op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range ops {
		m.applyLocked(o)
	}
	return nil
}

func (m *MemoryStore) applyLocked(o op) {
	switch o.kind {
	case opPutSession:
		m.sessions[o.session.ClientID] = o.session
	case opDeleteSession:
		delete(m.sessions, o.session.ClientID)
	case opPutSubscription:
		m.subs[subKey{o.sub.ClientID, o.sub.Filter}] = o.sub
	case opDeleteSubscription:
		delete(m.subs, subKey{o.sub.ClientID, o.sub.Filter})
	case opDeleteSubscriptionsByClient:
		for k := range m.subs {
			if k.clientID == o.sub.ClientID {
				delete(m.subs, k)
			}
		}
	case opPutInflight:
		m.inflight[inflightKey{o.inflight.ClientID, o.inflight.Direction, o.inflight.PacketID}] = o.inflight
	case opDeleteInflight:
		delete(m.inflight, inflightKey{o.inflight.ClientID, o.inflight.Direction, o.inflight.PacketID})
	case opDeleteInflightByClient:
		for k := range m.inflight {
			if k.clientID == o.inflight.ClientID {
				delete(m.inflight, k)
			}
		}
	case opPutRetained:
		m.retained[o.retained.Topic] = o.retained
	case opDeleteRetained:
		delete(m.retained, o.retained.Topic)
	}
}

type memorySessions struct{ m *MemoryStore }

func (ms memorySessions) Put(ctx context.Context, s Session) error {
	return ms.m.applyOps(ctx, []op{{kind: opPutSession, session: s}})
}

func (ms memorySessions) Delete(ctx context.Context, clientID string) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteSession, session: Session{ClientID: clientID}}})
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

func (ms memorySubs) Put(ctx context.Context, sub Subscription) error {
	return ms.m.applyOps(ctx, []op{{kind: opPutSubscription, sub: sub}})
}

func (ms memorySubs) Delete(ctx context.Context, clientID, filter string) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteSubscription, sub: Subscription{ClientID: clientID, Filter: filter}}})
}

func (ms memorySubs) DeleteByClient(ctx context.Context, clientID string) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteSubscriptionsByClient, sub: Subscription{ClientID: clientID}}})
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

func (ms memoryInflight) Put(ctx context.Context, msg InflightMessage) error {
	return ms.m.applyOps(ctx, []op{{kind: opPutInflight, inflight: msg}})
}

func (ms memoryInflight) Delete(ctx context.Context, clientID string, direction Direction, packetID uint16) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteInflight,
		inflight: InflightMessage{ClientID: clientID, Direction: direction, PacketID: packetID}}})
}

func (ms memoryInflight) DeleteByClient(ctx context.Context, clientID string) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteInflightByClient, inflight: InflightMessage{ClientID: clientID}}})
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

func (ms memoryRetained) Put(ctx context.Context, msg RetainedMessage) error {
	return ms.m.applyOps(ctx, []op{{kind: opPutRetained, retained: msg}})
}

func (ms memoryRetained) Delete(ctx context.Context, topic string) error {
	return ms.m.applyOps(ctx, []op{{kind: opDeleteRetained, retained: RetainedMessage{Topic: topic}}})
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
