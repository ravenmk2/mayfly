package session

import (
	"container/heap"
	"sync"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// ScheduledWill is a Will Message waiting for its Will Delay Interval to
// elapse. The broker publishes Message when Due returns it.
type ScheduledWill struct {
	ClientID string
	Message  packet.Packet // the Will as an outbound PUBLISH
	At       time.Time     // fire time from WillPublishTime
	// cancelled is set by Cancel; the entry is skipped lazily when popped.
	cancelled bool
}

// willHeap is a min-heap of scheduled wills ordered by fire time
// (architecture decision A9).
type willHeap []*ScheduledWill

func (h willHeap) Len() int           { return len(h) }
func (h willHeap) Less(i, j int) bool { return h[i].At.Before(h[j].At) }
func (h willHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *willHeap) Push(x any)        { *h = append(*h, x.(*ScheduledWill)) }
func (h *willHeap) Pop() any {
	old := *h
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return w
}

// WillScheduler tracks delayed Will Messages on a min-heap. It only decides
// when a Will fires; publishing is the caller's job. The zero value is ready
// to use.
type WillScheduler struct {
	mu   sync.Mutex
	heap willHeap
	byID map[string]*ScheduledWill
}

// Schedule registers w, replacing any pending Will for the same client ID.
func (s *WillScheduler) Schedule(w ScheduledWill) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byID == nil {
		s.byID = make(map[string]*ScheduledWill)
	}
	if old, ok := s.byID[w.ClientID]; ok {
		old.cancelled = true
	}
	entry := &w
	s.byID[w.ClientID] = entry
	heap.Push(&s.heap, entry)
}

// Cancel drops a pending Will — the client reconnected and resumed its
// session before the delay elapsed, or the session expired cleanly. It
// reports whether a Will was pending.
func (s *WillScheduler) Cancel(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.byID[clientID]
	if !ok {
		return false
	}
	w.cancelled = true
	delete(s.byID, clientID)
	return true
}

// Due removes and returns every Will whose fire time has passed, in fire
// time order. Cancelled entries are purged silently.
func (s *WillScheduler) Due(now time.Time) []ScheduledWill {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ScheduledWill
	for len(s.heap) > 0 && !s.heap[0].At.After(now) {
		w := heap.Pop(&s.heap).(*ScheduledWill)
		if w.cancelled {
			continue
		}
		delete(s.byID, w.ClientID)
		out = append(out, *w)
	}
	return out
}

// Len reports the number of pending (not cancelled) Wills.
func (s *WillScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}
