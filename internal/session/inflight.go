package session

import (
	"container/heap"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// InflightState is the handshake stage of an outbound QoS 1/2 message.
type InflightState byte

const (
	// InflightPublish: PUBLISH sent, awaiting PUBACK (QoS 1) or PUBREC
	// (QoS 2).
	InflightPublish InflightState = iota
	// InflightPubrel: PUBREL sent, awaiting PUBCOMP (QoS 2 only).
	InflightPubrel
)

// InflightMessage is one unacknowledged outbound QoS 1/2 publication.
type InflightMessage struct {
	// Packet is the outbound PUBLISH as sent, with its Packet Identifier.
	Packet packet.Packet
	// Sent is false while the message waits in the queue for a free
	// Receive Maximum window slot.
	Sent bool
	// State tracks the QoS 2 handshake stage.
	State InflightState
	// Expiry is the absolute Message Expiry deadline; zero means no expiry.
	Expiry time.Time
}

// expiryItem is one node of the inflight expiry min-heap.
type expiryItem struct {
	deadline time.Time
	id       uint16
}

// expiryHeap is a min-heap of inflight expiries ordered by deadline
// (architecture decision A9). Entries are removed lazily: acknowledging or
// dropping a message leaves its heap node behind until it reaches the top,
// where purgeStale discards it.
type expiryHeap []expiryItem

func (h expiryHeap) Len() int { return len(h) }

func (h expiryHeap) Less(i, j int) bool {
	if h[i].deadline.Equal(h[j].deadline) {
		return h[i].id < h[j].id
	}
	return h[i].deadline.Before(h[j].deadline)
}

func (h expiryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *expiryHeap) Push(x any) { *h = append(*h, x.(expiryItem)) }

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = expiryItem{}
	*h = old[:n-1]
	return item
}

// messageExpiry computes the absolute expiry of a publish from its Message
// Expiry Interval property, honoring an Expiry already set by the caller.
func messageExpiry(now time.Time, pk *packet.Packet) time.Time {
	if !pk.Expiry.IsZero() {
		return pk.Expiry
	}
	if pk.Properties.MessageExpiryFlag {
		return now.Add(time.Duration(pk.Properties.MessageExpiry) * time.Second)
	}
	return time.Time{}
}

// outboundInflight is the broker -> client window: messages awaiting
// acknowledgement plus a FIFO queue for messages held back by the peer's
// Receive Maximum [MQTT-3.1.2-11].
type outboundInflight struct {
	max      uint16 // peer Receive Maximum
	sent     int
	messages map[uint16]*InflightMessage
	queue    []uint16 // packet identifiers awaiting a free window slot, FIFO
	expiries expiryHeap
}

func newOutboundInflight(max uint16) outboundInflight {
	return outboundInflight{max: max, messages: make(map[uint16]*InflightMessage)}
}

// store registers msg, marking it sent and reporting true when a window
// slot is free; otherwise the message joins the back of the queue.
func (o *outboundInflight) store(msg *InflightMessage) (sendNow bool) {
	id := msg.Packet.PacketID
	o.messages[id] = msg
	if !msg.Expiry.IsZero() {
		heap.Push(&o.expiries, expiryItem{deadline: msg.Expiry, id: id})
	}
	if o.sent < int(o.max) {
		msg.Sent = true
		o.sent++
		return true
	}
	o.queue = append(o.queue, id)
	return false
}

// get returns the message with the given identifier, or nil.
func (o *outboundInflight) get(id uint16) *InflightMessage {
	return o.messages[id]
}

// remove deletes the message with the given identifier, returning it.
func (o *outboundInflight) remove(id uint16) *InflightMessage {
	msg, ok := o.messages[id]
	if !ok {
		return nil
	}
	delete(o.messages, id)
	if msg.Sent {
		o.sent--
	} else {
		o.dequeue(id)
	}
	return msg
}

// dequeue removes a queued (not yet sent) identifier from the FIFO queue.
func (o *outboundInflight) dequeue(id uint16) {
	for i, queued := range o.queue {
		if queued == id {
			o.queue = append(o.queue[:i], o.queue[i+1:]...)
			return
		}
	}
}

// nextImmediate promotes the head of the queue into a freed window slot,
// returning the message to send now, or nil when the queue is empty or the
// window is still full.
func (o *outboundInflight) nextImmediate() *InflightMessage {
	if len(o.queue) == 0 || o.sent >= int(o.max) {
		return nil
	}
	id := o.queue[0]
	o.queue = o.queue[1:]
	msg := o.messages[id]
	msg.Sent = true
	o.sent++
	return msg
}

// purgeStale discards heap nodes whose message vanished or changed.
func (o *outboundInflight) purgeStale() {
	for len(o.expiries) > 0 {
		top := o.expiries[0]
		msg, ok := o.messages[top.id]
		if ok && msg.Expiry.Equal(top.deadline) {
			return
		}
		heap.Pop(&o.expiries)
	}
}

// peekExpired returns the earliest expired inflight message without removing
// it, or nil when nothing has expired.
func (o *outboundInflight) peekExpired(now time.Time) *InflightMessage {
	o.purgeStale()
	if len(o.expiries) == 0 || o.expiries[0].deadline.After(now) {
		return nil
	}
	return o.messages[o.expiries[0].id]
}

// dropExpired removes and returns every expired message in deadline order.
func (o *outboundInflight) dropExpired(now time.Time) []*InflightMessage {
	var out []*InflightMessage
	for {
		msg := o.peekExpired(now)
		if msg == nil {
			return out
		}
		heap.Pop(&o.expiries)
		o.remove(msg.Packet.PacketID)
		out = append(out, msg)
	}
}

// inboundInflight is the client -> broker window: QoS 2 PUBLISH packets
// stored until their PUBREL arrives (strict exactly-once), bounded by the
// Receive Maximum the broker advertised in CONNACK.
type inboundInflight struct {
	max      uint16
	messages map[uint16]*packet.Packet // keyed by Packet Identifier
	expiries expiryHeap
}

func newInboundInflight(max uint16) inboundInflight {
	return inboundInflight{max: max, messages: make(map[uint16]*packet.Packet)}
}

// has reports whether a QoS 2 PUBLISH with the identifier is already stored.
func (i *inboundInflight) has(id uint16) bool {
	_, ok := i.messages[id]
	return ok
}

// store registers an inbound QoS 2 PUBLISH awaiting its PUBREL.
func (i *inboundInflight) store(pk *packet.Packet) {
	cp := *pk
	i.messages[pk.PacketID] = &cp
	if !cp.Expiry.IsZero() {
		heap.Push(&i.expiries, expiryItem{deadline: cp.Expiry, id: cp.PacketID})
	}
}

// take removes and returns the stored PUBLISH, or nil.
func (i *inboundInflight) take(id uint16) *packet.Packet {
	pk, ok := i.messages[id]
	if !ok {
		return nil
	}
	delete(i.messages, id)
	return pk
}

// full reports whether the Receive Maximum window is exhausted; the caller
// disconnects the client with 0x93 (Receive Maximum exceeded) [MQTT-3.3.4-9].
func (i *inboundInflight) full() bool {
	return len(i.messages) >= int(i.max)
}

// purgeStale discards heap nodes whose message vanished or changed.
func (i *inboundInflight) purgeStale() {
	for len(i.expiries) > 0 {
		top := i.expiries[0]
		pk, ok := i.messages[top.id]
		if ok && pk.Expiry.Equal(top.deadline) {
			return
		}
		heap.Pop(&i.expiries)
	}
}

// peekExpired returns the earliest expired pending PUBLISH, or nil.
func (i *inboundInflight) peekExpired(now time.Time) *packet.Packet {
	i.purgeStale()
	if len(i.expiries) == 0 || i.expiries[0].deadline.After(now) {
		return nil
	}
	return i.messages[i.expiries[0].id]
}

// dropExpired removes and returns every expired pending PUBLISH in deadline
// order.
func (i *inboundInflight) dropExpired(now time.Time) []*packet.Packet {
	var out []*packet.Packet
	for {
		pk := i.peekExpired(now)
		if pk == nil {
			return out
		}
		heap.Pop(&i.expiries)
		i.take(pk.PacketID)
		out = append(out, pk)
	}
}
