package session

import (
	"container/heap"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// This file carries the additive API used by broker persistence
// (docs/design/persistence.md): rebuilding an offline session from the store
// at startup, queueing messages for disconnected persistent sessions and
// promoting them on resume.

// NewRestoredClient rebuilds an offline session from its persisted record.
// The placeholder carries no connection; its durable state is filled through
// AddSubscription/RestoreOutbound/RestoreInbound and migrates into the new
// connection's Client via MigrateFrom when the client reconnects.
func NewRestoredClient(id string, expiry uint32, createdAt, stoppedAt time.Time, cfg Config) *Client {
	inboundMax := cfg.ReceiveMaximum
	if inboundMax == 0 {
		inboundMax = DefaultReceiveMaximum
	}
	return &Client{
		ID:              id,
		ProtocolVersion: packet.Version5,
		SessionExpiry:   expiry,
		CreatedAt:       createdAt,
		subscriptions:   make(map[string]topics.Subscription),
		inboundAliases:  make(map[uint16]string),
		outboundAliases: make(map[string]uint16),
		inboundAliasMax: cfg.TopicAliasMaximum,
		outbound:        newOutboundInflight(DefaultReceiveMaximum),
		inbound:         newInboundInflight(inboundMax),
		stoppedAt:       stoppedAt,
	}
}

// RestoreOutbound registers a persisted outbound inflight message, reserving
// its Packet Identifier. Sent messages are retransmitted with DUP=1 by
// ResendInflight on resume; queued ones are promoted by PromoteQueued.
func (c *Client) RestoreOutbound(msg InflightMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := msg.Packet.PacketID
	if id == 0 {
		return
	}
	c.ids.reserve(id)
	cp := msg
	c.outbound.messages[id] = &cp
	if !cp.Expiry.IsZero() {
		heap.Push(&c.outbound.expiries, expiryItem{deadline: cp.Expiry, id: id})
	}
	if cp.Sent {
		c.outbound.sent++
	} else {
		c.outbound.queue = append(c.outbound.queue, id)
	}
}

// RestoreInbound registers a persisted inbound QoS 2 PUBLISH still awaiting
// its PUBREL (strict exactly-once resumes mid-handshake).
func (c *Client) RestoreInbound(pk *packet.Packet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inbound.store(pk)
}

// QueueOffline registers an outbound QoS 1/2 PUBLISH for a disconnected
// persistent session: it occupies the inflight window and joins the back of
// the queue without being sent. On resume PromoteQueued transmits it, so the
// first delivery correctly carries DUP=0. The stored message is returned for
// persistence.
func (c *Client) QueueOffline(now time.Time, pk *packet.Packet) (*InflightMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pk.FixedHeader.Qos == 0 {
		return nil, packet.Code{Code: packet.CodeProtocolError,
			Reason: "QoS 0 messages are not queued for offline sessions"}
	}
	id, ok := c.ids.allocate()
	if !ok {
		return nil, packet.Code{Code: packet.CodePacketIdentifierInUse,
			Reason: "all 65535 packet identifiers are in use"}
	}
	msg := &InflightMessage{
		Packet: *pk,
		State:  InflightPublish,
		Expiry: messageExpiry(now, pk),
	}
	msg.Packet.PacketID = id
	msg.Packet.FixedHeader.Dup = false
	msg.Packet.Expiry = msg.Expiry
	c.outbound.messages[id] = msg
	if !msg.Expiry.IsZero() {
		heap.Push(&c.outbound.expiries, expiryItem{deadline: msg.Expiry, id: id})
	}
	c.outbound.queue = append(c.outbound.queue, id)
	cp := *msg
	return &cp, nil
}

// PromoteQueued fills free Receive Maximum window slots from the queue and
// returns the packets to send now, in FIFO order. The broker calls it after
// ResendInflight on session resume so messages queued while the client was
// offline are delivered.
func (c *Client) PromoteQueued() []packet.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []packet.Packet
	for {
		next := c.outbound.nextImmediate()
		if next == nil {
			return out
		}
		out = append(out, next.Packet)
	}
}

// OutboundMessage returns a copy of the stored outbound inflight message,
// for persisting handshake state transitions (e.g. PUBREC advancing a QoS 2
// message to the PUBREL stage).
func (c *Client) OutboundMessage(packetID uint16) (InflightMessage, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	msg := c.outbound.get(packetID)
	if msg == nil {
		return InflightMessage{}, false
	}
	return *msg, true
}
