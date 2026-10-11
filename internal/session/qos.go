package session

import (
	"cmp"
	"slices"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// DowngradeQoS computes the delivery QoS of a publication for a subscriber:
// the minimum of the publication and subscription QoS [MQTT-3.8.4-3].
func DowngradeQoS(publishQoS, subscriptionQoS byte) byte {
	return min(publishQoS, subscriptionQoS)
}

// ackPacket builds a PUBACK/PUBREC/PUBREL/PUBCOMP response.
func ackPacket(typ byte, id uint16, version byte, reason byte) packet.Packet {
	return packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: typ},
		ProtocolVersion: version,
		PacketID:        id,
		ReasonCode:      reason,
	}
}

// notFoundReason maps an unknown Packet Identifier to the v5 reason code
// 0x92 (Packet Identifier not found, spec 4.3.3); MQTT 3.1.1 acknowledgements
// carry no reason code.
func (c *Client) notFoundReason() byte {
	if c.ProtocolVersion == packet.Version5 {
		return packet.CodePacketIdentifierNotFound
	}
	return packet.CodeSuccess
}

// ReceivePublish processes an inbound PUBLISH through the QoS state machine.
// It returns the message to hand to the delivery engine (nil while a QoS 2
// handshake is still pending) and the acknowledgement to write back to the
// client, if any.
func (c *Client) ReceivePublish(now time.Time, pk *packet.Packet) (deliver *packet.Packet, ack *packet.Packet, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch pk.FixedHeader.Qos {
	case 0:
		// QoS 0: no acknowledgement, straight to the delivery engine.
		return pk, nil, nil
	case 1:
		// QoS 1: deliver, then acknowledge [MQTT-4.3.2-1]. The caller may
		// rewrite the v5 reason code (e.g. 0x10 No matching subscribers).
		response := ackPacket(packet.Puback, pk.PacketID, c.ProtocolVersion, packet.CodeSuccess)
		return pk, &response, nil
	case 2:
		return c.receivePublishQoS2(now, pk)
	default:
		return nil, nil, packet.Code{Code: packet.CodeProtocolError,
			Reason: "PUBLISH with reserved QoS 3"}
	}
}

// receivePublishQoS2 implements strict exactly-once inbound delivery: a
// PUBLISH is only stored and acknowledged with PUBREC; the message reaches
// the delivery engine when its PUBREL arrives [MQTT-4.3.3-1].
func (c *Client) receivePublishQoS2(now time.Time, pk *packet.Packet) (*packet.Packet, *packet.Packet, error) {
	if c.inbound.has(pk.PacketID) {
		// A duplicate PUBLISH re-sends PUBREC without re-delivering
		// [MQTT-4.3.3-1].
		response := ackPacket(packet.Pubrec, pk.PacketID, c.ProtocolVersion, packet.CodeSuccess)
		return nil, &response, nil
	}
	if c.inbound.full() {
		// The client exceeded the Receive Maximum the broker advertised in
		// CONNACK; the caller disconnects with 0x93 [MQTT-3.3.4-9].
		return nil, nil, packet.Code{Code: packet.CodeReceiveMaximumExceeded,
			Reason: "inflight window is full"}
	}
	pk.Expiry = messageExpiry(now, pk)
	c.inbound.store(pk)
	response := ackPacket(packet.Pubrec, pk.PacketID, c.ProtocolVersion, packet.CodeSuccess)
	return nil, &response, nil
}

// ReceivePubrel completes an inbound QoS 2 handshake: the stored PUBLISH
// becomes deliverable and a PUBCOMP is produced. A repeated or unknown
// PUBREL re-sends PUBCOMP (v5 with 0x92) without re-delivering [MQTT-4.3.3-2].
func (c *Client) ReceivePubrel(packetID uint16) (deliver *packet.Packet, ack packet.Packet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	deliver = c.inbound.take(packetID)
	reason := packet.CodeSuccess
	if deliver == nil {
		reason = c.notFoundReason()
	}
	return deliver, ackPacket(packet.Pubcomp, packetID, c.ProtocolVersion, reason)
}

// SendPublish registers an outbound PUBLISH in the inflight window and
// returns the packet to write now, or nil when the Receive Maximum window is
// full and the message was queued. QoS 0 messages bypass inflight entirely.
// The returned packet is a copy; the inflight entry holds its own.
func (c *Client) SendPublish(now time.Time, pk *packet.Packet) (*packet.Packet, error) {
	send, _, err := c.EnqueuePublish(now, pk)
	return send, err
}

// EnqueuePublish is SendPublish plus a copy of the stored inflight entry —
// including when the message was queued behind the window — so callers can
// persist it (docs/design/persistence.md). stored is nil for QoS 0.
func (c *Client) EnqueuePublish(now time.Time, pk *packet.Packet) (send *packet.Packet, stored *InflightMessage, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pk.FixedHeader.Qos == 0 {
		out := *pk
		return &out, nil, nil
	}
	id, ok := c.ids.allocate()
	if !ok {
		return nil, nil, packet.Code{Code: packet.CodePacketIdentifierInUse,
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
	cp := *msg
	if c.outbound.store(msg) {
		out := msg.Packet
		return &out, &cp, nil
	}
	return nil, &cp, nil
}

// promote moves the head of the queue into a freed window slot, returning
// the packet to send now.
func (c *Client) promote() *packet.Packet {
	if next := c.outbound.nextImmediate(); next != nil {
		pk := next.Packet
		return &pk
	}
	return nil
}

// ReceivePuback completes an outbound QoS 1 message and returns the next
// queued message promoted into the freed window slot, or nil.
func (c *Client) ReceivePuback(packetID uint16) *packet.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outbound.remove(packetID) == nil {
		return nil
	}
	c.ids.free(packetID)
	return c.promote()
}

// ReceivePubrec advances an outbound QoS 2 message to the PUBREL stage and
// returns the PUBREL to send. A duplicate PUBREC re-sends PUBREL with DUP
// set; an unknown identifier still completes the peer's handshake (v5 with
// 0x92) [MQTT-4.3.3-2].
func (c *Client) ReceivePubrec(packetID uint16) (packet.Packet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	msg := c.outbound.get(packetID)
	if msg == nil {
		return ackPacket(packet.Pubrel, packetID, c.ProtocolVersion, c.notFoundReason()), nil
	}
	if msg.Packet.FixedHeader.Qos != 2 {
		return packet.Packet{}, packet.Code{Code: packet.CodeProtocolError,
			Reason: "PUBREC received for a non-QoS 2 message"}
	}
	pubrel := ackPacket(packet.Pubrel, packetID, c.ProtocolVersion, packet.CodeSuccess)
	if msg.State == InflightPubrel {
		// Duplicate PUBREC: retransmit PUBREL with DUP=1.
		pubrel.FixedHeader.Dup = true
		return pubrel, nil
	}
	msg.State = InflightPubrel
	return pubrel, nil
}

// ReceivePubcomp completes an outbound QoS 2 message and returns the next
// queued message promoted into the freed window slot, or nil.
func (c *Client) ReceivePubcomp(packetID uint16) *packet.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outbound.remove(packetID) == nil {
		return nil
	}
	c.ids.free(packetID)
	return c.promote()
}

// ResendInflight returns the retransmission snapshot for a resumed session:
// every sent-but-unacknowledged message with DUP=1, PUBREL-stage messages as
// PUBREL, ordered by Packet Identifier [MQTT-4.4.0-1].
func (c *Client) ResendInflight() []packet.Packet {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]packet.Packet, 0, len(c.outbound.messages))
	for _, msg := range c.outbound.messages {
		if !msg.Sent {
			continue
		}
		switch msg.State {
		case InflightPublish:
			pk := msg.Packet
			pk.FixedHeader.Dup = true
			out = append(out, pk)
		case InflightPubrel:
			pk := ackPacket(packet.Pubrel, msg.Packet.PacketID, c.ProtocolVersion, packet.CodeSuccess)
			pk.FixedHeader.Dup = true
			out = append(out, pk)
		}
	}
	slices.SortFunc(out, func(a, b packet.Packet) int {
		return cmp.Compare(a.PacketID, b.PacketID)
	})
	return out
}

// PeekExpiredOutbound returns the earliest expired outbound inflight message
// without removing it, or nil.
func (c *Client) PeekExpiredOutbound(now time.Time) *InflightMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.outbound.peekExpired(now)
}

// DropExpiredOutbound removes every expired outbound inflight message,
// freeing its Packet Identifier, and returns them in deadline order.
func (c *Client) DropExpiredOutbound(now time.Time) []*InflightMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	msgs := c.outbound.dropExpired(now)
	for _, msg := range msgs {
		c.ids.free(msg.Packet.PacketID)
	}
	return msgs
}

// DropExpiredInbound removes every expired inbound QoS 2 PUBLISH awaiting
// PUBREL and returns them in deadline order.
func (c *Client) DropExpiredInbound(now time.Time) []*packet.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inbound.dropExpired(now)
}

// OutboundLen reports the number of unacknowledged outbound QoS 1/2
// messages, queued ones included.
func (c *Client) OutboundLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.outbound.messages)
}

// OutboundQueued reports how many outbound messages wait behind the Receive
// Maximum window.
func (c *Client) OutboundQueued() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.outbound.queue)
}

// InboundLen reports the number of inbound QoS 2 messages awaiting PUBREL.
func (c *Client) InboundLen() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.inbound.messages)
}
