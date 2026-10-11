package broker

import (
	"context"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// publishFrom handles an inbound PUBLISH from the read loop: topic alias
// resolution, authorization, the QoS handshake and, when the message becomes
// deliverable, routing. A returned error is fatal to the connection.
func (s *Server) publishFrom(c *conn, pk *packet.Packet) error {
	client := c.sessionClient()
	now := s.now()

	// Resolve the v5 Topic Alias before anything else needs the topic name
	// [MQTT-3.3.2-9].
	topic, err := client.ResolveInboundAlias(pk)
	if err != nil {
		return err
	}
	pk.TopicName = topic

	// Publish authorization is checked once at ingress; delivery does not
	// re-check per subscriber (architecture decision A5).
	if !s.authz.Allow(context.Background(), c.clientInfo(), hook.OpPublish, topic) {
		s.Stats.MessagesDropped.Add(1)
		s.log.Debug("publish denied", "client", client.ID, "topic", topic)
		// QoS 1/2 handshakes still complete so the client can proceed.
		c.ackDeniedPublish(pk)
		return nil
	}

	// Compute the Message Expiry deadline once so QoS 0/1 messages, which
	// bypass the inbound inflight store, expire consistently.
	if pk.Expiry.IsZero() && pk.Properties.MessageExpiryFlag {
		pk.Expiry = now.Add(time.Duration(pk.Properties.MessageExpiry) * time.Second)
	}

	deliver, ack, err := client.ReceivePublish(now, pk)
	if err != nil {
		// The Receive Maximum window was exceeded [MQTT-3.3.4-9].
		return err
	}
	if pk.FixedHeader.Qos == 2 && ack != nil && ack.FixedHeader.Type == packet.Pubrec {
		// A stored (or duplicate) inbound QoS 2 PUBLISH awaiting its PUBREL.
		s.storePutInbound(client.ID, pk)
	}

	matched := false
	if deliver != nil {
		matched = s.route(c, deliver, now)
	}

	if ack != nil {
		// Report "No matching subscribers" on the QoS 1 acknowledgement
		// when nothing was delivered [MQTT-3.3.4-10].
		if deliver != nil && !matched && ack.FixedHeader.Type == packet.Puback &&
			client.ProtocolVersion == packet.Version5 {
			ack.ReasonCode = packet.CodeNoMatchingSubscribers
		}
		c.enqueue(ack)
	}
	return nil
}

// releaseQoS2 routes an inbound QoS 2 message whose PUBREL completed the
// handshake (strict exactly-once, [MQTT-4.3.3-1]). The alias, authorization
// and expiry were handled when the PUBLISH arrived.
func (s *Server) releaseQoS2(c *conn, pk *packet.Packet) {
	s.route(c, pk, s.now())
}

// route stores a deliverable message when retained and fans it out,
// reporting whether any subscription matched.
func (s *Server) route(sender *conn, pk *packet.Packet, now time.Time) (matched bool) {
	s.Stats.MessagesReceived.Add(1)
	if pk.FixedHeader.Retain {
		s.retain(*pk)
	}
	return s.deliver(sender, pk, now)
}

// ackDeniedPublish completes the QoS handshake of an unauthorized PUBLISH
// with a failure reason (v5) so the client is not stuck waiting.
func (c *conn) ackDeniedPublish(pk *packet.Packet) {
	version := c.protocolVersion()
	switch pk.FixedHeader.Qos {
	case 1:
		reason := packet.CodeSuccess
		if version == packet.Version5 {
			reason = packet.CodeNotAuthorized
		}
		c.enqueue(&packet.Packet{
			FixedHeader:     packet.FixedHeader{Type: packet.Puback},
			ProtocolVersion: version,
			PacketID:        pk.PacketID,
			ReasonCode:      reason,
		})
	case 2:
		reason := packet.CodeSuccess
		if version == packet.Version5 {
			reason = packet.CodeNotAuthorized
		}
		c.enqueue(&packet.Packet{
			FixedHeader:     packet.FixedHeader{Type: packet.Pubrec},
			ProtocolVersion: version,
			PacketID:        pk.PacketID,
			ReasonCode:      reason,
		})
	}
}

// deliver fans a publication out to the matching subscribers and reports
// whether any subscription matched. NoLocal filters out the publisher's own
// subscriptions [MQTT-3.8.3-3]; offline members of shared groups are skipped
// (the message has no offline queue until the Store milestone).
func (s *Server) deliver(sender *conn, pk *packet.Packet, now time.Time) (matched bool) {
	subs := s.topics.Subscribers(pk.TopicName)
	senderID := ""
	if sender != nil {
		if cl := sender.sessionClient(); cl != nil {
			senderID = cl.ID
		}
	}
	for id, sub := range subs {
		if sub.NoLocal && id == senderID {
			continue
		}
		matched = true
		target := s.lookupConn(id)
		if target == nil || target.isStopped() || target.sessionClient() == nil ||
			target.sessionClient().TakenOver() {
			continue // offline; shared-group selection lands elsewhere next time
		}
		s.deliverTo(target, sub, pk, now, false)
	}
	return matched
}

// deliverTo builds the per-subscriber PUBLISH and hands it to the session
// inflight window; the returned packet is queued on the connection. replay
// marks subscribe-time retained delivery, where the RETAIN flag produced by
// topics.Messages ([MQTT-3.3.1-9]) is kept as is.
func (s *Server) deliverTo(target *conn, sub topics.Subscription, pk *packet.Packet, now time.Time, replay bool) {
	client := target.sessionClient()
	if client == nil {
		return
	}
	out := buildDelivery(client, sub, pk, now, replay)
	if out == nil {
		s.Stats.MessagesDropped.Add(1)
		return // expired or otherwise undeliverable
	}
	if !fitsPeerMaximum(client, out) {
		// The message exceeds the Maximum Packet Size the client declared
		// in CONNECT: drop it without registering it in inflight, so an
		// undeliverable QoS 1/2 message cannot be retransmitted forever
		// [MQTT-3.1.2-24]. This is a peer-capacity drop, not backpressure.
		s.Stats.MessagesOversized.Add(1)
		s.log.Debug("dropping message exceeding the client maximum packet size",
			"client", client.ID, "topic", out.TopicName)
		return
	}
	send, stored, err := client.EnqueuePublish(now, out)
	if err != nil {
		// Packet identifiers exhausted: an effectively unreachable state.
		s.log.Warn("dropping undeliverable message", "client", client.ID, "error", err)
		s.Stats.MessagesDropped.Add(1)
		return
	}
	s.storePutOutbound(client.ID, stored)
	if send != nil {
		target.enqueue(send)
	}
}

// fitsPeerMaximum reports whether the encoded form of pk fits within the
// Maximum Packet Size the client declared in CONNECT. MQTT 3.1.1 clients and
// v5 clients without the property accept any size. QoS 1/2 packets are
// measured with a Packet Identifier present, since SendPublish assigns one
// afterwards.
func fitsPeerMaximum(client *session.Client, pk *packet.Packet) bool {
	if client.ProtocolVersion != packet.Version5 || !client.Properties.MaximumPacketSizeFlag {
		return true
	}
	probe := *pk
	if probe.FixedHeader.Qos > 0 && probe.PacketID == 0 {
		probe.PacketID = 1
	}
	encoded, err := probe.Encode()
	if err != nil {
		return true // encode failures are counted and dropped by the write loop
	}
	return len(encoded) <= int(client.Properties.MaximumPacketSize)
}

// buildDelivery creates the outbound PUBLISH for one subscriber: QoS
// downgrade [MQTT-3.8.4-3], retain flag rules [MQTT-3.3.1-9], v5 property
// forwarding with the Message Expiry rewritten to the remaining seconds
// [MQTT-3.3.2-5], Subscription Identifiers [MQTT-3.3.4-6] and the outbound
// Topic Alias. nil means the message must not be delivered (expired).
// replay marks subscribe-time retained delivery, which keeps the RETAIN
// flag topics.Messages computed.
func buildDelivery(client *session.Client, sub topics.Subscription, pk *packet.Packet, now time.Time, replay bool) *packet.Packet {
	if !pk.Expiry.IsZero() && !now.Before(pk.Expiry) {
		return nil
	}
	out := &packet.Packet{
		FixedHeader: packet.FixedHeader{
			Type:   packet.Publish,
			Qos:    session.DowngradeQoS(pk.FixedHeader.Qos, sub.Qos),
			Retain: pk.FixedHeader.Retain,
		},
		TopicName:       pk.TopicName,
		Payload:         pk.Payload,
		Expiry:          pk.Expiry,
		Created:         pk.Created,
		ProtocolVersion: client.ProtocolVersion,
	}
	// Live delivery: RetainAsPublished controls whether the retain flag
	// passes through; without it (and for every MQTT 3.1.1 delivery) the
	// flag is cleared [MQTT-3.3.1-9]. Retained replays keep the flag that
	// topics.Messages already computed.
	if !replay && !sub.RetainAsPublished {
		out.FixedHeader.Retain = false
	}
	if client.ProtocolVersion != packet.Version5 {
		return out
	}

	props := &out.Properties
	props.PayloadFormat, props.PayloadFormatFlag = pk.Properties.PayloadFormat, pk.Properties.PayloadFormatFlag
	props.ContentType, props.ContentTypeFlag = pk.Properties.ContentType, pk.Properties.ContentTypeFlag
	props.ResponseTopic, props.ResponseTopicFlag = pk.Properties.ResponseTopic, pk.Properties.ResponseTopicFlag
	props.CorrelationData, props.CorrelationDataFlag = pk.Properties.CorrelationData, pk.Properties.CorrelationDataFlag
	props.User = pk.Properties.User
	if !pk.Expiry.IsZero() {
		// Rewrite Message Expiry to the remaining interval; sub-second
		// remainders round up because 0 would mean "no expiry".
		remaining := uint32(pk.Expiry.Sub(now) / time.Second)
		if remaining == 0 {
			remaining = 1
		}
		props.MessageExpiry = remaining
		props.MessageExpiryFlag = true
	}
	if len(sub.Identifiers) > 0 {
		props.SubscriptionID = sub.Identifiers
	}
	// Assign an outbound Topic Alias and keep the topic name on the wire:
	// the pair (re)registers the mapping, which stays valid even when the
	// client's table was reset by a reconnect [MQTT-3.3.2-9].
	if alias, ok := client.AssignOutboundAlias(pk.TopicName); ok {
		props.TopicAlias = alias
		props.TopicAliasFlag = true
	}
	return out
}
