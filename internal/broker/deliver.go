package broker

import (
	"context"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// publishFrom handles the broker side of an inbound PUBLISH after the QoS
// state machine accepted it: topic alias resolution, authorization, retained
// storage and fan-out. It is called for QoS 0/1 directly from the read loop
// and for QoS 2 when the PUBREL completes the handshake (strict
// exactly-once, [MQTT-4.3.3-1]). A returned error is fatal to the
// connection.
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

	matched := false
	if deliver != nil {
		s.Stats.MessagesReceived.Add(1)
		if deliver.FixedHeader.Retain {
			if err := s.topics.Retain(*deliver); err != nil {
				s.log.Warn("retain failed", "topic", deliver.TopicName, "error", err)
			}
		}
		matched = s.deliver(c, deliver, now)
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
		s.deliverTo(target, sub, pk, now)
	}
	return matched
}

// deliverTo builds the per-subscriber PUBLISH and hands it to the session
// inflight window; the returned packet is queued on the connection.
func (s *Server) deliverTo(target *conn, sub topics.Subscription, pk *packet.Packet, now time.Time) {
	client := target.sessionClient()
	if client == nil {
		return
	}
	out := buildDelivery(client, sub, pk, now)
	if out == nil {
		s.Stats.MessagesDropped.Add(1)
		return // expired or otherwise undeliverable
	}
	send, err := client.SendPublish(now, out)
	if err != nil {
		// Packet identifiers exhausted: an effectively unreachable state.
		s.log.Warn("dropping undeliverable message", "client", client.ID, "error", err)
		s.Stats.MessagesDropped.Add(1)
		return
	}
	if send != nil {
		target.enqueue(send)
	}
}

// buildDelivery creates the outbound PUBLISH for one subscriber: QoS
// downgrade [MQTT-3.8.4-3], retain flag rules [MQTT-3.3.1-9], v5 property
// forwarding with the Message Expiry rewritten to the remaining seconds
// [MQTT-3.3.2-5], Subscription Identifiers [MQTT-3.3.4-6] and the outbound
// Topic Alias. nil means the message must not be delivered (expired).
func buildDelivery(client *session.Client, sub topics.Subscription, pk *packet.Packet, now time.Time) *packet.Packet {
	if !pk.Expiry.IsZero() && !now.Before(pk.Expiry) {
		return nil
	}
	out := &packet.Packet{
		FixedHeader: packet.FixedHeader{
			Type: packet.Publish,
			Qos:  session.DowngradeQoS(pk.FixedHeader.Qos, sub.Qos),
		},
		TopicName:       pk.TopicName,
		Payload:         pk.Payload,
		Expiry:          pk.Expiry,
		Created:         pk.Created,
		ProtocolVersion: client.ProtocolVersion,
	}
	// RetainAsPublished controls whether the retain flag passes through;
	// without it (and for every MQTT 3.1.1 delivery) the flag is cleared
	// [MQTT-3.3.1-9].
	if sub.RetainAsPublished {
		out.FixedHeader.Retain = pk.FixedHeader.Retain
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
