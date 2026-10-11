package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
	"github.com/ravenmk2/mayfly/internal/store"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// processPacket dispatches one decoded packet through the session state
// machine and the delivery engine. A returned error ends the connection:
// errNormalClose silently, packet.Code with a v5 DISCONNECT first.
func (c *conn) processPacket(pk *packet.Packet) error {
	s := c.server
	switch pk.FixedHeader.Type {
	case packet.Connect:
		return c.onConnect(pk)
	case packet.Publish:
		return s.publishFrom(c, pk)
	case packet.Puback:
		cl := c.sessionClient()
		if next := cl.ReceivePuback(pk.PacketID); next != nil {
			c.enqueue(next)
			s.storePutPromoted(cl, next.PacketID)
		}
		s.storeDeleteInflight(cl.ID, store.Outbound, pk.PacketID)
	case packet.Pubrec:
		cl := c.sessionClient()
		pubrel, err := cl.ReceivePubrec(pk.PacketID)
		if err != nil {
			return err
		}
		c.enqueue(&pubrel)
		// The QoS 2 handshake advanced to the PUBREL stage: persist the
		// transition so a crash does not resurrect the PUBLISH stage.
		if msg, ok := cl.OutboundMessage(pk.PacketID); ok {
			s.storePutOutbound(cl.ID, &msg)
		}
	case packet.Pubrel:
		cl := c.sessionClient()
		deliver, ack := cl.ReceivePubrel(pk.PacketID)
		s.storeDeleteInflight(cl.ID, store.Inbound, pk.PacketID)
		if deliver != nil {
			s.releaseQoS2(c, deliver)
		}
		c.enqueue(&ack)
	case packet.Pubcomp:
		cl := c.sessionClient()
		if next := cl.ReceivePubcomp(pk.PacketID); next != nil {
			c.enqueue(next)
			s.storePutPromoted(cl, next.PacketID)
		}
		s.storeDeleteInflight(cl.ID, store.Outbound, pk.PacketID)
	case packet.Subscribe:
		return c.onSubscribe(pk)
	case packet.Unsubscribe:
		return c.onUnsubscribe(pk)
	case packet.Pingreq:
		c.enqueue(&packet.Packet{
			FixedHeader:     packet.FixedHeader{Type: packet.Pingresp},
			ProtocolVersion: c.protocolVersion(),
		})
	case packet.Disconnect:
		return c.onDisconnect(pk)
	case packet.Auth:
		// Enhanced authentication is not supported; an AUTH packet outside
		// an exchange is a Protocol Error [MQTT-3.15.2-2].
		return packet.Code{Code: packet.CodeProtocolError,
			Reason: "AUTH received without an enhanced authentication exchange"}
	default:
		// A client MUST NOT send CONNACK/SUBACK/... to a server
		// [MQTT-3.1.0-1]; treat server-only packets as protocol errors.
		return packet.Code{Code: packet.CodeProtocolError,
			Reason: "packet type not permitted from a client"}
	}
	return nil
}

// onConnect runs the CONNECT handshake: authentication, client identifier
// assignment, session takeover/resume, CONNACK and inflight resend.
func (c *conn) onConnect(pk *packet.Packet) error {
	s := c.server
	if c.sessionClient() != nil {
		// A second CONNECT on one connection is a Protocol Error
		// [MQTT-3.1.0-2].
		return packet.Code{Code: packet.CodeProtocolError,
			Reason: "CONNECT received on an established connection"}
	}

	client := session.NewClient(pk, session.Config{
		ReceiveMaximum:    s.opts.ReceiveMaximum,
		TopicAliasMaximum: s.opts.TopicAliasMaximum,
	})
	client.MarkCreated(s.now())
	c.mu.Lock()
	c.version = client.ProtocolVersion
	c.mu.Unlock()

	if client.ProtocolVersion == packet.Version5 {
		// Enhanced authentication is not supported: a CONNECT carrying an
		// Authentication Method is rejected with 0x8C [MQTT-3.1.2-22].
		if pk.Properties.AuthMethodFlag {
			s.Stats.AuthFailures.Add(1)
			c.rejectConnect(client.ProtocolVersion, packet.CodeBadAuthenticationMethod)
			return errNormalClose
		}
		// Request Problem Information 0 opts out of Reason String
		// properties [MQTT-3.1.2-29].
		if pk.Properties.RequestProblemInfoFlag && pk.Properties.RequestProblemInfo == 0 {
			c.omitReasonString.Store(true)
		}
	}

	// An mTLS listener configured with use_cn_as_username vouches for the
	// client identity: the verified certificate CN replaces any CONNECT
	// username (docs/design/listeners.md section 2).
	if lc, ok := c.nc.(*listener.Conn); ok && lc.CertUsername != "" {
		client.Username = lc.CertUsername
	}

	// Authentication happens before any session state is touched.
	res, err := s.authn.Authenticate(context.Background(), hook.ConnectInfo{
		ClientID:        client.ID,
		Username:        client.Username,
		Password:        pk.Connect.Password,
		RemoteAddr:      c.remoteAddr(),
		ListenerID:      c.listenerID(),
		ProtocolVersion: client.ProtocolVersion,
		CleanStart:      client.CleanStart,
		Keepalive:       client.Keepalive,
	})
	if err != nil || !res.OK {
		s.Stats.AuthFailures.Add(1)
		code := packet.CodeNotAuthorized
		if pk.Connect.UsernameFlag || pk.Connect.PasswordFlag {
			code = packet.CodeBadUserNameOrPassword
		}
		reason := res.Reason
		if err != nil {
			reason = "authentication failed"
		}
		s.log.Debug("authentication rejected", "client", client.ID, "reason", reason)
		c.rejectConnect(client.ProtocolVersion, code)
		return errNormalClose
	}
	s.Stats.AuthSucceeded.Add(1)

	// Empty Client Identifier: assign one when the client starts a fresh
	// session, reject persistent sessions without an identifier
	// [MQTT-3.1.3-6] [MQTT-3.1.3-8].
	assignedID := ""
	if client.ID == "" {
		if !client.CleanStart {
			c.rejectConnect(client.ProtocolVersion, packet.CodeClientIdentifierNotValid)
			return errNormalClose
		}
		assignedID = "auto-" + hex.EncodeToString(randBytes(8))
		client.ID = assignedID
	}

	// Session takeover and resume.
	var endedWill *session.Will // old session ends on a clean takeover
	cleanTakeover := false
	s.mu.Lock()
	old := s.clients[client.ID]
	existed := false
	if old != nil {
		if oldClient := old.sessionClient(); oldClient != nil && !oldClient.TakenOver() {
			existed = true
			oldClient.TakeOver()
			if oldClient.ProtocolVersion == packet.Version5 {
				old.enqueue(&packet.Packet{
					FixedHeader:     packet.FixedHeader{Type: packet.Disconnect},
					ProtocolVersion: packet.Version5,
					ReasonCode:      packet.CodeSessionTakenOver,
				})
			}
			old.stop()
			if client.CleanStart {
				// The old session ends here; its delayed Will fires now
				// [MQTT-3.1.3-9].
				s.wills.Cancel(client.ID)
				endedWill = oldClient.Will
				s.removeSubscriptionsLocked(oldClient)
				cleanTakeover = true
			} else {
				// The session continues: a pending delayed Will is
				// cancelled and durable state migrates [MQTT-3.1.2-4].
				s.wills.Cancel(client.ID)
				client.MigrateFrom(oldClient)
			}
		}
	}
	s.clients[client.ID] = c
	s.mu.Unlock()

	if cleanTakeover {
		// Purge the ended session's persisted subscriptions and inflight
		// before the fresh session row replaces it.
		s.storeDeleteSessionState(client.ID)
	}
	s.storePutSession(client)

	if endedWill != nil {
		s.publishWill(client.ID, endedWill)
	}

	c.mu.Lock()
	c.client = client
	c.info = hook.ClientInfo{
		ClientID:   client.ID,
		Username:   client.Username,
		Superuser:  res.Superuser,
		RemoteAddr: c.remoteAddr(),
		ListenerID: c.listenerID(),
	}
	c.mu.Unlock()

	// CONNACK: the v3 encoder downgrades the v5 reason code automatically.
	connack := &packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Connack},
		ProtocolVersion: client.ProtocolVersion,
		ReasonCode:      packet.CodeSuccess,
		SessionPresent:  session.SessionPresent(client.CleanStart, existed),
	}
	if client.ProtocolVersion == packet.Version5 {
		connack.Properties.ReceiveMaximum = s.opts.ReceiveMaximum
		connack.Properties.ReceiveMaximumFlag = true
		connack.Properties.TopicAliasMaximum = s.opts.TopicAliasMaximum
		connack.Properties.TopicAliasMaximumFlag = true
		if s.opts.MaxPacketSize > 0 {
			// Advertise the broker's inbound packet limit so the client
			// must not send larger packets [MQTT-3.1.2-24].
			connack.Properties.MaximumPacketSize = uint32(s.opts.MaxPacketSize) //nolint:gosec // bounded by packet.MaxRemainingLength
			connack.Properties.MaximumPacketSizeFlag = true
		}
		if assignedID != "" {
			connack.Properties.AssignedClientID = assignedID
			connack.Properties.AssignedClientIDFlag = true
		}
	}
	c.enqueue(connack)

	// A resumed session replays its unacknowledged outbound messages with
	// DUP=1 [MQTT-4.4.0-1].
	if connack.SessionPresent {
		for _, resend := range client.ResendInflight() {
			r := resend
			c.enqueue(&r)
		}
	}
	// Messages queued behind the Receive Maximum window — the offline queue
	// included — fill the free slots now.
	for _, p := range client.PromoteQueued() {
		promoted := p
		c.enqueue(&promoted)
		s.storePutPromoted(client, promoted.PacketID)
	}
	s.log.Debug("client connected", "client", client.ID, "version", client.ProtocolVersion,
		"sessionPresent", connack.SessionPresent)
	return nil
}

// randBytes returns n random bytes for assigned client identifiers.
func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// rejectConnect answers a failed CONNECT with a CONNACK carrying the reason
// code; the caller then closes the connection.
func (c *conn) rejectConnect(version byte, code byte) {
	c.enqueue(&packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Connack},
		ProtocolVersion: version,
		ReasonCode:      code,
	})
	c.stop()
}

// onSubscribe processes SUBSCRIBE filter by filter: authorization first,
// then the trie update, then the SUBACK, and finally retained delivery
// honoring Retain Handling [MQTT-3.8.3-1].
func (c *conn) onSubscribe(pk *packet.Packet) error {
	s := c.server
	client := c.sessionClient()
	version := c.protocolVersion()
	info := c.clientInfo()

	denyCode := packet.V3SubackFailure
	if version == packet.Version5 {
		denyCode = packet.CodeNotAuthorized
	}

	type grantedSub struct {
		sub     topics.Subscription
		existed bool
	}
	var granted []grantedSub
	codes := make([]byte, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		sub := topics.Subscription{
			Filter:            f.Filter,
			Qos:               f.Qos,
			NoLocal:           f.NoLocal,
			RetainAsPublished: f.RetainAsPublished,
			RetainHandling:    f.RetainHandling,
		}
		if f.Identifier != 0 {
			sub.Identifiers = []uint32{f.Identifier}
		}
		if !s.authz.Allow(context.Background(), info, hook.OpSubscribe, f.Filter) {
			s.Stats.ACLDenied.Add(1)
			s.log.Debug("subscribe denied", "client", client.ID, "filter", f.Filter)
			codes = append(codes, denyCode)
			continue
		}
		s.Stats.ACLAllowed.Add(1)
		// Subscribe splits the shared prefix into sub.Shared / sub.Filter.
		existed, err := s.topics.Subscribe(client.ID, sub)
		if err != nil {
			code := denyCode
			if version == packet.Version5 {
				code = packet.CodeTopicFilterInvalid
			}
			codes = append(codes, code)
			continue
		}
		client.AddSubscription(sub)
		s.storePutSubscription(client.ID, sub)
		codes = append(codes, f.Qos) // granted QoS equals the numeric reason code
		granted = append(granted, grantedSub{sub: sub, existed: existed})
	}

	c.enqueue(&packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Suback},
		ProtocolVersion: version,
		PacketID:        pk.PacketID,
		ReasonCodes:     codes,
	})

	// Retained messages follow the SUBACK [MQTT-3.3.1-8]; the single
	// outbound queue preserves that ordering.
	now := s.now()
	for _, g := range granted {
		for _, retained := range s.topics.Messages(g.sub, g.existed) {
			m := retained
			s.deliverTo(c, g.sub, &m, now, true)
		}
	}
	return nil
}

// onUnsubscribe removes each filter from the trie and the session, then
// acknowledges. v5 reports 0x11 for filters that had no subscription
// [MQTT-3.11.3-1].
func (c *conn) onUnsubscribe(pk *packet.Packet) error {
	s := c.server
	client := c.sessionClient()
	version := c.protocolVersion()

	codes := make([]byte, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		if err := s.topics.Unsubscribe(client.ID, f.Filter); err != nil {
			return packet.Code{Code: packet.CodeTopicFilterInvalid, Reason: err.Error()}
		}
		s.storeDeleteSubscription(client.ID, f.Filter)
		removed := false
		for _, sub := range client.Subscriptions() {
			if fullFilter(sub) == f.Filter {
				removed = client.RemoveSubscription(session.SubscriptionKey(sub))
				break
			}
		}
		switch {
		case version != packet.Version5:
			// v3 UNSUBACK carries no reason codes.
		case removed:
			codes = append(codes, packet.CodeSuccess)
		default:
			codes = append(codes, packet.CodeNoSubscriptionExisted)
		}
	}
	c.enqueue(&packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Unsuback},
		ProtocolVersion: version,
		PacketID:        pk.PacketID,
		ReasonCodes:     codes,
	})
	return nil
}

// onDisconnect applies the v5 Session Expiry override and ends the
// connection; the cleanup path decides Will publication from the reason
// code [MQTT-3.1.2-8].
func (c *conn) onDisconnect(pk *packet.Packet) error {
	client := c.sessionClient()
	c.mu.Lock()
	c.receivedDisconnect = true
	c.disconnectReason = pk.ReasonCode
	c.mu.Unlock()
	if c.protocolVersion() == packet.Version5 {
		expiry, err := session.SessionExpiryAfterDisconnect(client.Expiry(), pk)
		if err != nil {
			// Extending a session that expires at disconnect is a Protocol
			// Error [MQTT-3.1.2-11]: notify the client before closing.
			c.fail(err)
			return errNormalClose
		}
		client.SetSessionExpiry(expiry)
	}
	return errNormalClose
}

// clientInfo returns the authorization view of the connection.
func (c *conn) clientInfo() hook.ClientInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

// cleanup runs once per connection when its read loop ends: it deregisters
// or persists the session and arranges Will publication.
func (c *conn) cleanup() {
	c.cleanupOnce.Do(func() {
		s := c.server
		s.Stats.ConnectionsCurrent.Add(-1)

		c.mu.Lock()
		client := c.client
		receivedDisconnect := c.receivedDisconnect
		reason := c.disconnectReason
		c.mu.Unlock()
		if client == nil {
			return
		}

		publishWill := session.WillOnDisconnect(receivedDisconnect, reason) &&
			client.Will != nil && !client.TakenOver() && !c.suppressWill.Load()

		sessionEnded, sessionStopped := false, false
		s.mu.Lock()
		if s.clients[client.ID] == c && !client.TakenOver() {
			if client.Expiry() == 0 {
				// The session ends at network disconnection
				// [MQTT-3.1.2-11].
				delete(s.clients, client.ID)
				s.removeSubscriptionsLocked(client)
				sessionEnded = true
			} else {
				client.MarkStopped(s.now())
				s.scheduleSessionExpiryLocked(client)
				sessionStopped = true
			}
		}
		s.mu.Unlock()

		switch {
		case sessionEnded:
			s.storeDeleteSessionState(client.ID)
		case sessionStopped:
			s.storePutSession(client)
		}

		if publishWill {
			s.arrangeWill(client)
		}
		s.log.Debug("client disconnected", "client", client.ID)
	})
}

// arrangeWill publishes the Will immediately or schedules it for its Will
// Delay Interval, capped by the session expiry [MQTT-3.1.3-8].
func (s *Server) arrangeWill(client *session.Client) {
	will := client.Will
	now := s.now()
	delay := uint32(0)
	if will.Properties.WillDelayFlag {
		delay = will.Properties.WillDelay
	}
	at := session.WillPublishTime(now, delay, client.Expiry())
	if delay == 0 || !at.After(now) {
		s.publishWill(client.ID, will)
		return
	}
	s.wills.Schedule(session.ScheduledWill{
		ClientID: client.ID,
		Message:  willPacket(will, now),
		At:       at,
	})
}

// publishWill builds the Will PUBLISH and injects it into the delivery
// engine as if the client had sent it.
func (s *Server) publishWill(clientID string, will *session.Will) {
	pk := willPacket(will, s.now())
	if will.Retain {
		s.retain(pk)
	}
	s.deliver(nil, &pk, s.now())
	s.log.Debug("will published", "client", clientID, "topic", will.Topic)
}

// fireWill injects a scheduled Will whose delay elapsed.
func (s *Server) fireWill(pk *packet.Packet) {
	if pk.FixedHeader.Retain {
		s.retain(*pk)
	}
	s.deliver(nil, pk, s.now())
}

// willPacket converts a session Will into an outbound PUBLISH. The Will
// Delay property is stripped: it is not valid on PUBLISH [MQTT-3.1.3.2].
func willPacket(will *session.Will, now time.Time) packet.Packet {
	props := will.Properties
	props.WillDelay = 0
	props.WillDelayFlag = false
	pk := packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: will.Qos, Retain: will.Retain},
		TopicName:   will.Topic,
		Payload:     will.Payload,
		Properties:  props,
		Created:     now,
	}
	if props.MessageExpiryFlag {
		// The Message Expiry Interval starts when the Will is published
		// [MQTT-3.1.3-9].
		pk.Expiry = now.Add(time.Duration(props.MessageExpiry) * time.Second)
	}
	return pk
}
