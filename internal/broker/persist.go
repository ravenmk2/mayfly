package broker

import (
	"context"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
	"github.com/ravenmk2/mayfly/internal/store"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// Persistence wiring (docs/design/persistence.md): the in-memory state stays
// authoritative and every mutation is mirrored into the Store, which is a
// crash-recovery replica. When a BatchWriter fronts the Store the writes are
// asynchronous and ordered per entity; the helpers here never block the
// broker beyond the bounded queue's backpressure.

// storePutSession mirrors the session record (connected: StoppedAt zero;
// disconnected: StoppedAt set).
func (s *Server) storePutSession(cl *session.Client) {
	st := s.getStore()
	if st == nil {
		return
	}
	if err := st.Sessions().Put(context.Background(), store.Session{
		ClientID:  cl.ID,
		Expiry:    cl.Expiry(),
		CreatedAt: cl.CreatedAt,
		StoppedAt: cl.StoppedAt(),
	}); err != nil {
		s.log.Debug("session persist failed", "client", cl.ID, "error", err)
	}
}

// storeDeleteSessionState removes every persisted trace of a session: the
// row itself, its subscriptions and both inflight windows.
func (s *Server) storeDeleteSessionState(clientID string) {
	st := s.getStore()
	if st == nil {
		return
	}
	ctx := context.Background()
	if err := st.Subscriptions().DeleteByClient(ctx, clientID); err != nil {
		s.log.Debug("subscription purge failed", "client", clientID, "error", err)
	}
	if err := st.Inflight().DeleteByClient(ctx, clientID); err != nil {
		s.log.Debug("inflight purge failed", "client", clientID, "error", err)
	}
	if err := st.Sessions().Delete(ctx, clientID); err != nil {
		s.log.Debug("session delete failed", "client", clientID, "error", err)
	}
}

// storePutSubscription mirrors one granted subscription.
func (s *Server) storePutSubscription(clientID string, sub topics.Subscription) {
	st := s.getStore()
	if st == nil {
		return
	}
	rec := store.Subscription{
		ClientID:          clientID,
		Filter:            fullFilter(sub),
		Qos:               sub.Qos,
		NoLocal:           sub.NoLocal,
		RetainAsPublished: sub.RetainAsPublished,
		RetainHandling:    sub.RetainHandling,
	}
	if len(sub.Identifiers) > 0 {
		rec.Identifier = sub.Identifiers[0]
	}
	if err := st.Subscriptions().Put(context.Background(), rec); err != nil {
		s.log.Debug("subscription persist failed", "client", clientID, "filter", rec.Filter, "error", err)
	}
}

// storeDeleteSubscription mirrors one removed subscription.
func (s *Server) storeDeleteSubscription(clientID, filter string) {
	st := s.getStore()
	if st == nil {
		return
	}
	if err := st.Subscriptions().Delete(context.Background(), clientID, filter); err != nil {
		s.log.Debug("subscription delete failed", "client", clientID, "filter", filter, "error", err)
	}
}

// storePutOutbound mirrors one outbound inflight entry (queued, sent and
// PUBREL-stage alike).
func (s *Server) storePutOutbound(clientID string, msg *session.InflightMessage) {
	st := s.getStore()
	if st == nil || msg == nil {
		return
	}
	rec := store.InflightMessage{
		ClientID:  clientID,
		PacketID:  msg.Packet.PacketID,
		Direction: store.Outbound,
		Packet:    msg.Packet,
		Sent:      msg.Sent,
		Pubrel:    msg.State == session.InflightPubrel,
		Expiry:    msg.Expiry,
	}
	if err := st.Inflight().Put(context.Background(), rec); err != nil {
		s.log.Debug("inflight persist failed", "client", clientID, "packetID", rec.PacketID, "error", err)
	}
}

// storePutInbound mirrors one inbound QoS 2 PUBLISH awaiting its PUBREL.
func (s *Server) storePutInbound(clientID string, pk *packet.Packet) {
	st := s.getStore()
	if st == nil || pk == nil {
		return
	}
	rec := store.InflightMessage{
		ClientID:  clientID,
		PacketID:  pk.PacketID,
		Direction: store.Inbound,
		Packet:    *pk,
		Expiry:    pk.Expiry,
	}
	if err := st.Inflight().Put(context.Background(), rec); err != nil {
		s.log.Debug("inbound inflight persist failed", "client", clientID, "packetID", pk.PacketID, "error", err)
	}
}

// storePutPromoted mirrors a queued message promoted into a freed Receive
// Maximum window slot: its persisted Sent flag flips so a crash-recovery
// resend correctly carries DUP=1.
func (s *Server) storePutPromoted(cl *session.Client, packetID uint16) {
	if s.getStore() == nil || cl == nil {
		return
	}
	if msg, ok := cl.OutboundMessage(packetID); ok {
		s.storePutOutbound(cl.ID, &msg)
	}
}

// storeDeleteInflight mirrors one completed or expired inflight entry.
func (s *Server) storeDeleteInflight(clientID string, dir store.Direction, packetID uint16) {
	st := s.getStore()
	if st == nil {
		return
	}
	if err := st.Inflight().Delete(context.Background(), clientID, dir, packetID); err != nil {
		s.log.Debug("inflight delete failed", "client", clientID, "packetID", packetID, "error", err)
	}
}

// retain wraps the trie Retain and mirrors the outcome into the store: an
// empty payload deletes the retained message [MQTT-3.3.1-6].
func (s *Server) retain(pk packet.Packet) {
	if err := s.topics.Retain(pk); err != nil {
		s.log.Warn("retain failed", "topic", pk.TopicName, "error", err)
		return
	}
	st := s.getStore()
	if st == nil {
		return
	}
	ctx := context.Background()
	if len(pk.Payload) == 0 {
		if err := st.Retained().Delete(ctx, pk.TopicName); err != nil {
			s.log.Debug("retained delete failed", "topic", pk.TopicName, "error", err)
		}
		return
	}
	pk.FixedHeader.Retain = true
	if err := st.Retained().Put(ctx, store.RetainedMessage{
		Topic:  pk.TopicName,
		Packet: pk,
		Expiry: pk.Expiry,
	}); err != nil {
		s.log.Debug("retained persist failed", "topic", pk.TopicName, "error", err)
	}
}

// recoverStore rebuilds the in-memory state from the Store at startup,
// before any listener accepts connections (docs/design/persistence.md
// section 3): sessions first, then their subscriptions and inflight windows,
// then the retained messages. Expired rows are purged. Sessions whose expiry
// is 0 ended at the moment the previous process died and are not restored;
// sessions that were connected at the crash (StoppedAt zero) restart their
// expiry countdown at recovery time.
func (s *Server) recoverStore() {
	st := s.getStore()
	if st == nil {
		return
	}
	ctx := context.Background()
	now := s.now()
	var sessions, subs, inflight, retained, purged int

	err := st.Sessions().Iterate(ctx, func(rec store.Session) bool {
		if rec.Expiry == 0 || sessionRowExpired(rec, now) {
			purged++
			s.storeDeleteSessionState(rec.ClientID)
			return true
		}
		stoppedAt := rec.StoppedAt
		if stoppedAt.IsZero() {
			stoppedAt = now
		}
		cl := session.NewRestoredClient(rec.ClientID, rec.Expiry, rec.CreatedAt, stoppedAt, session.Config{
			ReceiveMaximum:    s.opts.ReceiveMaximum,
			TopicAliasMaximum: s.opts.TopicAliasMaximum,
		})
		c := newConn(s, "restored-"+rec.ClientID, nil)
		c.client = cl
		c.stop() // a restored session is offline until the client reconnects
		s.mu.Lock()
		s.clients[rec.ClientID] = c
		s.scheduleSessionExpiryLocked(cl)
		s.mu.Unlock()
		sessions++
		return true
	})
	if err != nil {
		s.log.Error("session recovery failed", "error", err)
	}

	err = st.Subscriptions().Iterate(ctx, func(rec store.Subscription) bool {
		c := s.lookupConn(rec.ClientID)
		if c == nil || c.sessionClient() == nil {
			purged++
			s.storeDeleteSubscription(rec.ClientID, rec.Filter)
			return true
		}
		sub := topics.Subscription{
			Filter:            rec.Filter,
			Qos:               rec.Qos,
			NoLocal:           rec.NoLocal,
			RetainAsPublished: rec.RetainAsPublished,
			RetainHandling:    rec.RetainHandling,
		}
		if rec.Identifier != 0 {
			sub.Identifiers = []uint32{rec.Identifier}
		}
		if _, err := s.topics.Subscribe(rec.ClientID, sub); err != nil {
			s.log.Warn("subscription recovery failed", "client", rec.ClientID,
				"filter", rec.Filter, "error", err)
			return true
		}
		c.sessionClient().AddSubscription(sub)
		subs++
		return true
	})
	if err != nil {
		s.log.Error("subscription recovery failed", "error", err)
	}

	err = st.Inflight().Iterate(ctx, func(rec store.InflightMessage) bool {
		c := s.lookupConn(rec.ClientID)
		if c == nil || c.sessionClient() == nil {
			purged++
			s.storeDeleteInflight(rec.ClientID, rec.Direction, rec.PacketID)
			return true
		}
		if !rec.Expiry.IsZero() && !now.Before(rec.Expiry) {
			purged++
			s.storeDeleteInflight(rec.ClientID, rec.Direction, rec.PacketID)
			return true
		}
		cl := c.sessionClient()
		pk := rec.Packet
		pk.Expiry = rec.Expiry
		if rec.Direction == store.Inbound {
			cl.RestoreInbound(&pk)
			inflight++
			return true
		}
		// Topic alias mappings are connection-scoped [MQTT-3.3.2-6]; the
		// restored packet must carry its topic name on the wire.
		pk.Properties.TopicAlias = 0
		pk.Properties.TopicAliasFlag = false
		state := session.InflightPublish
		if rec.Pubrel {
			state = session.InflightPubrel
		}
		cl.RestoreOutbound(session.InflightMessage{
			Packet: pk,
			Sent:   rec.Sent,
			State:  state,
			Expiry: rec.Expiry,
		})
		inflight++
		return true
	})
	if err != nil {
		s.log.Error("inflight recovery failed", "error", err)
	}

	err = st.Retained().Iterate(ctx, func(rec store.RetainedMessage) bool {
		if !rec.Expiry.IsZero() && !now.Before(rec.Expiry) {
			purged++
			if err := st.Retained().Delete(ctx, rec.Topic); err != nil {
				s.log.Debug("retained purge failed", "topic", rec.Topic, "error", err)
			}
			return true
		}
		pk := rec.Packet
		pk.Expiry = rec.Expiry
		if err := s.topics.Retain(pk); err != nil {
			s.log.Warn("retained recovery failed", "topic", rec.Topic, "error", err)
			return true
		}
		retained++
		return true
	})
	if err != nil {
		s.log.Error("retained recovery failed", "error", err)
	}

	s.log.Info("store recovered", "sessions", sessions, "subscriptions", subs,
		"inflight", inflight, "retained", retained, "purged", purged)
}

// sessionRowExpired reports whether a stored session whose disconnect time
// is known passed its expiry deadline.
func sessionRowExpired(rec store.Session, now time.Time) bool {
	if rec.StoppedAt.IsZero() || rec.Expiry == session.SessionExpiryNever {
		return false
	}
	deadline, ok := session.SessionExpiryDeadline(rec.Expiry, rec.StoppedAt)
	return ok && !deadline.After(now)
}
