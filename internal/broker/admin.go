package broker

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// ConnInfo is the management view of one registered client (connected or
// with a persisted session), consumed by the admin REST API
// (docs/design/admin-api.md section 3).
type ConnInfo struct {
	ClientID        string
	Username        string
	RemoteAddr      string
	ListenerID      string
	ProtocolVersion byte
	// Connected reports whether the network connection is live; false marks
	// an offline persisted session.
	Connected   bool
	ConnectedAt time.Time
	// CreatedAt is when the session was first established.
	CreatedAt time.Time
	// StoppedAt is the disconnect time of an offline session (zero while
	// connected).
	StoppedAt      time.Time
	Keepalive      uint16
	CleanStart     bool
	SessionExpiry  uint32
	Superuser      bool
	Subscriptions  int
	InflightOut    int
	InflightIn     int
}

// connInfo snapshots one registered connection.
func (s *Server) connInfo(c *conn) ConnInfo {
	info := ConnInfo{Connected: !c.isStopped()}
	c.mu.Lock()
	info.RemoteAddr = c.remoteAddr()
	info.ListenerID = c.listenerID()
	info.ConnectedAt = c.connectedAt
	info.Superuser = c.info.Superuser
	c.mu.Unlock()
	cl := c.sessionClient()
	if cl == nil {
		return info
	}
	info.ClientID = cl.ID
	info.Username = cl.Username
	info.ProtocolVersion = cl.ProtocolVersion
	info.Keepalive = cl.Keepalive
	info.CleanStart = cl.CleanStart
	info.SessionExpiry = cl.Expiry()
	info.CreatedAt = cl.CreatedAt
	info.StoppedAt = cl.StoppedAt()
	info.Subscriptions = len(cl.Subscriptions())
	info.InflightOut = cl.OutboundLen()
	info.InflightIn = cl.InboundLen()
	return info
}

// ListConnections returns every registered client ordered by client ID.
func (s *Server) ListConnections() []ConnInfo {
	s.mu.RLock()
	conns := make([]*conn, 0, len(s.clients))
	for _, c := range s.clients {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	out := make([]ConnInfo, 0, len(conns))
	for _, c := range conns {
		out = append(out, s.connInfo(c))
	}
	sortConnInfo(out)
	return out
}

// ConnectionInfo returns the view of one client, ok=false when unknown.
func (s *Server) ConnectionInfo(clientID string) (ConnInfo, bool) {
	c := s.lookupConn(clientID)
	if c == nil || c.sessionClient() == nil {
		return ConnInfo{}, false
	}
	return s.connInfo(c), true
}

// KickConnection administratively disconnects a live client: a v5 client
// receives DISCONNECT 0x98 (Administrative Action) and the Will is
// suppressed; the session's expiry semantics are untouched. It reports
// whether a live connection was kicked.
func (s *Server) KickConnection(clientID string) bool {
	c := s.lookupConn(clientID)
	if c == nil || c.isStopped() {
		return false
	}
	if cl := c.sessionClient(); cl != nil && cl.ProtocolVersion == packet.Version5 {
		c.enqueue(&packet.Packet{
			FixedHeader:     packet.FixedHeader{Type: packet.Disconnect},
			ProtocolVersion: packet.Version5,
			ReasonCode:      packet.CodeAdministrativeAction,
		})
	}
	c.suppressWill.Store(true)
	c.stop()
	s.log.Info("connection kicked", "client_id", clientID)
	return true
}

// ClearSession removes one session entirely: the live connection (if any) is
// closed, subscriptions drop from the trie, pending Wills are cancelled and
// the persisted state is deleted. It reports whether a session existed.
func (s *Server) ClearSession(clientID string) bool {
	s.mu.Lock()
	c, ok := s.clients[clientID]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.clients, clientID)
	if cl := c.sessionClient(); cl != nil {
		s.removeSubscriptionsLocked(cl)
	}
	s.wills.Cancel(clientID)
	s.mu.Unlock()

	c.suppressWill.Store(true)
	c.stop()
	s.storeDeleteSessionState(clientID)
	s.log.Info("session cleared", "client_id", clientID)
	return true
}

// SubInfo is the management view of one subscription.
type SubInfo struct {
	ClientID string
	// Filter is the full on-the-wire filter, including any $share/{group}/
	// prefix.
	Filter            string
	Qos               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	Shared            string
}

// ListSubscriptions returns the subscriptions of every registered session,
// or of one client when clientID is non-empty, ordered by client ID then
// filter.
func (s *Server) ListSubscriptions(clientID string) []SubInfo {
	var out []SubInfo
	collect := func(c *conn) {
		cl := c.sessionClient()
		if cl == nil {
			return
		}
		for _, sub := range cl.Subscriptions() {
			filter := fullFilter(sub)
			out = append(out, SubInfo{
				ClientID:          cl.ID,
				Filter:            filter,
				Qos:               sub.Qos,
				NoLocal:           sub.NoLocal,
				RetainAsPublished: sub.RetainAsPublished,
				RetainHandling:    sub.RetainHandling,
				Shared:            sharedGroupOf(filter),
			})
		}
	}
	if clientID != "" {
		if c := s.lookupConn(clientID); c != nil {
			collect(c)
		}
		return out
	}
	s.mu.RLock()
	conns := make([]*conn, 0, len(s.clients))
	for _, c := range s.clients {
		conns = append(conns, c)
	}
	s.mu.RUnlock()
	for _, c := range conns {
		collect(c)
	}
	sortSubInfo(out)
	return out
}

// ListRetained returns every retained message sorted by topic name.
func (s *Server) ListRetained() []packet.Packet {
	return s.topics.RetainedMessages()
}

// DeleteRetained removes the retained message for one topic. A topic without
// a retained message is a no-op.
func (s *Server) DeleteRetained(topic string) error {
	if !topics.IsValidTopicName(topic) {
		return fmt.Errorf("broker: invalid topic name %q", topic)
	}
	// An empty payload deletes the retained message [MQTT-3.3.1-6].
	s.retain(packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
		TopicName:   topic,
	})
	return nil
}

// sortConnInfo orders by client ID.
func sortConnInfo(v []ConnInfo) {
	slices.SortFunc(v, func(a, b ConnInfo) int { return strings.Compare(a.ClientID, b.ClientID) })
}

// sortSubInfo orders by client ID then filter.
func sortSubInfo(v []SubInfo) {
	slices.SortFunc(v, func(a, b SubInfo) int {
		if c := strings.Compare(a.ClientID, b.ClientID); c != 0 {
			return c
		}
		return strings.Compare(a.Filter, b.Filter)
	})
}

// sharedGroupOf extracts the group name from a full "$share/{group}/{filter}"
// filter, empty for regular filters.
func sharedGroupOf(filter string) string {
	if !strings.HasPrefix(filter, "$share/") {
		return ""
	}
	rest := filter[len("$share/"):]
	if i := strings.IndexByte(rest, '/'); i > 0 {
		return rest[:i]
	}
	return ""
}

// SetChains swaps the authentication and authorization chains at runtime
// (PUT /api/v1/auth/chains); both fall back to the allow-all stub on nil.
func (s *Server) SetChains(authn hook.Authenticator, authz hook.Gate) {
	if authn == nil {
		authn = hook.NewAuthChain(hook.AllowAll{})
	}
	if authz == nil {
		authz = hook.NewAuthzChain(hook.AllowAll{})
	}
	s.mu.Lock()
	s.authn = authn
	s.authz = authz
	s.mu.Unlock()
}

// authenticator returns the current authentication chain.
func (s *Server) authenticator() hook.Authenticator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authn
}

// authorizer returns the current authorization chain.
func (s *Server) authorizer() hook.Gate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authz
}
