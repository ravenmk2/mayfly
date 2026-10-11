// Package store implements broker persistence (docs/design/persistence.md):
// sessions, subscriptions, inflight messages and retained messages behind a
// narrow Store interface. The in-memory broker state stays authoritative;
// the Store is the crash-recovery replica written asynchronously through a
// BatchWriter. Implementations: MemoryStore (tests and memory-only runs) and
// SQLStore (MySQL and SQLite sharing one SQL logic).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// Direction distinguishes the two inflight windows of a session.
type Direction byte

const (
	// Inbound is the client -> broker window: QoS 2 PUBLISH packets stored
	// until their PUBREL arrives.
	Inbound Direction = iota
	// Outbound is the broker -> client window: QoS 1/2 messages awaiting
	// acknowledgement, offline-queue entries included.
	Outbound
)

// Session is the persisted form of one client session.
type Session struct {
	ClientID string
	// Expiry is the Session Expiry Interval in seconds (0xFFFFFFFF = never).
	Expiry uint32
	// CreatedAt records when the session was first established.
	CreatedAt time.Time
	// StoppedAt is the disconnect time; the zero value means the session was
	// connected when it was written.
	StoppedAt time.Time
}

// Subscription is the persisted form of one client subscription. Filter is
// the full on-the-wire filter including any $share/{group}/ prefix.
type Subscription struct {
	ClientID          string
	Filter            string
	Qos               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	// Identifier is the v5 Subscription Identifier; 0 means none (0 is not a
	// valid identifier on the wire).
	Identifier uint32
}

// InflightMessage is the persisted form of one unacknowledged QoS 1/2
// message in either window.
type InflightMessage struct {
	ClientID  string
	PacketID  uint16
	Direction Direction
	// Packet is the full PUBLISH packet; for Outbound it carries the
	// assigned Packet Identifier.
	Packet packet.Packet
	// Sent marks outbound messages already transmitted (DUP-resend on
	// resume); false while queued behind the Receive Maximum window or held
	// in the offline queue. Unused for Inbound.
	Sent bool
	// Pubrel marks outbound QoS 2 messages whose handshake advanced to the
	// PUBREL stage. Unused for Inbound.
	Pubrel bool
	// Expiry is the absolute Message Expiry deadline; zero means no expiry.
	Expiry time.Time
}

// RetainedMessage is the persisted form of one retained publication.
type RetainedMessage struct {
	Topic  string
	Packet packet.Packet
	// Expiry is the absolute Message Expiry deadline; zero means no expiry.
	Expiry time.Time
}

// SessionStore persists client sessions. Delete is idempotent.
type SessionStore interface {
	Put(ctx context.Context, s Session) error
	Delete(ctx context.Context, clientID string) error
	Get(ctx context.Context, clientID string) (Session, bool, error)
	// Iterate calls fn for every stored session until fn returns false.
	Iterate(ctx context.Context, fn func(Session) bool) error
}

// SubscriptionStore persists subscriptions keyed by (client, filter).
type SubscriptionStore interface {
	Put(ctx context.Context, sub Subscription) error
	Delete(ctx context.Context, clientID, filter string) error
	DeleteByClient(ctx context.Context, clientID string) error
	Iterate(ctx context.Context, fn func(Subscription) bool) error
}

// InflightStore persists inflight messages keyed by (client, direction,
// packet identifier).
type InflightStore interface {
	Put(ctx context.Context, msg InflightMessage) error
	Delete(ctx context.Context, clientID string, direction Direction, packetID uint16) error
	DeleteByClient(ctx context.Context, clientID string) error
	Iterate(ctx context.Context, fn func(InflightMessage) bool) error
}

// RetainedStore persists retained messages keyed by topic.
type RetainedStore interface {
	Put(ctx context.Context, msg RetainedMessage) error
	Delete(ctx context.Context, topic string) error
	Iterate(ctx context.Context, fn func(RetainedMessage) bool) error
}

// Store is the narrow persistence interface, decoupled from the hook event
// stream (docs/design/persistence.md section 2).
type Store interface {
	Sessions() SessionStore
	Subscriptions() SubscriptionStore
	Inflight() InflightStore
	Retained() RetainedStore
	Close() error
}

// encodePacket serializes pk with a one-byte protocol version prefix so the
// exact codec dialect (v5 properties included) survives the round trip. A
// zero ProtocolVersion is normalized to Version5 so v5 properties of
// broker-built packets (e.g. retained Will Messages) are never dropped.
func encodePacket(pk packet.Packet) ([]byte, error) {
	version := pk.ProtocolVersion
	if version == 0 {
		version = packet.Version5
	}
	cp := pk
	cp.ProtocolVersion = version
	raw, err := cp.Encode()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(raw)+1)
	out = append(out, version)
	return append(out, raw...), nil
}

// decodePacket deserializes a packet written by encodePacket.
func decodePacket(b []byte) (packet.Packet, error) {
	if len(b) < 2 {
		return packet.Packet{}, errors.New("store: persisted packet truncated")
	}
	pk := packet.Packet{ProtocolVersion: b[0]}
	if err := pk.Decode(b[1:]); err != nil {
		return packet.Packet{}, fmt.Errorf("store: decode persisted packet: %w", err)
	}
	return pk, nil
}
