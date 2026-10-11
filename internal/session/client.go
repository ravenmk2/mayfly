// Package session implements the MQTT session state machine: client session
// lifecycle, Packet Identifier allocation, inflight windows with Receive
// Maximum flow control, the QoS 0/1/2 handshake state machines and topic
// alias bookkeeping. The package is a pure state machine: it never touches
// the network. Callers (the broker read/write loops) feed decoded packets in
// and write the returned packets out.
package session

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// DefaultReceiveMaximum applies to a direction when no Receive Maximum was
// negotiated (MQTT 3.1.1 connections and v5 connections without the
// property) [MQTT-3.1.2-11].
const DefaultReceiveMaximum = 65535

// Config carries the broker-side limits applied to a client session.
type Config struct {
	// ReceiveMaximum bounds concurrent unacknowledged QoS 1/2 PUBLISH
	// packets the client may send to the broker; it is the value the broker
	// advertises in CONNACK. Zero selects DefaultReceiveMaximum.
	ReceiveMaximum uint16
	// TopicAliasMaximum is the number of inbound topic aliases the broker
	// accepts from the client, advertised in CONNACK. Zero disables inbound
	// aliases.
	TopicAliasMaximum uint16
}

// Will holds the Will Message carried by CONNECT.
type Will struct {
	Topic   string
	Payload []byte
	Qos     byte
	Retain  bool
	// Properties carries the v5 Will Properties, including the Will Delay
	// Interval.
	Properties packet.Properties
}

// Client is the per-connection session state: identity, subscriptions,
// inflight windows and topic alias tables. Exported identity fields are
// immutable after construction; all mutable state is guarded by the
// embedded mutex and reachable only through methods.
type Client struct {
	mu sync.RWMutex

	// ID is the Client Identifier from CONNECT (or assigned by the broker).
	ID string
	// Username is the CONNECT User Name, empty when absent.
	Username string
	// ProtocolVersion is packet.Version311 or packet.Version5.
	ProtocolVersion byte
	// CleanStart is the v5 Clean Start / v3 Clean Session flag [MQTT-3.1.2-4].
	CleanStart bool
	// SessionExpiry is the effective Session Expiry Interval in seconds
	// (SessionExpiryNever for no expiry).
	SessionExpiry uint32
	// Keepalive is the negotiated Keep Alive in seconds.
	Keepalive uint16
	// Properties mirrors the v5 CONNECT properties (zero value for v3).
	Properties packet.Properties
	// Will is the Will Message, nil when the client did not set one.
	Will *Will

	// CreatedAt records when the session was first established (persistence
	// bookkeeping). The broker sets it via MarkCreated.
	CreatedAt time.Time

	subscriptions map[string]topics.Subscription

	inboundAliases   map[uint16]string // alias -> topic, client -> broker
	outboundAliases  map[string]uint16 // topic -> alias, broker -> client
	aliasCursor      uint16
	inboundAliasMax  uint16
	outboundAliasMax uint16

	ids      packetIDAllocator
	outbound outboundInflight
	inbound  inboundInflight

	stoppedAt time.Time

	// takenOver marks that a newer connection with the same client ID
	// replaced this session; the old connection's cleanup must not delete
	// the migrated session state.
	takenOver atomic.Bool
}

// NewClient builds a session from a decoded CONNECT packet and the broker
// configuration.
func NewClient(connect *packet.Packet, cfg Config) *Client {
	cp := connect.Connect
	version := cp.ProtocolVersion
	if version == 0 {
		version = packet.Version311
	}
	outboundMax := uint16(DefaultReceiveMaximum)
	var outboundAliasMax uint16
	if version == packet.Version5 {
		// The client's Receive Maximum bounds the broker -> client window;
		// its absence means 65535 [MQTT-3.1.2-11].
		if connect.Properties.ReceiveMaximumFlag {
			outboundMax = connect.Properties.ReceiveMaximum
		}
		if connect.Properties.TopicAliasMaximumFlag {
			outboundAliasMax = connect.Properties.TopicAliasMaximum
		}
	}
	inboundMax := cfg.ReceiveMaximum
	if inboundMax == 0 {
		inboundMax = DefaultReceiveMaximum
	}
	c := &Client{
		ID:               cp.ClientIdentifier,
		Username:         cp.Username,
		ProtocolVersion:  version,
		CleanStart:       cp.Clean,
		SessionExpiry:    EffectiveSessionExpiry(version, cp.Clean, connect.Properties),
		Keepalive:        cp.Keepalive,
		subscriptions:    make(map[string]topics.Subscription),
		inboundAliases:   make(map[uint16]string),
		outboundAliases:  make(map[string]uint16),
		inboundAliasMax:  cfg.TopicAliasMaximum,
		outboundAliasMax: outboundAliasMax,
		outbound:         newOutboundInflight(outboundMax),
		inbound:          newInboundInflight(inboundMax),
	}
	if version == packet.Version5 {
		c.Properties = connect.Properties
	}
	if cp.WillFlag {
		c.Will = &Will{
			Topic:      cp.WillTopic,
			Payload:    cp.WillPayload,
			Qos:        cp.WillQos,
			Retain:     cp.WillRetain,
			Properties: cp.WillProperties,
		}
	}
	return c
}

// TakeOver marks the session as replaced by a newer connection with the same
// client ID. The broker sends the old connection a DISCONNECT with reason
// 0x8E (Session taken over) and skips session cleanup for the old
// connection.
func (c *Client) TakeOver() {
	c.takenOver.Store(true)
}

// TakenOver reports whether the session was replaced by a newer connection.
func (c *Client) TakenOver() bool {
	return c.takenOver.Load()
}

// MarkCreated records when the session was first established. It is set
// once at CONNECT time and migrates with the session on takeover.
func (c *Client) MarkCreated(t time.Time) {
	c.mu.Lock()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = t
	}
	c.mu.Unlock()
}

// MarkStopped records the disconnect time that ExpiryDeadline works from.
func (c *Client) MarkStopped(t time.Time) {
	c.mu.Lock()
	c.stoppedAt = t
	c.mu.Unlock()
}

// StoppedAt returns the recorded disconnect time.
func (c *Client) StoppedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stoppedAt
}

// ExpiryDeadline returns when the stored session must be discarded; ok is
// false when the session never expires.
func (c *Client) ExpiryDeadline() (deadline time.Time, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return SessionExpiryDeadline(c.SessionExpiry, c.stoppedAt)
}

// KeepaliveTimeout returns how long the read loop may wait for the next
// packet from this client: one and a half times the Keep Alive. A Keep Alive
// of 0 disables the check [MQTT-3.1.2-24].
func (c *Client) KeepaliveTimeout() time.Duration {
	if c.Keepalive == 0 {
		return 0
	}
	return time.Duration(c.Keepalive) * time.Second * 3 / 2
}

// SubscriptionKey identifies a subscription within the session's list. The
// NUL separator cannot appear in topic filters or group names [MQTT-1.5.4-1].
func SubscriptionKey(sub topics.Subscription) string {
	return sub.Shared + "\x00" + sub.Filter
}

// AddSubscription records a subscription in the session's list, replacing
// any existing entry with the same key [MQTT-3.8.4-2].
func (c *Client) AddSubscription(sub topics.Subscription) {
	c.mu.Lock()
	c.subscriptions[SubscriptionKey(sub)] = sub
	c.mu.Unlock()
}

// RemoveSubscription drops the subscription with the given key, reporting
// whether it existed.
func (c *Client) RemoveSubscription(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.subscriptions[key]; !ok {
		return false
	}
	delete(c.subscriptions, key)
	return true
}

// Subscriptions returns a copy of the session's subscription list, sorted by
// key for deterministic persistence and session migration.
func (c *Client) Subscriptions() []topics.Subscription {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]topics.Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		out = append(out, sub)
	}
	slices.SortFunc(out, func(a, b topics.Subscription) int {
		return cmp.Compare(SubscriptionKey(a), SubscriptionKey(b))
	})
	return out
}
