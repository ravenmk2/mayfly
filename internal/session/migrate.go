package session

// MigrateFrom transfers the durable session state of old into c: the
// subscription list, the outbound inflight window (queued overflow
// included), pending inbound QoS 2 messages and Packet Identifier occupancy.
// The broker calls it when a client reconnects with Clean Start 0 and the
// broker still holds its session, so unacknowledged QoS 1/2 state survives
// the reconnection. Topic alias tables are connection-scoped [MQTT-3.3.2-6]
// and are not migrated. The flow control limits negotiated by the new
// connection replace the old ones.
func (c *Client) MigrateFrom(old *Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old.mu.Lock()
	defer old.mu.Unlock()
	for k, sub := range old.subscriptions {
		c.subscriptions[k] = sub
	}
	c.CreatedAt = old.CreatedAt
	outMax, inMax := c.outbound.max, c.inbound.max
	c.ids = old.ids
	c.outbound = old.outbound
	c.inbound = old.inbound
	c.outbound.max, c.inbound.max = outMax, inMax
}

// SetSessionExpiry updates the Session Expiry Interval, e.g. when a v5
// DISCONNECT carries an override [MQTT-3.1.2-11].
func (c *Client) SetSessionExpiry(v uint32) {
	c.mu.Lock()
	c.SessionExpiry = v
	c.mu.Unlock()
}

// Expiry returns the current Session Expiry Interval in seconds.
func (c *Client) Expiry() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.SessionExpiry
}
