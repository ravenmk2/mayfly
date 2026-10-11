package session

import (
	"github.com/ravenmk2/mayfly/internal/packet"
)

// ResolveInboundAlias processes the v5 Topic Alias property of an inbound
// PUBLISH and returns the effective topic name. A PUBLISH carrying both a
// topic name and an alias (re)registers the mapping; a PUBLISH with an empty
// topic name resolves the registered mapping [MQTT-3.3.2-9] [MQTT-3.3.2-12].
func (c *Client) ResolveInboundAlias(pk *packet.Packet) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !pk.Properties.TopicAliasFlag {
		return pk.TopicName, nil
	}
	alias := pk.Properties.TopicAlias
	if alias == 0 || alias > c.inboundAliasMax {
		// The alias is 0 or exceeds the Topic Alias Maximum the broker
		// advertised in CONNACK [MQTT-3.3.2-8].
		return "", packet.Code{Code: packet.CodeTopicAliasInvalid,
			Reason: "topic alias is 0 or exceeds the topic alias maximum"}
	}
	if pk.TopicName != "" {
		c.inboundAliases[alias] = pk.TopicName
		return pk.TopicName, nil
	}
	topic, ok := c.inboundAliases[alias]
	if !ok {
		// An alias without a registered mapping is a Protocol Error
		// [MQTT-3.3.2-11].
		return "", packet.Code{Code: packet.CodeProtocolError,
			Reason: "topic alias has no registered topic name"}
	}
	return topic, nil
}

// AssignOutboundAlias returns the topic alias to use for an outbound PUBLISH
// to this client, allocating a fresh one while the client's Topic Alias
// Maximum permits. ok is false when the client accepts no aliases or all
// alias values are taken; the caller then sends the full topic name.
func (c *Client) AssignOutboundAlias(topic string) (alias uint16, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.outboundAliasMax == 0 {
		return 0, false
	}
	if alias, ok := c.outboundAliases[topic]; ok {
		return alias, true
	}
	if c.aliasCursor >= c.outboundAliasMax {
		return 0, false
	}
	c.aliasCursor++
	c.outboundAliases[topic] = c.aliasCursor
	return c.aliasCursor, true
}
