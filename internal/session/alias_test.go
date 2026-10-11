package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestResolveInboundAliasRegisterAndResolve(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "alias"), Config{TopicAliasMaximum: 4})

	// No alias property: the topic passes through unchanged.
	topic, err := c.ResolveInboundAlias(publishPacket(1, 1, "plain/topic"))
	require.NoError(t, err)
	assert.Equal(t, "plain/topic", topic)

	// Topic plus alias registers the mapping.
	pk := publishPacket(2, 1, "sensors/temp")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 1
	topic, err = c.ResolveInboundAlias(pk)
	require.NoError(t, err)
	assert.Equal(t, "sensors/temp", topic)

	// Empty topic plus alias resolves the registered mapping [MQTT-3.3.2-12].
	pk = publishPacket(3, 1, "")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 1
	topic, err = c.ResolveInboundAlias(pk)
	require.NoError(t, err)
	assert.Equal(t, "sensors/temp", topic)

	// A topic name with the same alias overwrites the mapping.
	pk = publishPacket(4, 1, "sensors/humidity")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 1
	_, err = c.ResolveInboundAlias(pk)
	require.NoError(t, err)
	pk = publishPacket(5, 1, "")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 1
	topic, err = c.ResolveInboundAlias(pk)
	require.NoError(t, err)
	assert.Equal(t, "sensors/humidity", topic)
}

func TestResolveInboundAliasErrors(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "alias-err"), Config{TopicAliasMaximum: 2})

	// Alias 0 is invalid [MQTT-3.3.2-8].
	pk := publishPacket(1, 1, "a")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 0
	_, err := c.ResolveInboundAlias(pk)
	require.Error(t, err)
	assert.Equal(t, packet.CodeTopicAliasInvalid, err.(packet.Code).Code)

	// Beyond the advertised maximum.
	pk = publishPacket(2, 1, "a")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 3
	_, err = c.ResolveInboundAlias(pk)
	require.Error(t, err)
	assert.Equal(t, packet.CodeTopicAliasInvalid, err.(packet.Code).Code)

	// Unknown alias with an empty topic is a Protocol Error [MQTT-3.3.2-11].
	pk = publishPacket(3, 1, "")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 2
	_, err = c.ResolveInboundAlias(pk)
	require.Error(t, err)
	assert.Equal(t, packet.CodeProtocolError, err.(packet.Code).Code)
}

func TestResolveInboundAliasDisabled(t *testing.T) {
	// TopicAliasMaximum 0 means the broker accepts no aliases.
	c := NewClient(connectPacket(packet.Version5, true, "alias-off"), Config{})
	pk := publishPacket(1, 1, "a")
	pk.Properties.TopicAliasFlag = true
	pk.Properties.TopicAlias = 1
	_, err := c.ResolveInboundAlias(pk)
	require.Error(t, err)
	assert.Equal(t, packet.CodeTopicAliasInvalid, err.(packet.Code).Code)
}

func TestAssignOutboundAlias(t *testing.T) {
	cp := connectPacket(packet.Version5, true, "alias-out")
	cp.Properties.TopicAliasMaximumFlag = true
	cp.Properties.TopicAliasMaximum = 2
	c := NewClient(cp, Config{})

	// Fresh topics allocate sequential aliases.
	alias, ok := c.AssignOutboundAlias("a/b")
	require.True(t, ok)
	assert.Equal(t, uint16(1), alias)
	alias, ok = c.AssignOutboundAlias("c/d")
	require.True(t, ok)
	assert.Equal(t, uint16(2), alias)

	// Known topics reuse their alias.
	alias, ok = c.AssignOutboundAlias("a/b")
	require.True(t, ok)
	assert.Equal(t, uint16(1), alias)

	// The table is full: unknown topics fall back to the full topic name.
	_, ok = c.AssignOutboundAlias("e/f")
	assert.False(t, ok)
}

func TestAssignOutboundAliasUnsupported(t *testing.T) {
	// A v5 client that sent no Topic Alias Maximum accepts no aliases.
	c := NewClient(connectPacket(packet.Version5, true, "alias-none"), Config{})
	_, ok := c.AssignOutboundAlias("a/b")
	assert.False(t, ok)

	// MQTT 3.1.1 has no topic aliases at all.
	c3 := NewClient(connectPacket(packet.Version311, true, "alias-v3"), Config{})
	_, ok = c3.AssignOutboundAlias("a/b")
	assert.False(t, ok)
}
