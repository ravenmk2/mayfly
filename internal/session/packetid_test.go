package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestPacketIDSequentialAllocation(t *testing.T) {
	var a packetIDAllocator
	a.cursor = 1
	seen := make(map[uint16]bool)
	for range 100 {
		id, ok := a.allocate()
		require.True(t, ok)
		assert.NotZero(t, id)
		assert.False(t, seen[id], "identifier %d handed out twice", id)
		seen[id] = true
	}
	assert.Equal(t, 100, a.count)
}

func TestPacketIDExhaustion(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, false, "exhaust"), Config{})
	for range maxPacketID {
		_, err := c.AllocatePacketID()
		require.NoError(t, err)
	}
	assert.Equal(t, maxPacketID, c.PacketIDsInUse())

	// All 65535 identifiers in use: allocation fails.
	_, err := c.AllocatePacketID()
	require.Error(t, err)
	code, ok := err.(packet.Code)
	require.True(t, ok)
	assert.Equal(t, packet.CodePacketIdentifierInUse, code.Code)

	// Freeing one identifier makes exactly one available again.
	c.FreePacketID(42)
	id, err := c.AllocatePacketID()
	require.NoError(t, err)
	assert.Equal(t, uint16(42), id)
	_, err = c.AllocatePacketID()
	require.Error(t, err)
}

func TestPacketIDWrapAroundSkipsUsed(t *testing.T) {
	var a packetIDAllocator
	a.cursor = 1
	// Fill everything, then free a sparse set; allocation must wrap and skip.
	for range maxPacketID {
		_, ok := a.allocate()
		require.True(t, ok)
	}
	a.free(1)
	a.free(65535)
	a.free(30000)

	got := make(map[uint16]bool)
	for range 3 {
		id, ok := a.allocate()
		require.True(t, ok)
		got[id] = true
	}
	assert.Equal(t, map[uint16]bool{1: true, 30000: true, 65535: true}, got)
	// The next allocation wraps past the re-taken tail and fails.
	_, ok := a.allocate()
	assert.False(t, ok)
}

func TestPacketIDReserveAndFree(t *testing.T) {
	c := NewClient(connectPacket(packet.Version311, false, "reserve"), Config{})

	require.NoError(t, c.ReservePacketID(7))
	// Double reservation fails.
	err := c.ReservePacketID(7)
	require.Error(t, err)
	assert.Equal(t, packet.CodePacketIdentifierInUse, err.(packet.Code).Code)
	// Identifier 0 is reserved by the protocol [MQTT-2.2.1-2].
	require.Error(t, c.ReservePacketID(0))

	// Allocation skips the reserved identifier.
	for range 20 {
		id, err := c.AllocatePacketID()
		require.NoError(t, err)
		assert.NotEqual(t, uint16(7), id)
	}

	// Freeing twice is a no-op.
	c.FreePacketID(7)
	c.FreePacketID(7)
	assert.Equal(t, 20, c.PacketIDsInUse())
	c.FreePacketID(0)
	assert.Equal(t, 20, c.PacketIDsInUse())
}

func TestPacketIDAllocatorInUse(t *testing.T) {
	var a packetIDAllocator
	assert.False(t, a.inUse(0))
	assert.False(t, a.inUse(9))
	require.True(t, a.take(9))
	assert.True(t, a.inUse(9))
	a.free(9)
	assert.False(t, a.inUse(9))
}
