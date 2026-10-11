package session

import "github.com/ravenmk2/mayfly/internal/packet"

// maxPacketID is the highest valid Packet Identifier: identifiers run 1 to
// 65535 and zero is reserved [MQTT-2.2.1-2].
const maxPacketID = 65535

// bitmapWords covers identifiers 1..maxPacketID with one bit each.
const bitmapWords = (maxPacketID + 63) / 64

// packetIDAllocator hands out Packet Identifiers for outbound QoS 1/2
// messages using a rotating cursor over an occupancy bitmap: amortized O(1)
// allocation that wraps around and skips in-use identifiers instead of
// linearly scanning from 1 every time.
type packetIDAllocator struct {
	cursor uint16 // next candidate, always 1..maxPacketID
	used   [bitmapWords]uint64
	count  int
}

// allocate takes the next free identifier, returning ok=false when all
// 65535 identifiers are in use.
func (a *packetIDAllocator) allocate() (uint16, bool) {
	if a.count == maxPacketID {
		return 0, false
	}
	for {
		id := a.cursor
		a.cursor++
		if a.cursor == 0 { // wrapped past 65535
			a.cursor = 1
		}
		if a.take(id) {
			return id, true
		}
	}
}

// reserve marks a specific identifier as used, reporting success. Identifier
// 0 is invalid [MQTT-2.2.1-2].
func (a *packetIDAllocator) reserve(id uint16) bool {
	return a.take(id)
}

// take marks id used when it is free, reporting whether it was taken.
func (a *packetIDAllocator) take(id uint16) bool {
	if id == 0 {
		return false
	}
	word, bit := (id-1)/64, (id-1)%64
	mask := uint64(1) << bit
	if a.used[word]&mask != 0 {
		return false
	}
	a.used[word] |= mask
	a.count++
	return true
}

// free releases an identifier. Freeing an unused identifier or zero is a
// no-op.
func (a *packetIDAllocator) free(id uint16) {
	if id == 0 {
		return
	}
	word, bit := (id-1)/64, (id-1)%64
	mask := uint64(1) << bit
	if a.used[word]&mask != 0 {
		a.used[word] &^= mask
		a.count--
	}
}

// inUse reports whether the identifier is currently allocated.
func (a *packetIDAllocator) inUse(id uint16) bool {
	if id == 0 {
		return false
	}
	word, bit := (id-1)/64, (id-1)%64
	return a.used[word]&(uint64(1)<<bit) != 0
}

// AllocatePacketID takes the next free Packet Identifier for an outbound
// QoS 1/2 message [MQTT-2.2.1-2]. It fails when all 65535 identifiers are
// in use.
func (c *Client) AllocatePacketID() (uint16, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.ids.allocate()
	if !ok {
		return 0, packet.Code{Code: packet.CodePacketIdentifierInUse,
			Reason: "all 65535 packet identifiers are in use"}
	}
	return id, nil
}

// ReservePacketID marks a specific identifier as used, failing when it is
// already taken or zero.
func (c *Client) ReservePacketID(id uint16) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.ids.reserve(id) {
		return packet.Code{Code: packet.CodePacketIdentifierInUse,
			Reason: "packet identifier is 0 or already in use"}
	}
	return nil
}

// FreePacketID releases an identifier back to the pool.
func (c *Client) FreePacketID(id uint16) {
	c.mu.Lock()
	c.ids.free(id)
	c.mu.Unlock()
}

// PacketIDsInUse reports how many identifiers are currently allocated.
func (c *Client) PacketIDsInUse() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ids.count
}
