package session

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// TestConcurrentAllocateAck hammers the outbound window from many goroutines
// to surface data races and lost updates under -race.
func TestConcurrentAllocateAck(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "race"), Config{})
	now := time.Now()

	const workers = 16
	const perWorker = 500

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				send, err := c.SendPublish(now, publishPacket(0, 1, "race/topic"))
				require.NoError(t, err)
				if send == nil {
					// Window full: drain one message to make progress.
					continue
				}
				c.ReceivePuback(send.PacketID)
			}
		}()
	}
	// Concurrent raw allocate/free cycles alongside the publish flow.
	for range workers / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				id, err := c.AllocatePacketID()
				if err == nil {
					c.FreePacketID(id)
				}
			}
		}()
	}
	wg.Wait()

	// Every message that entered the window was acked; queued ones remain.
	assert.Equal(t, c.OutboundQueued(), c.OutboundLen())
	assert.Equal(t, c.OutboundLen(), c.PacketIDsInUse())
}

// TestConcurrentInboundQoS2 exercises the inbound QoS 2 window from many
// goroutines under -race.
func TestConcurrentInboundQoS2(t *testing.T) {
	c := NewClient(connectPacket(packet.Version5, true, "race-in"), Config{})
	now := time.Now()

	const workers = 16
	const perWorker = 200

	var wg sync.WaitGroup
	var mu sync.Mutex
	nextID := uint16(1)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				mu.Lock()
				id := nextID
				nextID++
				mu.Unlock()
				_, ack, err := c.ReceivePublish(now, publishPacket(id, 2, "race/in"))
				require.NoError(t, err)
				require.NotNil(t, ack)
				deliver, pubcomp := c.ReceivePubrel(id)
				require.NotNil(t, deliver)
				assert.Equal(t, packet.Pubcomp, pubcomp.FixedHeader.Type)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 0, c.InboundLen())
}

// TestConcurrentMisc covers subscriptions, aliases, takeover and expiry
// sweeps racing with the publish flows under -race.
func TestConcurrentMisc(t *testing.T) {
	cp := connectPacket(packet.Version5, true, "race-misc")
	cp.Properties.TopicAliasMaximumFlag = true
	cp.Properties.TopicAliasMaximum = 8
	c := NewClient(cp, Config{TopicAliasMaximum: 8})
	now := time.Now()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pk := publishPacket(1, 1, "misc/topic")
			pk.Properties.TopicAliasFlag = true
			pk.Properties.TopicAlias = uint16(i%8) + 1
			for range 100 {
				_, _ = c.ResolveInboundAlias(pk)
				_, _ = c.AssignOutboundAlias("misc/topic")
				c.AddSubscription(topicsSub(i))
				_ = c.Subscriptions()
				c.TakeOver()
				_ = c.TakenOver()
				c.MarkStopped(now)
				_ = c.DropExpiredOutbound(now)
				_ = c.DropExpiredInbound(now)
			}
		}(i)
	}
	wg.Wait()
}

func topicsSub(i int) topics.Subscription {
	return topics.Subscription{Filter: "misc/" + string(rune('a'+i)), Qos: 1}
}
