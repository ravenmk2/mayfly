package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// runContractTests exercises the Store contract against every implementation:
// put/overwrite, idempotent delete, Get miss/hit and Iterate over empty and
// large result sets with early stop.
func runContractTests(t *testing.T, factory func(t *testing.T) Store) {
	t.Helper()

	newStore := func(t *testing.T) Store {
		t.Helper()
		s := factory(t)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("SessionPutGetDelete", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_, ok, err := s.Sessions().Get(ctx, "c1")
		require.NoError(t, err)
		assert.False(t, ok)

		created := time.Now().Truncate(time.Millisecond)
		sess := Session{ClientID: "c1", Expiry: 300, CreatedAt: created}
		require.NoError(t, s.Sessions().Put(ctx, sess))
		got, ok, err := s.Sessions().Get(ctx, "c1")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "c1", got.ClientID)
		assert.Equal(t, uint32(300), got.Expiry)
		assert.True(t, created.Equal(got.CreatedAt))
		assert.True(t, got.StoppedAt.IsZero())

		// Overwrite: disconnect marks StoppedAt.
		stopped := created.Add(time.Minute)
		sess.StoppedAt = stopped
		require.NoError(t, s.Sessions().Put(ctx, sess))
		got, ok, err = s.Sessions().Get(ctx, "c1")
		require.NoError(t, err)
		require.True(t, ok)
		assert.True(t, stopped.Equal(got.StoppedAt))

		require.NoError(t, s.Sessions().Delete(ctx, "c1"))
		_, ok, err = s.Sessions().Get(ctx, "c1")
		require.NoError(t, err)
		assert.False(t, ok)
		// Delete is idempotent.
		require.NoError(t, s.Sessions().Delete(ctx, "c1"))
	})

	t.Run("SessionIterate", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		// Empty store iterates zero entries.
		require.NoError(t, s.Sessions().Iterate(ctx, func(Session) bool {
			t.Fatal("unexpected entry")
			return true
		}))

		require.NoError(t, s.Sessions().Put(ctx, Session{ClientID: "b", Expiry: 1}))
		require.NoError(t, s.Sessions().Put(ctx, Session{ClientID: "a", Expiry: 1}))
		var ids []string
		require.NoError(t, s.Sessions().Iterate(ctx, func(s Session) bool {
			ids = append(ids, s.ClientID)
			return true
		}))
		assert.Equal(t, []string{"a", "b"}, ids)

		// Early stop.
		count := 0
		require.NoError(t, s.Sessions().Iterate(ctx, func(Session) bool {
			count++
			return false
		}))
		assert.Equal(t, 1, count)
	})

	t.Run("SubscriptionRoundTrip", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		sub := Subscription{
			ClientID: "c1", Filter: "sensors/+", Qos: 1,
			NoLocal: true, RetainAsPublished: true, RetainHandling: 1, Identifier: 42,
		}
		require.NoError(t, s.Subscriptions().Put(ctx, sub))
		shared := Subscription{ClientID: "c1", Filter: "$share/g/a/#", Qos: 2}
		require.NoError(t, s.Subscriptions().Put(ctx, shared))
		// Overwrite same key.
		sub.Qos = 2
		require.NoError(t, s.Subscriptions().Put(ctx, sub))

		var got []Subscription
		require.NoError(t, s.Subscriptions().Iterate(ctx, func(x Subscription) bool {
			got = append(got, x)
			return true
		}))
		require.Len(t, got, 2)
		byFilter := map[string]Subscription{}
		for _, g := range got {
			byFilter[g.Filter] = g
		}
		assert.Equal(t, sub, byFilter["sensors/+"])
		assert.Equal(t, shared, byFilter["$share/g/a/#"])

		require.NoError(t, s.Subscriptions().Delete(ctx, "c1", "sensors/+"))
		require.NoError(t, s.Subscriptions().Delete(ctx, "c1", "sensors/+")) // idempotent
		got = got[:0]
		require.NoError(t, s.Subscriptions().Iterate(ctx, func(x Subscription) bool {
			got = append(got, x)
			return true
		}))
		require.Len(t, got, 1)
		assert.Equal(t, "$share/g/a/#", got[0].Filter)

		// DeleteByClient drops the remainder, unknown client included.
		require.NoError(t, s.Subscriptions().DeleteByClient(ctx, "nobody"))
		require.NoError(t, s.Subscriptions().DeleteByClient(ctx, "c1"))
		require.NoError(t, s.Subscriptions().Iterate(ctx, func(Subscription) bool {
			t.Fatal("unexpected entry")
			return true
		}))
	})

	t.Run("SubscriptionIterateLarge", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		const n = 2000
		for i := 0; i < n; i++ {
			require.NoError(t, s.Subscriptions().Put(ctx, Subscription{
				ClientID: "bulk", Filter: fmt.Sprintf("t/%04d", i), Qos: 1,
			}))
		}
		count := 0
		require.NoError(t, s.Subscriptions().Iterate(ctx, func(x Subscription) bool {
			assert.Equal(t, "bulk", x.ClientID)
			count++
			return true
		}))
		assert.Equal(t, n, count)
	})

	t.Run("InflightRoundTrip", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		outMsg := InflightMessage{
			ClientID: "c1", PacketID: 7, Direction: Outbound,
			Packet: packet.Packet{
				FixedHeader:     packet.FixedHeader{Type: packet.Publish, Qos: 2},
				ProtocolVersion: packet.Version5,
				PacketID:        7,
				TopicName:       "a/b",
				Payload:         []byte("payload"),
				Properties:      packet.Properties{ContentType: "text", ContentTypeFlag: true},
			},
			Sent:   true,
			Pubrel: true,
			Expiry: time.Now().Add(time.Hour).Truncate(time.Millisecond),
		}
		require.NoError(t, s.Inflight().Put(ctx, outMsg))
		inMsg := InflightMessage{
			ClientID: "c1", PacketID: 3, Direction: Inbound,
			Packet: packet.Packet{
				FixedHeader:     packet.FixedHeader{Type: packet.Publish, Qos: 2},
				ProtocolVersion: packet.Version311,
				PacketID:        3,
				TopicName:       "in/topic",
				Payload:         []byte("in"),
			},
		}
		require.NoError(t, s.Inflight().Put(ctx, inMsg))
		// Same packet identifier in the other direction is a distinct row.
		dupID := outMsg
		dupID.Direction = Inbound
		require.NoError(t, s.Inflight().Put(ctx, dupID))
		// Overwrite: QoS 2 handshake advancing to the PUBREL stage rewrites.
		outMsg.Pubrel = false
		require.NoError(t, s.Inflight().Put(ctx, outMsg))

		var got []InflightMessage
		require.NoError(t, s.Inflight().Iterate(ctx, func(m InflightMessage) bool {
			got = append(got, m)
			return true
		}))
		require.Len(t, got, 3)
		byKey := map[string]InflightMessage{}
		for _, m := range got {
			byKey[fmt.Sprintf("%s/%d/%d", m.ClientID, m.Direction, m.PacketID)] = m
		}
		restored := byKey[fmt.Sprintf("%s/%d/%d", "c1", Outbound, 7)]
		assert.False(t, restored.Pubrel) // overwrite won
		assert.True(t, restored.Sent)
		assert.Equal(t, "a/b", restored.Packet.TopicName)
		assert.Equal(t, []byte("payload"), restored.Packet.Payload)
		assert.Equal(t, "text", restored.Packet.Properties.ContentType)
		assert.Equal(t, packet.Version5, restored.Packet.ProtocolVersion)
		assert.True(t, outMsg.Expiry.Equal(restored.Expiry))
		in := byKey[fmt.Sprintf("%s/%d/%d", "c1", Inbound, 3)]
		assert.Equal(t, packet.Version311, in.Packet.ProtocolVersion)

		require.NoError(t, s.Inflight().Delete(ctx, "c1", Outbound, 7))
		require.NoError(t, s.Inflight().Delete(ctx, "c1", Outbound, 7)) // idempotent
		require.NoError(t, s.Inflight().DeleteByClient(ctx, "c1"))
		require.NoError(t, s.Inflight().Iterate(ctx, func(InflightMessage) bool {
			t.Fatal("unexpected entry")
			return true
		}))
	})

	t.Run("RetainedRoundTrip", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		msg := RetainedMessage{
			Topic: "retain/a",
			Packet: packet.Packet{
				FixedHeader:     packet.FixedHeader{Type: packet.Publish, Qos: 1, Retain: true},
				ProtocolVersion: packet.Version5,
				TopicName:       "retain/a",
				Payload:         []byte("v1"),
				Properties:      packet.Properties{PayloadFormat: 1, PayloadFormatFlag: true},
			},
		}
		require.NoError(t, s.Retained().Put(ctx, msg))
		exp := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		msg.Packet.Payload = []byte("v2")
		msg.Expiry = exp
		require.NoError(t, s.Retained().Put(ctx, msg)) // overwrite

		var got []RetainedMessage
		require.NoError(t, s.Retained().Iterate(ctx, func(m RetainedMessage) bool {
			got = append(got, m)
			return true
		}))
		require.Len(t, got, 1)
		assert.Equal(t, []byte("v2"), got[0].Packet.Payload)
		assert.True(t, got[0].Packet.FixedHeader.Retain)
		assert.Equal(t, byte(1), got[0].Packet.Properties.PayloadFormat)
		assert.True(t, exp.Equal(got[0].Expiry))

		require.NoError(t, s.Retained().Delete(ctx, "retain/a"))
		require.NoError(t, s.Retained().Delete(ctx, "retain/a")) // idempotent
		require.NoError(t, s.Retained().Iterate(ctx, func(RetainedMessage) bool {
			t.Fatal("unexpected entry")
			return true
		}))
	})
}

func TestMemoryStoreContract(t *testing.T) {
	runContractTests(t, func(t *testing.T) Store {
		t.Helper()
		return NewMemory()
	})
}

func TestPacketCodecRoundTrip(t *testing.T) {
	original := packet.Packet{
		FixedHeader:     packet.FixedHeader{Type: packet.Publish, Qos: 1, Retain: true},
		ProtocolVersion: packet.Version5,
		PacketID:        9,
		TopicName:       "codec/test",
		Payload:         []byte("data"),
		Properties: packet.Properties{
			ContentType: "bin", ContentTypeFlag: true,
			MessageExpiry: 60, MessageExpiryFlag: true,
		},
	}
	b, err := encodePacket(original)
	require.NoError(t, err)
	got, err := decodePacket(b)
	require.NoError(t, err)
	assert.Equal(t, original.ProtocolVersion, got.ProtocolVersion)
	assert.Equal(t, original.TopicName, got.TopicName)
	assert.Equal(t, original.Payload, got.Payload)
	assert.Equal(t, original.PacketID, got.PacketID)
	assert.Equal(t, "bin", got.Properties.ContentType)
	assert.Equal(t, uint32(60), got.Properties.MessageExpiry)

	// A zero protocol version normalizes to v5 so properties survive.
	zero := packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
		TopicName:   "codec/zero",
		Payload:     []byte("z"),
		Properties:  packet.Properties{ContentType: "x", ContentTypeFlag: true},
	}
	b, err = encodePacket(zero)
	require.NoError(t, err)
	got, err = decodePacket(b)
	require.NoError(t, err)
	assert.Equal(t, "x", got.Properties.ContentType)

	_, err = decodePacket(nil)
	assert.Error(t, err)
}
