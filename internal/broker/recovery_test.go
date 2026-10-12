package broker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/store"
)

// waitForCondition polls cond until it holds, failing the test after 3s.
func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestCrashRecoveryEquivalence simulates a broker crash and restart over the
// same database: a persistent session with a subscription, an unacknowledged
// outbound QoS 1 message, a QoS 2 message mid-handshake (PUBREL stage), a
// message queued while the client was offline and a retained publication all
// survive, and the resuming client sees exactly the state the pre-crash
// broker held.
func TestCrashRecoveryEquivalence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "mayfly.db")

	// Phase 1: build up state, then "crash" (Close drains the batch writer,
	// so every write has landed in the database).
	s1, closeStore1 := newStoreServer(t, dbPath)
	sub := dial(t, s1).v5()
	ack := sub.connect("sub", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	require.False(t, ack.SessionPresent)
	suback := sub.subscribe(1, packet.Subscription{Filter: "crash/#", Qos: 2})
	require.Equal(t, []byte{packet.CodeGrantedQos2}, suback.ReasonCodes)

	pub := dial(t, s1).v5()
	pub.connect("pub", true, packet.Properties{})

	// Outbound QoS 1, never acknowledged.
	pub.publish(1, "crash/q1", []byte("one"), 1, packet.Properties{})
	require.Equal(t, packet.Puback, pub.read().FixedHeader.Type)
	m1 := sub.read()
	require.Equal(t, packet.Publish, m1.FixedHeader.Type)
	require.Equal(t, byte(1), m1.FixedHeader.Qos)

	// Outbound QoS 2 advanced to the PUBREL stage, never completed.
	pub.publish(2, "crash/q2", []byte("two"), 9, packet.Properties{})
	require.Equal(t, packet.Pubrec, pub.read().FixedHeader.Type)
	pub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrel}, PacketID: 9})
	require.Equal(t, packet.Pubcomp, pub.read().FixedHeader.Type)
	m2 := sub.read()
	require.Equal(t, packet.Publish, m2.FixedHeader.Type)
	require.Equal(t, byte(2), m2.FixedHeader.Qos)
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubrec}, PacketID: m2.PacketID})
	require.Equal(t, packet.Pubrel, sub.read().FixedHeader.Type)

	// A retained message on a topic outside the subscription. QoS 1 + PUBACK
	// is an explicit synchronization point for the broker-side handling.
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1, Retain: true},
		TopicName:   "other/ret",
		Payload:     []byte("kept"),
		PacketID:    10,
	})
	require.Equal(t, packet.Puback, pub.read().FixedHeader.Type)

	// Clean disconnect; wait until the session is marked stopped so the next
	// publish deterministically takes the offline-queue path.
	sub.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	sub.expectClosed(3 * time.Second)
	waitForCondition(t, "session marked stopped", func() bool {
		cl := s1.lookupConn("sub").sessionClient()
		return cl != nil && !cl.StoppedAt().IsZero()
	})
	pub.publish(1, "crash/offline", []byte("off"), 2, packet.Properties{})
	require.Equal(t, packet.Puback, pub.read().FixedHeader.Type)

	pub.close()
	require.NoError(t, s1.Close())
	closeStore1()

	// The crash image holds the session, the subscription and the subscriber
	// inflight rows (QoS 1 sent, QoS 2 in the PUBREL stage, offline queued).
	probe, err := store.Open(ctx, "sqlite:"+dbPath)
	require.NoError(t, err)
	sess, ok, err := probe.Sessions().Get(ctx, "sub")
	require.NoError(t, err)
	require.True(t, ok)
	assert.False(t, sess.StoppedAt.IsZero())
	rows := map[string]store.InflightMessage{}
	require.NoError(t, probe.Inflight().Iterate(ctx, func(m store.InflightMessage) bool {
		if m.ClientID == "sub" {
			rows[m.Packet.TopicName] = m
		}
		return true
	}))
	require.Len(t, rows, 3)
	assert.True(t, rows["crash/q1"].Sent)
	assert.False(t, rows["crash/q1"].Pubrel)
	assert.True(t, rows["crash/q2"].Sent)
	assert.True(t, rows["crash/q2"].Pubrel)
	assert.False(t, rows["crash/offline"].Sent, "the offline-queued message is not marked sent")
	require.NoError(t, probe.Close())

	// Phase 2: a fresh broker over the same database recovers everything.
	s2, closeStore2 := newStoreServer(t, dbPath)
	defer closeStore2()
	defer func() { require.NoError(t, s2.Close()) }()

	sub2 := dial(t, s2).v5()
	ack2 := sub2.connect("sub", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.True(t, ack2.SessionPresent, "session recovered")

	// Unacknowledged messages resend with DUP=1, ordered by packet
	// identifier: the QoS 1 PUBLISH, then the QoS 2 PUBREL.
	r1 := sub2.read()
	require.Equal(t, packet.Publish, r1.FixedHeader.Type)
	assert.Equal(t, m1.PacketID, r1.PacketID)
	assert.True(t, r1.FixedHeader.Dup)
	assert.Equal(t, "crash/q1", r1.TopicName)
	assert.Equal(t, []byte("one"), r1.Payload)
	r2 := sub2.read()
	require.Equal(t, packet.Pubrel, r2.FixedHeader.Type)
	assert.Equal(t, m2.PacketID, r2.PacketID)
	assert.True(t, r2.FixedHeader.Dup)

	// The offline-queued message is promoted afterwards, first transmission.
	r3 := sub2.read()
	require.Equal(t, packet.Publish, r3.FixedHeader.Type)
	assert.False(t, r3.FixedHeader.Dup)
	assert.Equal(t, "crash/offline", r3.TopicName)
	assert.Equal(t, []byte("off"), r3.Payload)

	// Completing the handshakes clears the persisted inflight rows.
	sub2.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Puback}, PacketID: r1.PacketID})
	sub2.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Pubcomp}, PacketID: r2.PacketID})
	sub2.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Puback}, PacketID: r3.PacketID})

	// The recovered subscription still delivers live traffic.
	pub2 := dial(t, s2).v5()
	pub2.connect("pub2", true, packet.Properties{})
	pub2.publish(0, "crash/live", []byte("after"), 0, packet.Properties{})
	live := sub2.read()
	require.Equal(t, packet.Publish, live.FixedHeader.Type)
	assert.Equal(t, "crash/live", live.TopicName)

	// The retained message survived the crash.
	watcher := dial(t, s2).v5()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "other/#", Qos: 0})
	ret := watcher.read()
	require.Equal(t, packet.Publish, ret.FixedHeader.Type)
	assert.Equal(t, "other/ret", ret.TopicName)
	assert.Equal(t, []byte("kept"), ret.Payload)
	assert.True(t, ret.FixedHeader.Retain)
}
