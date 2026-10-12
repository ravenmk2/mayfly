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

// openBatchStore opens a SQLite store in a temp directory and wraps it in a
// BatchWriter. The returned close function drains the writer and closes the
// database (simulating shutdown with all writes landed).
func openBatchStore(t *testing.T, path string) (*store.BatchWriter, func()) {
	t.Helper()
	inner, err := store.Open(context.Background(), "sqlite:"+path)
	require.NoError(t, err)
	bw, err := store.NewBatchWriter(inner, store.BatchOptions{})
	require.NoError(t, err)
	return bw, func() { require.NoError(t, bw.Close()) }
}

// newStoreServer builds a broker backed by the SQLite store at path.
func newStoreServer(t *testing.T, path string) (*Server, func()) {
	t.Helper()
	bw, closeStore := openBatchStore(t, path)
	s := newTestServer(t, func(o *Options) { o.Store = bw })
	return s, closeStore
}

func TestStoreWiringAndRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "mayfly.db")

	// Boot 1: persistent session subscribes; a retained message is stored.
	s1, closeStore1 := newStoreServer(t, dbPath)
	dev := dial(t, s1).v5()
	ack := dev.connect("dev", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.False(t, ack.SessionPresent)
	sub := dev.subscribe(1, packet.Subscription{Filter: "sensors/+", Qos: 1})
	require.Equal(t, []byte{packet.CodeGrantedQos1}, sub.ReasonCodes)

	pub := dial(t, s1).v5()
	pub.connect("pub", true, packet.Properties{})
	// QoS 1 + PUBACK is the synchronization point: without it the broker
	// read loop may not have processed the retained publish before Close.
	pub.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1, Retain: true},
		TopicName:   "retain/topic",
		Payload:     []byte("kept"),
		PacketID:    1,
	})
	require.Equal(t, packet.Puback, pub.read().FixedHeader.Type)

	dev.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	dev.expectClosed(time.Second)
	pub.close()
	require.NoError(t, s1.Close())
	closeStore1()

	// The database holds the session (stopped), the subscription and the
	// retained message.
	probe, err := store.Open(ctx, "sqlite:"+dbPath)
	require.NoError(t, err)
	sess, ok, err := probe.Sessions().Get(ctx, "dev")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(300), sess.Expiry)
	assert.False(t, sess.StoppedAt.IsZero())
	var subRows int
	require.NoError(t, probe.Subscriptions().Iterate(ctx, func(store.Subscription) bool {
		subRows++
		return true
	}))
	assert.Equal(t, 1, subRows)
	var retRows int
	require.NoError(t, probe.Retained().Iterate(ctx, func(store.RetainedMessage) bool {
		retRows++
		return true
	}))
	assert.Equal(t, 1, retRows)
	require.NoError(t, probe.Close())

	// Boot 2: state is recovered before connections are served.
	s2, closeStore2 := newStoreServer(t, dbPath)
	defer closeStore2()
	defer func() { require.NoError(t, s2.Close()) }()
	require.Equal(t, 1, s2.ClientCount(), "offline session restored")
	assert.EqualValues(t, 1, s2.Topics().SubscriptionCount())
	assert.Equal(t, 1, s2.Topics().RetainedCount())

	dev2 := dial(t, s2).v5()
	ack2 := dev2.connect("dev", false, packet.Properties{SessionExpiry: 300, SessionExpiryFlag: true})
	assert.True(t, ack2.SessionPresent)

	// The recovered subscription delivers.
	pub2 := dial(t, s2).v5()
	pub2.connect("pub2", true, packet.Properties{})
	pub2.publish(1, "sensors/temp", []byte("21"), 5, packet.Properties{})
	puback := pub2.read()
	require.Equal(t, packet.Puback, puback.FixedHeader.Type)
	msg := dev2.read()
	require.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, "sensors/temp", msg.TopicName)
	assert.Equal(t, []byte("21"), msg.Payload)

	// The recovered retained message reaches a new subscriber.
	watcher := dial(t, s2).v5()
	watcher.connect("watcher", true, packet.Properties{})
	watcher.subscribe(1, packet.Subscription{Filter: "retain/#", Qos: 0})
	ret := watcher.read()
	require.Equal(t, packet.Publish, ret.FixedHeader.Type)
	assert.Equal(t, "retain/topic", ret.TopicName)
	assert.Equal(t, []byte("kept"), ret.Payload)
	assert.True(t, ret.FixedHeader.Retain)
}

func TestStoreSessionLifecycleRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "mayfly.db")

	s, closeStore := newStoreServer(t, dbPath)
	// Clean session: removed from the store at disconnect.
	clean := dial(t, s).v5()
	clean.connect("clean", true, packet.Properties{})
	clean.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	clean.expectClosed(time.Second)

	// Expired persistent session: purged by the sweep.
	expiring := dial(t, s).v5()
	expiring.connect("expiring", false, packet.Properties{SessionExpiry: 60, SessionExpiryFlag: true})
	expiring.write(&packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}})
	expiring.expectClosed(time.Second)
	require.Equal(t, 1, s.ClientCount())
	s.sweep(s.now().Add(61 * time.Second))
	require.Equal(t, 0, s.ClientCount())

	require.NoError(t, s.Close())
	closeStore()

	probe, err := store.Open(ctx, "sqlite:"+dbPath)
	require.NoError(t, err)
	defer func() { _ = probe.Close() }()
	require.NoError(t, probe.Sessions().Iterate(ctx, func(sess store.Session) bool {
		t.Errorf("unexpected session row: %+v", sess)
		return true
	}))
}

func TestMemoryOnlyServerWithoutStore(t *testing.T) {
	// A broker without a store keeps the pre-persistence behavior.
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("c1", true, packet.Properties{})
	c.subscribe(1, packet.Subscription{Filter: "a/#", Qos: 1})
	pub := dial(t, s).v5()
	pub.connect("p", true, packet.Properties{})
	pub.publish(1, "a/b", []byte("x"), 1, packet.Properties{})
	pub.read()
	msg := c.read()
	assert.Equal(t, []byte("x"), msg.Payload)
}
