package broker

// Interoperability tests driving the broker with the real Eclipse paho
// client libraries over a loopback TCP listener: paho.golang (MQTT 5.0) and
// paho.mqtt.golang (MQTT 3.1.1, since paho.golang is v5-only). They live in
// the broker package to reuse the serveBroker test harness instead of
// duplicating it in a separate package.

import (
	"context"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqttv3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/listener"
)

// interopBroker starts a broker on a real TCP listener at 127.0.0.1:0 and
// returns its address.
func interopBroker(t *testing.T, mutate func(*Options)) string {
	t.Helper()
	l, err := listener.NewTCP(listener.TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	serveBroker(t, l, mutate)
	return l.Addr()
}

func interopCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// inbox collects inbound publishes from a paho handler.
type inbox[T any] struct {
	ch chan T
}

func newInbox[T any]() *inbox[T] { return &inbox[T]{ch: make(chan T, 64)} }

func (b *inbox[T]) next(t *testing.T) T {
	t.Helper()
	select {
	case m := <-b.ch:
		return m
	case <-time.After(5 * time.Second):
		var zero T
		t.Fatal("timed out waiting for a message")
		return zero
	}
}

func (b *inbox[T]) none(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case m := <-b.ch:
		t.Fatalf("unexpected message: %+v", m)
	case <-time.After(d):
	}
}

func (b *inbox[T]) drain() int {
	n := 0
	for {
		select {
		case <-b.ch:
			n++
		default:
			return n
		}
	}
}

// ---------------------------------------------------------------------------
// MQTT 5.0 via paho.golang
// ---------------------------------------------------------------------------

type v5msg struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// dialV5 connects a paho.golang client and returns it with the raw conn (for
// abnormal-disconnect scenarios) and the CONNACK.
func dialV5(t *testing.T, addr string, cp *paho.Connect, box *inbox[v5msg]) (*paho.Client, net.Conn, *paho.Connack) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	cfg := paho.ClientConfig{Conn: nc, ClientID: cp.ClientID}
	if box != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				box.ch <- v5msg{
					topic:   pr.Packet.Topic,
					payload: append([]byte(nil), pr.Packet.Payload...),
					qos:     pr.Packet.QoS,
					retain:  pr.Packet.Retain,
				}
				return true, nil
			},
		}
	}
	c := paho.NewClient(cfg)
	ctx, cancel := interopCtx()
	defer cancel()
	ca, err := c.Connect(ctx, cp)
	require.NoError(t, err)
	return c, nc, ca
}

func v5Subscribe(t *testing.T, c *paho.Client, filter string, qos byte) *paho.Suback {
	t.Helper()
	ctx, cancel := interopCtx()
	defer cancel()
	ack, err := c.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}},
	})
	require.NoError(t, err)
	require.Len(t, ack.Reasons, 1)
	require.Equal(t, qos, ack.Reasons[0], "SUBACK reason for %s", filter)
	return ack
}

func v5Publish(t *testing.T, c *paho.Client, topic string, payload []byte, qos byte, retain bool) {
	t.Helper()
	ctx, cancel := interopCtx()
	defer cancel()
	_, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: payload, Retain: retain})
	require.NoError(t, err)
}

func TestInteropV5Connect(t *testing.T) {
	addr := interopBroker(t, nil)
	c, _, ca := dialV5(t, addr, &paho.Connect{
		ClientID: "interop-v5-conn", CleanStart: true, KeepAlive: 30,
	}, nil)
	assert.Equal(t, uint8(0), ca.ReasonCode)
	assert.False(t, ca.SessionPresent)
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5PubSubQoS(t *testing.T) {
	addr := interopBroker(t, nil)
	for _, qos := range []byte{0, 1, 2} {
		t.Run("qos"+string(rune('0'+qos)), func(t *testing.T) {
			box := newInbox[v5msg]()
			sub, _, _ := dialV5(t, addr, &paho.Connect{
				ClientID: "v5-sub", CleanStart: true, KeepAlive: 30,
			}, box)
			v5Subscribe(t, sub, "interop/v5/qos", qos)

			pub, _, _ := dialV5(t, addr, &paho.Connect{
				ClientID: "v5-pub", CleanStart: true, KeepAlive: 30,
			}, nil)
			v5Publish(t, pub, "interop/v5/qos", []byte("payload"), qos, false)

			msg := box.next(t)
			assert.Equal(t, "interop/v5/qos", msg.topic)
			assert.Equal(t, []byte("payload"), msg.payload)
			assert.Equal(t, qos, msg.qos)

			require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
			require.NoError(t, sub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
		})
	}
}

func TestInteropV5Retained(t *testing.T) {
	addr := interopBroker(t, nil)
	pub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-ret-pub", CleanStart: true, KeepAlive: 30}, nil)
	v5Publish(t, pub, "interop/v5/retained", []byte("sticky"), 1, true)
	require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// A subscriber connecting after the retained publish must receive it with
	// the retain flag set.
	box := newInbox[v5msg]()
	sub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-ret-sub", CleanStart: true, KeepAlive: 30}, box)
	v5Subscribe(t, sub, "interop/v5/retained", 1)
	msg := box.next(t)
	assert.Equal(t, []byte("sticky"), msg.payload)
	assert.True(t, msg.retain)
	require.NoError(t, sub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5Will(t *testing.T) {
	addr := interopBroker(t, nil)
	box := newInbox[v5msg]()
	watcher, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-will-watch", CleanStart: true, KeepAlive: 30}, box)
	v5Subscribe(t, watcher, "interop/v5/will", 0)

	_, nc, _ := dialV5(t, addr, &paho.Connect{
		ClientID: "v5-will-dead", CleanStart: true, KeepAlive: 30,
		WillMessage: &paho.WillMessage{
			Topic: "interop/v5/will", Payload: []byte("gone"), QoS: 0,
		},
	}, nil)
	// Abnormal disconnect: close the socket without DISCONNECT.
	require.NoError(t, nc.Close())

	msg := box.next(t)
	assert.Equal(t, "interop/v5/will", msg.topic)
	assert.Equal(t, []byte("gone"), msg.payload)
	require.NoError(t, watcher.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5SharedSubscription(t *testing.T) {
	addr := interopBroker(t, nil)
	boxA, boxB := newInbox[v5msg](), newInbox[v5msg]()
	subA, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-share-a", CleanStart: true, KeepAlive: 30}, boxA)
	subB, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-share-b", CleanStart: true, KeepAlive: 30}, boxB)
	v5Subscribe(t, subA, "$share/workers/interop/v5/shared", 1)
	v5Subscribe(t, subB, "$share/workers/interop/v5/shared", 1)

	pub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-share-pub", CleanStart: true, KeepAlive: 30}, nil)
	const total = 6
	for range total {
		v5Publish(t, pub, "interop/v5/shared", []byte("job"), 1, false)
	}

	// Every message lands on exactly one member; over several messages the
	// group load-balances.
	var gotA, gotB int
	require.Eventually(t, func() bool {
		gotA += boxA.drain()
		gotB += boxB.drain()
		return gotA+gotB == total
	}, 5*time.Second, 50*time.Millisecond)
	assert.Positive(t, gotA, "shared subscriber A got nothing")
	assert.Positive(t, gotB, "shared subscriber B got nothing")

	require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	require.NoError(t, subA.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	require.NoError(t, subB.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5Unsubscribe(t *testing.T) {
	addr := interopBroker(t, nil)
	box := newInbox[v5msg]()
	sub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-unsub", CleanStart: true, KeepAlive: 30}, box)
	v5Subscribe(t, sub, "interop/v5/unsub", 0)

	ctx, cancel := interopCtx()
	defer cancel()
	ack, err := sub.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{"interop/v5/unsub"}})
	require.NoError(t, err)
	require.Equal(t, []byte{0}, ack.Reasons) // success

	pub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-unsub-pub", CleanStart: true, KeepAlive: 30}, nil)
	v5Publish(t, pub, "interop/v5/unsub", []byte("lost"), 0, false)
	box.none(t, 300*time.Millisecond)

	require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	require.NoError(t, sub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5Keepalive(t *testing.T) {
	addr := interopBroker(t, nil)
	var clientErr atomic.Bool
	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	c := paho.NewClient(paho.ClientConfig{
		Conn: nc, ClientID: "v5-ping",
		OnClientError: func(error) { clientErr.Store(true) },
	})
	ctx, cancel := interopCtx()
	defer cancel()
	_, err = c.Connect(ctx, &paho.Connect{ClientID: "v5-ping", CleanStart: true, KeepAlive: 1})
	require.NoError(t, err)

	// With KeepAlive 1s the broker drops the connection after ~1.5s of
	// silence; the default pinger must keep it alive well beyond that.
	time.Sleep(3500 * time.Millisecond)
	assert.False(t, clientErr.Load(), "client errored during keepalive idle period")
	v5Publish(t, c, "interop/v5/ping", []byte("alive"), 0, false)
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

func TestInteropV5SessionResume(t *testing.T) {
	addr := interopBroker(t, nil)
	expiry := uint32(60)

	box := newInbox[v5msg]()
	c1, _, ca1 := dialV5(t, addr, &paho.Connect{
		ClientID: "v5-resume", CleanStart: true, KeepAlive: 30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	}, box)
	assert.False(t, ca1.SessionPresent)
	v5Subscribe(t, c1, "interop/v5/resume", 1)
	require.NoError(t, c1.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// Reconnect without clean start: the session (and subscription) survive.
	c2, _, ca2 := dialV5(t, addr, &paho.Connect{
		ClientID: "v5-resume", CleanStart: false, KeepAlive: 30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	}, box)
	assert.True(t, ca2.SessionPresent)

	pub, _, _ := dialV5(t, addr, &paho.Connect{ClientID: "v5-resume-pub", CleanStart: true, KeepAlive: 30}, nil)
	v5Publish(t, pub, "interop/v5/resume", []byte("after-resume"), 1, false)
	msg := box.next(t)
	assert.Equal(t, []byte("after-resume"), msg.payload)

	require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	require.NoError(t, c2.Disconnect(&paho.Disconnect{ReasonCode: 0}))
}

// ---------------------------------------------------------------------------
// MQTT 3.1.1 via paho.mqtt.golang
// ---------------------------------------------------------------------------

type v3msg struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// dialV3 connects a paho.mqtt.golang client. When captureConn is set, the
// underlying net.Conn is returned for abnormal-disconnect scenarios.
func dialV3(t *testing.T, addr string, mutate func(*mqttv3.ClientOptions), box *inbox[v3msg]) (mqttv3.Client, net.Conn, *mqttv3.ConnectToken) {
	t.Helper()
	opts := mqttv3.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(5 * time.Second).
		SetWriteTimeout(5 * time.Second).
		SetPingTimeout(3 * time.Second)
	if box != nil {
		opts.SetDefaultPublishHandler(func(_ mqttv3.Client, m mqttv3.Message) {
			box.ch <- v3msg{
				topic:   m.Topic(),
				payload: append([]byte(nil), m.Payload()...),
				qos:     m.Qos(),
				retain:  m.Retained(),
			}
		})
	}
	var raw net.Conn
	opts.SetCustomOpenConnectionFn(func(uri *url.URL, _ mqttv3.ClientOptions) (net.Conn, error) {
		c, err := net.Dial("tcp", uri.Host)
		if err == nil {
			raw = c
		}
		return c, err
	})
	if mutate != nil {
		mutate(opts)
	}
	c := mqttv3.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(10*time.Second), "v3 CONNECT timed out")
	require.NoError(t, tok.Error())
	ct, ok := tok.(*mqttv3.ConnectToken)
	require.True(t, ok)
	return c, raw, ct
}

func v3Subscribe(t *testing.T, c mqttv3.Client, filter string, qos byte) {
	t.Helper()
	tok := c.Subscribe(filter, qos, nil)
	require.True(t, tok.WaitTimeout(10*time.Second), "v3 SUBSCRIBE timed out")
	require.NoError(t, tok.Error())
	st, ok := tok.(*mqttv3.SubscribeToken)
	require.True(t, ok)
	// The result map is keyed by the normalized filter (paho.mqtt strips the
	// $share/<group>/ prefix), so assert on the granted values only.
	require.Len(t, st.Result(), 1)
	for _, granted := range st.Result() {
		require.Equal(t, qos, granted)
	}
}

func v3Publish(t *testing.T, c mqttv3.Client, topic string, payload []byte, qos byte, retain bool) {
	t.Helper()
	tok := c.Publish(topic, qos, retain, payload)
	require.True(t, tok.WaitTimeout(10*time.Second), "v3 PUBLISH timed out")
	require.NoError(t, tok.Error())
}

func TestInteropV3Connect(t *testing.T) {
	addr := interopBroker(t, nil)
	c, _, ct := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("interop-v3-conn") }, nil)
	assert.False(t, ct.SessionPresent())
	c.Disconnect(250)
}

func TestInteropV3PubSubQoS(t *testing.T) {
	addr := interopBroker(t, nil)
	for _, qos := range []byte{0, 1, 2} {
		t.Run("qos"+string(rune('0'+qos)), func(t *testing.T) {
			box := newInbox[v3msg]()
			sub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-sub") }, box)
			v3Subscribe(t, sub, "interop/v3/qos", qos)

			pub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-pub") }, nil)
			v3Publish(t, pub, "interop/v3/qos", []byte("payload"), qos, false)

			msg := box.next(t)
			assert.Equal(t, "interop/v3/qos", msg.topic)
			assert.Equal(t, []byte("payload"), msg.payload)
			assert.Equal(t, qos, msg.qos)

			pub.Disconnect(250)
			sub.Disconnect(250)
		})
	}
}

func TestInteropV3Retained(t *testing.T) {
	addr := interopBroker(t, nil)
	pub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-ret-pub") }, nil)
	v3Publish(t, pub, "interop/v3/retained", []byte("sticky"), 1, true)
	pub.Disconnect(250)

	box := newInbox[v3msg]()
	sub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-ret-sub") }, box)
	v3Subscribe(t, sub, "interop/v3/retained", 1)
	msg := box.next(t)
	assert.Equal(t, []byte("sticky"), msg.payload)
	assert.True(t, msg.retain)
	sub.Disconnect(250)
}

func TestInteropV3Will(t *testing.T) {
	addr := interopBroker(t, nil)
	box := newInbox[v3msg]()
	watcher, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-will-watch") }, box)
	v3Subscribe(t, watcher, "interop/v3/will", 0)

	_, raw, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) {
		o.SetClientID("v3-will-dead")
		o.SetWill("interop/v3/will", "gone", 0, false)
	}, nil)
	require.NotNil(t, raw)
	// Abnormal disconnect: close the socket without DISCONNECT.
	require.NoError(t, raw.Close())

	msg := box.next(t)
	assert.Equal(t, "interop/v3/will", msg.topic)
	assert.Equal(t, []byte("gone"), msg.payload)
	watcher.Disconnect(250)
}

func TestInteropV3SharedSubscription(t *testing.T) {
	addr := interopBroker(t, nil)
	boxA, boxB := newInbox[v3msg](), newInbox[v3msg]()
	subA, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-share-a") }, boxA)
	subB, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-share-b") }, boxB)
	v3Subscribe(t, subA, "$share/workers/interop/v3/shared", 1)
	v3Subscribe(t, subB, "$share/workers/interop/v3/shared", 1)

	pub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-share-pub") }, nil)
	const total = 6
	for range total {
		v3Publish(t, pub, "interop/v3/shared", []byte("job"), 1, false)
	}

	var gotA, gotB int
	require.Eventually(t, func() bool {
		gotA += boxA.drain()
		gotB += boxB.drain()
		return gotA+gotB == total
	}, 5*time.Second, 50*time.Millisecond)
	assert.Positive(t, gotA, "shared subscriber A got nothing")
	assert.Positive(t, gotB, "shared subscriber B got nothing")

	pub.Disconnect(250)
	subA.Disconnect(250)
	subB.Disconnect(250)
}

func TestInteropV3Unsubscribe(t *testing.T) {
	addr := interopBroker(t, nil)
	box := newInbox[v3msg]()
	sub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-unsub") }, box)
	v3Subscribe(t, sub, "interop/v3/unsub", 0)

	tok := sub.Unsubscribe("interop/v3/unsub")
	require.True(t, tok.WaitTimeout(10*time.Second))
	require.NoError(t, tok.Error())

	pub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-unsub-pub") }, nil)
	v3Publish(t, pub, "interop/v3/unsub", []byte("lost"), 0, false)
	box.none(t, 300*time.Millisecond)

	pub.Disconnect(250)
	sub.Disconnect(250)
}

func TestInteropV3Keepalive(t *testing.T) {
	addr := interopBroker(t, nil)
	c, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) {
		o.SetClientID("v3-ping")
		o.SetKeepAlive(time.Second)
	}, nil)

	// With KeepAlive 1s the broker drops silent connections after ~1.5s; the
	// client's automatic ping must keep this one alive.
	time.Sleep(3500 * time.Millisecond)
	assert.True(t, c.IsConnected(), "connection dropped during keepalive idle period")
	v3Publish(t, c, "interop/v3/ping", []byte("alive"), 0, false)
	c.Disconnect(250)
}

func TestInteropV3SessionResume(t *testing.T) {
	addr := interopBroker(t, nil)

	box := newInbox[v3msg]()
	c1, _, ct1 := dialV3(t, addr, func(o *mqttv3.ClientOptions) {
		o.SetClientID("v3-resume")
		o.SetCleanSession(false)
	}, box)
	assert.False(t, ct1.SessionPresent())
	v3Subscribe(t, c1, "interop/v3/resume", 1)
	c1.Disconnect(250)

	// Reconnect with the same ID and clean session off: the broker resumes
	// the persisted subscription.
	c2, _, ct2 := dialV3(t, addr, func(o *mqttv3.ClientOptions) {
		o.SetClientID("v3-resume")
		o.SetCleanSession(false)
	}, box)
	assert.True(t, ct2.SessionPresent())

	pub, _, _ := dialV3(t, addr, func(o *mqttv3.ClientOptions) { o.SetClientID("v3-resume-pub") }, nil)
	v3Publish(t, pub, "interop/v3/resume", []byte("after-resume"), 1, false)
	msg := box.next(t)
	assert.Equal(t, []byte("after-resume"), msg.payload)

	pub.Disconnect(250)
	c2.Disconnect(250)
}
