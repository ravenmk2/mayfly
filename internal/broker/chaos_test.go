package broker

// Chaos tests: the broker is hammered by clients that disconnect mid-QoS2,
// never read their socket (slow consumers) and reconnect over each other
// with the same client ID (takeover storms). The assertions are behavioral:
// no panic, no deadlock (the test finishes inside its time budget), the
// client registry eventually returns to empty and the goroutine count
// settles back to the pre-test baseline.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
)

// chaosWrite encodes and sends one packet, never failing the test: workers
// run on goroutines where require is off-limits, and chaos errors are
// expected.
func chaosWrite(nc net.Conn, pk *packet.Packet) error {
	if pk.ProtocolVersion == 0 {
		pk.ProtocolVersion = packet.Version5
	}
	b, err := pk.Encode()
	if err != nil {
		return err
	}
	_ = nc.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = nc.Write(b)
	return err
}

// chaosReadFrame consumes exactly one packet, discarding its content.
func chaosReadFrame(nc net.Conn, br *bufio.Reader, d time.Duration) error {
	_ = nc.SetReadDeadline(time.Now().Add(d))
	if _, err := br.ReadByte(); err != nil {
		return err
	}
	remaining := 0
	for i := 0; i < 4; i++ {
		b, err := br.ReadByte()
		if err != nil {
			return err
		}
		remaining |= int(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			break
		}
	}
	if remaining > 0 {
		_, err := io.CopyN(io.Discard, br, int64(remaining))
		return err
	}
	return nil
}

// chaosWorker drives one pseudo-client for the active window. Every
// iteration reconnects (possibly over a live twin with the same ID),
// subscribes to the shared chaos topic and then plays random actions until
// it either reconnects abruptly or the window closes.
func chaosWorker(addr, id string, rng *rand.Rand, active time.Duration, slow bool) {
	deadline := time.Now().Add(active)
	for time.Now().Before(deadline) {
		nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		br := bufio.NewReader(nc)
		if err := chaosWrite(nc, &packet.Packet{
			FixedHeader: packet.FixedHeader{Type: packet.Connect},
			Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: id},
		}); err != nil {
			_ = nc.Close()
			continue
		}
		if chaosReadFrame(nc, br, 2*time.Second) != nil { // CONNACK
			_ = nc.Close()
			continue
		}
		_ = chaosWrite(nc, &packet.Packet{
			FixedHeader: packet.FixedHeader{Type: packet.Subscribe},
			PacketID:    1,
			Filters:     []packet.Subscription{{Filter: "chaos/#", Qos: 2}},
		})
		_ = chaosReadFrame(nc, br, 2*time.Second) // SUBACK

		var pid uint16 = 10
		for time.Now().Before(deadline) {
			pid++
			switch r := rng.Float64(); {
			case r < 0.45:
				// QoS 2 publish; frequently abandoned mid-handshake when the
				// reconnect branch below fires before PUBREL.
				if chaosWrite(nc, &packet.Packet{
					FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 2},
					TopicName:   "chaos/m", Payload: []byte("x"), PacketID: pid,
				}) != nil {
					goto reconnect
				}
				if rng.Float64() < 0.6 {
					if chaosReadFrame(nc, br, 300*time.Millisecond) != nil { // PUBREC
						goto reconnect
					}
					if rng.Float64() < 0.8 {
						_ = chaosWrite(nc, &packet.Packet{
							FixedHeader: packet.FixedHeader{Type: packet.Pubrel}, PacketID: pid,
						})
						_ = chaosReadFrame(nc, br, 300*time.Millisecond) // PUBCOMP
					}
				}
			case r < 0.65:
				// QoS 1 publish, ack only sometimes awaited.
				if chaosWrite(nc, &packet.Packet{
					FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 1},
					TopicName:   "chaos/m", Payload: []byte("y"), PacketID: pid,
				}) != nil {
					goto reconnect
				}
				if rng.Float64() < 0.5 {
					_ = chaosReadFrame(nc, br, 200*time.Millisecond)
				}
			case r < 0.85 || slow:
				// Slow consumer: sit on the socket without reading so the
				// outbound queue and inflight window fill up.
				time.Sleep(time.Duration(5+rng.IntN(40)) * time.Millisecond)
			default:
				// Fast consumer: drain whatever the broker pushed.
				_ = chaosReadFrame(nc, br, 5*time.Millisecond)
			}
			if rng.Float64() < 0.08 {
				goto reconnect // abrupt reconnect without DISCONNECT
			}
		}
	reconnect:
		_ = nc.Close()
	}
}

func TestChaosDisconnectsSlowConsumersTakeover(t *testing.T) {
	baseline := runtime.NumGoroutine()

	l, err := listener.NewTCP(listener.TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	s := newTestServer(t, nil)
	s.AddListener(l)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	const workers = 50
	const idPool = 10 // fewer IDs than workers: reconnects collide and take over
	active := 4 * time.Second
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(1, uint64(i)+1))
			chaosWorker(l.Addr(), fmt.Sprintf("chaos-%d", i%idPool), rng, active, i%5 == 0)
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("chaos workers did not finish: suspected deadlock")
	}

	// Every session was clean, so the registry must drain completely once the
	// broker notices the closed sockets.
	require.Eventually(t, func() bool { return s.ClientCount() == 0 },
		10*time.Second, 50*time.Millisecond, "client registry did not drain")

	// The broker is still alive and functional after the storm.
	c := dialTCP(t, l.Addr()).v5()
	c.connect("chaos-survivor", true, packet.Properties{})
	ack := c.subscribe(1, packet.Subscription{Filter: "chaos/after", Qos: 0})
	require.Equal(t, []byte{packet.CodeGrantedQos0}, ack.ReasonCodes)
	c.publish(0, "chaos/after", []byte("still-here"), 0, packet.Properties{})
	msg := c.read()
	assert.Equal(t, packet.Publish, msg.FixedHeader.Type)
	assert.Equal(t, []byte("still-here"), msg.Payload)
	c.close()
	require.Eventually(t, func() bool { return s.ClientCount() == 0 },
		10*time.Second, 50*time.Millisecond)

	// Shut down and wait for the goroutines to return to the baseline.
	cancel()
	require.NoError(t, <-errCh)
	require.NoError(t, s.Close())
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+25 },
		15*time.Second, 100*time.Millisecond,
		"goroutine leak: baseline %d, now %d", baseline, runtime.NumGoroutine())
}
