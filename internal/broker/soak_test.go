//go:build soak

package broker

// Soak test: 10k concurrent TCP connections with continuous pub/sub,
// asserting the broker stays healthy under sustained load. Excluded from the
// default test run by the "soak" build tag; run manually with
//
//	go test -tags=soak -run TestSoak -v ./internal/broker/
//
// MAYFLY_SOAK_CLIENTS overrides the connection count (default 10000) and
// MAYFLY_SOAK_DURATION the load phase (default 60s, e.g. "30m").

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
)

func soakDial(t *testing.T, addr, id, shard string) net.Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 10*time.Second)
	require.NoError(t, err)
	br := bufio.NewReader(nc)
	require.NoError(t, chaosWrite(nc, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: id},
	}))
	require.NoError(t, chaosReadFrame(nc, br, 10*time.Second)) // CONNACK
	require.NoError(t, chaosWrite(nc, &packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Subscribe},
		PacketID:    1,
		Filters:     []packet.Subscription{{Filter: "soak/" + shard, Qos: 0}},
	}))
	require.NoError(t, chaosReadFrame(nc, br, 10*time.Second)) // SUBACK
	return nc
}

func TestSoak10KConnections(t *testing.T) {
	total := 10000
	if v := os.Getenv("MAYFLY_SOAK_CLIENTS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		total = n
	}
	duration := time.Minute
	if v := os.Getenv("MAYFLY_SOAK_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoError(t, err)
		duration = d
	}

	l, err := listener.NewTCP(listener.TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)
	s := New(Options{FlushInterval: 0})
	s.AddListener(l)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-errCh
		_ = s.Close()
	})

	// Connect everything up front; shards bound the fan-out per publish.
	const shards = 256
	conns := make([]net.Conn, 0, total)
	for i := range total {
		conns = append(conns, soakDial(t, l.Addr(),
			fmt.Sprintf("soak-%d", i), fmt.Sprintf("%d", i%shards)))
	}
	defer func() {
		for _, nc := range conns {
			_ = nc.Close()
		}
	}()
	require.Eventually(t, func() bool { return s.ClientCount() == total },
		30*time.Second, 200*time.Millisecond, "broker did not register all connections")
	t.Logf("%d connections established, %d goroutines", total, runtime.NumGoroutine())

	// Continuous publish load: one publisher per shard, ~4 msg/s each.
	stop := make(chan struct{})
	var pubWG sync.WaitGroup
	var pubErrors atomic.Int64
	step := max(total/shards, 1)
	for i := 0; i < total; i += step {
		pubWG.Add(1)
		go func(nc net.Conn, shard string) {
			defer pubWG.Done()
			pk := &packet.Packet{
				FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: 0},
				TopicName:   "soak/" + shard, Payload: []byte("soak"),
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := chaosWrite(nc, pk); err != nil {
					pubErrors.Add(1)
					return
				}
				time.Sleep(250 * time.Millisecond)
			}
		}(conns[i], fmt.Sprintf("%d", i%shards))
	}

	time.Sleep(duration)
	close(stop)
	pubWG.Wait()
	require.Zero(t, pubErrors.Load(), "publishers hit write errors during soak")

	// The registry still tracks every connection; none were dropped.
	require.Eventually(t, func() bool { return s.ClientCount() == total },
		10*time.Second, 200*time.Millisecond, "broker dropped connections during soak")

	// Messages are still flowing: sample subscribers must see traffic.
	for _, i := range []int{0, total / 2, total - 1} {
		br := bufio.NewReader(conns[i])
		seen := 0
		for range 3 {
			if chaosReadFrame(conns[i], br, 2*time.Second) != nil {
				break
			}
			seen++
		}
		require.Positive(t, seen, "connection %d received no traffic after soak", i)
	}

	// Fresh clients are still served: full pub/sub round trip.
	box := dialTCP(t, l.Addr()).v5()
	box.connect("soak-probe", true, packet.Properties{})
	ack := box.subscribe(1, packet.Subscription{Filter: "soak/probe", Qos: 0})
	require.Equal(t, []byte{packet.CodeGrantedQos0}, ack.ReasonCodes)
	box.publish(0, "soak/probe", []byte("ok"), 0, packet.Properties{})
	msg := box.read()
	require.Equal(t, []byte("ok"), msg.Payload)
	box.close()

	t.Logf("soak complete: %d connections, %d goroutines, mem %.1f MiB",
		s.ClientCount(), runtime.NumGoroutine(), heapMiB())
}

func heapMiB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}
