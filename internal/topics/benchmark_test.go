package topics

import (
	"strconv"
	"testing"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/stretchr/testify/require"
)

func BenchmarkSubscribe(b *testing.B) {
	x := New()
	i := 0
	for b.Loop() {
		_, err := x.Subscribe("client-"+strconv.Itoa(i%1000),
			Subscription{Filter: "bench/" + strconv.Itoa(i) + "/x", Qos: 1})
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkSubscribers(b *testing.B) {
	for _, fanout := range []int{1, 100, 10000} {
		b.Run("fanout_"+strconv.Itoa(fanout), func(b *testing.B) {
			x := New()
			for i := 0; i < fanout; i++ {
				var filter string
				switch i % 3 {
				case 0:
					filter = "bench/a/b"
				case 1:
					filter = "bench/+/b"
				default:
					filter = "bench/#"
				}
				_, err := x.Subscribe("client-"+strconv.Itoa(i), Subscription{Filter: filter})
				require.NoError(b, err)
			}
			b.ResetTimer()
			for b.Loop() {
				x.Subscribers("bench/a/b")
			}
		})
	}
}

func BenchmarkUnsubscribe(b *testing.B) {
	x := New()
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = "client-" + strconv.Itoa(i)
		_, err := x.Subscribe(ids[i], Subscription{Filter: "bench/a/b"})
		require.NoError(b, err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := x.Unsubscribe(ids[i], "bench/a/b"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRetain(b *testing.B) {
	x := New()
	pk := packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
		Payload:     []byte("payload"),
	}
	i := 0
	for b.Loop() {
		pk.TopicName = "bench/" + strconv.Itoa(i)
		if err := x.Retain(pk); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkMessages(b *testing.B) {
	x := New()
	for i := 0; i < 1000; i++ {
		err := x.Retain(packet.Packet{
			FixedHeader: packet.FixedHeader{Type: packet.Publish, Retain: true},
			TopicName:   "bench/" + strconv.Itoa(i) + "/x",
			Payload:     []byte("payload"),
		})
		require.NoError(b, err)
	}
	sub := Subscription{Filter: "bench/#"}
	b.ResetTimer()
	for b.Loop() {
		x.Messages(sub, false)
	}
}
