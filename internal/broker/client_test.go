package broker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// testClient is a byte-level MQTT client driving the broker over net.Pipe.
type testClient struct {
	t       *testing.T
	nc      net.Conn
	br      *bufio.Reader
	version byte
}

var connSeq atomic.Int64

// dial attaches a fresh in-memory connection to the broker.
func dial(t *testing.T, s *Server) *testClient {
	t.Helper()
	client, server := net.Pipe()
	s.ServeConn(fmt.Sprintf("test-%d", connSeq.Add(1)), server)
	return &testClient{t: t, nc: client, br: bufio.NewReader(client)}
}

// dialTCP connects over a real TCP listener served by the broker.
func dialTCP(t *testing.T, addr string) *testClient {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	return &testClient{t: t, nc: nc, br: bufio.NewReader(nc)}
}

// close cuts the connection without a DISCONNECT (an abnormal disconnect).
func (c *testClient) close() {
	_ = c.nc.Close()
}

// write encodes and sends pk.
func (c *testClient) write(pk *packet.Packet) {
	c.t.Helper()
	c.writeOrFail(pk, true)
}

// writeBestEffort sends pk, tolerating the broker closing the connection
// mid-write (oversized packet rejection).
func (c *testClient) writeBestEffort(pk *packet.Packet) {
	c.t.Helper()
	c.writeOrFail(pk, false)
}

func (c *testClient) writeOrFail(pk *packet.Packet, fail bool) {
	c.t.Helper()
	if pk.ProtocolVersion == 0 {
		pk.ProtocolVersion = c.version
	}
	b, err := pk.Encode()
	require.NoError(c.t, err)
	_ = c.nc.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.nc.Write(b); err != nil && fail {
		require.NoError(c.t, err)
	}
}

// read returns the next packet from the broker, failing after 3s.
func (c *testClient) read() *packet.Packet {
	c.t.Helper()
	pk, err := c.readFrame(3 * time.Second)
	require.NoError(c.t, err)
	return pk
}

// readMaybe returns the next packet or nil when none arrives within d.
func (c *testClient) readMaybe(d time.Duration) *packet.Packet {
	c.t.Helper()
	pk, _ := c.readFrame(d)
	return pk
}

// readEOF reads until the broker closes the connection.
func (c *testClient) readEOF(d time.Duration) error {
	c.t.Helper()
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	_, err := c.nc.Read(make([]byte, 1))
	if err == nil {
		return nil
	}
	return err
}

// readFrame decodes one packet with a read deadline; EOF/close is returned
// as a nil packet with the error.
func (c *testClient) readFrame(d time.Duration) (*packet.Packet, error) {
	_ = c.nc.SetReadDeadline(time.Now().Add(d))
	br := c.br
	first, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	raw := []byte{first}
	remaining := 0
	for i := 0; i < 4; i++ {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		raw = append(raw, b)
		remaining |= int(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			break
		}
	}
	if remaining > 0 {
		body := make([]byte, remaining)
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, err
		}
		raw = append(raw, body...)
	}
	pk := &packet.Packet{ProtocolVersion: c.version}
	if err := pk.Decode(raw); err != nil {
		return nil, err
	}
	return pk, nil
}

// connect sends CONNECT and returns the CONNACK, failing on rejection.
func (c *testClient) connect(id string, clean bool, props packet.Properties) *packet.Packet {
	c.t.Helper()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion:  c.version,
			Clean:            clean,
			ClientIdentifier: id,
		},
		Properties: props,
	})
	ack := c.read()
	require.Equal(c.t, packet.Connack, ack.FixedHeader.Type)
	require.Equal(c.t, packet.CodeSuccess, ack.ReasonCode, "CONNACK rejected the connection")
	return ack
}

// subscribe sends SUBSCRIBE for one filter and returns the SUBACK.
func (c *testClient) subscribe(packetID uint16, sub packet.Subscription) *packet.Packet {
	c.t.Helper()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Subscribe},
		PacketID:    packetID,
		Filters:     []packet.Subscription{sub},
		Properties:  subscriptionIDProp(sub),
	})
	ack := c.read()
	require.Equal(c.t, packet.Suback, ack.FixedHeader.Type)
	require.Equal(c.t, packetID, ack.PacketID)
	return ack
}

// subscriptionIDProp moves the per-filter identifier into the v5 SUBSCRIBE
// properties where the encoder expects it.
func subscriptionIDProp(sub packet.Subscription) packet.Properties {
	if sub.Identifier == 0 {
		return packet.Properties{}
	}
	return packet.Properties{SubscriptionID: []uint32{sub.Identifier}}
}

// publish sends a PUBLISH.
func (c *testClient) publish(qos byte, topic string, payload []byte, packetID uint16, props packet.Properties) {
	c.t.Helper()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: qos},
		TopicName:   topic,
		Payload:     payload,
		PacketID:    packetID,
		Properties:  props,
	})
}

// v3 marks the client as MQTT 3.1.1.
func (c *testClient) v3() *testClient { c.version = packet.Version311; return c }

// v5 marks the client as MQTT 5.0.
func (c *testClient) v5() *testClient { c.version = packet.Version5; return c }

// newTestServer builds a broker with fast housekeeping for tests.
func newTestServer(t *testing.T, mutate func(*Options)) *Server {
	t.Helper()
	opts := Options{FlushInterval: 0}
	if mutate != nil {
		mutate(&opts)
	}
	return New(opts)
}

// expectClosed asserts the broker closes the connection within d.
func (c *testClient) expectClosed(d time.Duration) {
	c.t.Helper()
	err := c.readEOF(d)
	if err == nil {
		c.t.Fatal("expected the broker to close the connection")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		// A read deadline exceeded error also proves the connection stayed
		// open, which is a failure here.
		var nerr net.Error
		if errors.As(err, &nerr) && nerr.Timeout() {
			c.t.Fatal("connection still open after the deadline")
		}
	}
}
