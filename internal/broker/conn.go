package broker

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/listener"
	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/session"
)

// errNormalClose ends the read loop without an error log (peer DISCONNECT or
// a clean EOF).
var errNormalClose = errors.New("broker: connection closed")

// conn is one network connection and its write path state. The session state
// machine lives in the referenced session.Client.
type conn struct {
	server *Server
	id     string // broker-unique connection id ("<listener>-<seq>")
	nc     net.Conn

	// queue is the single bounded outbound queue; every packet written to
	// the peer, control packets included, passes through it so ordering is
	// preserved (architecture decision A2).
	queue chan *packet.Packet

	done        chan struct{} // closed by stop
	stopOnce    sync.Once
	closeNet    sync.Once
	cleanupOnce sync.Once

	// suppressWill marks connections whose Will is handled elsewhere
	// (session expiry sweep), so cleanup does not publish it twice.
	suppressWill atomic.Bool
	// omitReasonString records a v5 client's Request Problem Information
	// of 0: the broker then omits Reason String properties on packets it
	// sends [MQTT-3.1.2-29].
	omitReasonString atomic.Bool

	mu                 sync.Mutex
	client             *session.Client // nil until CONNECT succeeds
	info               hook.ClientInfo
	version            byte
	connectedAt        time.Time
	receivedDisconnect bool
	disconnectReason   byte
}

func newConn(s *Server, id string, nc net.Conn) *conn {
	return &conn{
		server: s,
		id:     id,
		nc:     nc,
		queue:  make(chan *packet.Packet, s.opts.OutboundQueueDepth),
		done:   make(chan struct{}),
	}
}

// remoteAddr returns the peer address for logs and hook calls.
func (c *conn) remoteAddr() string {
	if lc, ok := c.nc.(*listener.Conn); ok {
		return lc.Conn.RemoteAddr().String()
	}
	return c.nc.RemoteAddr().String()
}

// listenerID returns the identifier of the listener the connection arrived
// on, empty for in-memory transports.
func (c *conn) listenerID() string {
	if lc, ok := c.nc.(*listener.Conn); ok {
		return lc.ListenerID
	}
	return ""
}

// sessionClient returns the session, nil before CONNECT.
func (c *conn) sessionClient() *session.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}

// protocolVersion returns the negotiated protocol version, zero before
// CONNECT.
func (c *conn) protocolVersion() byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.version
}

// stop signals termination exactly once. The socket itself is closed by the
// write loop after it drains the queue, so final control packets (CONNACK
// rejections, protocol error DISCONNECTs) still reach the peer.
func (c *conn) stop() {
	c.stopOnce.Do(func() {
		close(c.done)
	})
}

// closeSocket closes the network connection exactly once.
func (c *conn) closeSocket() {
	c.closeNet.Do(func() {
		_ = c.nc.Close()
	})
}

func (c *conn) isStopped() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// readLoop is the inbound goroutine (architecture decision A2): it reads one
// full packet per iteration and feeds the session state machine; it never
// writes to the socket directly.
func (c *conn) readLoop() {
	defer c.server.wg.Done()
	defer c.cleanup()
	defer c.stop()

	s := c.server
	br := bufio.NewReaderSize(c.nc, s.opts.ReadBufferSize)
	for {
		c.setReadDeadline()
		pk, err := c.readPacket(br)
		if err != nil {
			// Codec violations are protocol errors the v5 client is
			// notified about; I/O failures are handled quietly.
			var code packet.Code
			if errors.As(err, &code) {
				c.fail(code)
			} else {
				c.abortOnReadError(err)
			}
			return
		}
		if c.sessionClient() == nil && pk.FixedHeader.Type != packet.Connect {
			// The first packet on a connection MUST be CONNECT
			// [MQTT-3.1.0-1].
			s.log.Debug("first packet not CONNECT", "conn", c.id)
			return
		}
		if err := c.processPacket(pk); err != nil {
			if !errors.Is(err, errNormalClose) {
				c.fail(err)
			}
			return
		}
	}
}

// setReadDeadline applies the keepalive-derived read deadline: before
// CONNECT the connect timeout applies; afterwards one and a half times the
// negotiated Keep Alive [MQTT-3.1.2-24]. A Keep Alive of 0 disables the
// check.
func (c *conn) setReadDeadline() {
	timeout := c.server.opts.ConnectTimeout
	if cl := c.sessionClient(); cl != nil {
		timeout = cl.KeepaliveTimeout()
	}
	if timeout <= 0 {
		_ = c.nc.SetReadDeadline(time.Time{})
		return
	}
	_ = c.nc.SetReadDeadline(c.server.now().Add(timeout))
}

// readPacket reads one complete control packet: the fixed header byte, the
// Remaining Length varint, then the body. max_packet_size is enforced right
// after the fixed header so oversized packets are rejected without reading
// the body (docs/design/listeners.md section 3).
func (c *conn) readPacket(br *bufio.Reader) (*packet.Packet, error) {
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
		if i == 3 {
			// A fifth Remaining Length byte exceeds the encoding limit
			// [MQTT-2.1.4-1].
			return nil, packet.Code{Code: packet.CodeMalformedPacket,
				Reason: "remaining length exceeds 4 bytes"}
		}
	}
	if max := c.server.opts.MaxPacketSize; len(raw)+remaining > max {
		return nil, packet.Code{Code: packet.CodePacketTooLarge,
			Reason: "packet exceeds the broker maximum packet size"}
	}
	if remaining > 0 {
		body := make([]byte, remaining)
		if _, err := io.ReadFull(br, body); err != nil {
			return nil, err
		}
		raw = append(raw, body...)
	}
	pk := &packet.Packet{ProtocolVersion: c.protocolVersion()}
	if err := pk.Decode(raw); err != nil {
		return nil, err
	}
	c.server.Stats.BytesReceived.Add(int64(len(raw)))
	return pk, nil
}

// abortOnReadError classifies a read failure: keepalive expiry notifies the
// client (v5), EOF and close are silent.
func (c *conn) abortOnReadError(err error) {
	s := c.server
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		if cl := c.sessionClient(); cl != nil && cl.KeepaliveTimeout() > 0 {
			s.log.Debug("keepalive timeout", "client", cl.ID)
			if c.protocolVersion() == packet.Version5 {
				c.enqueue(&packet.Packet{
					FixedHeader:     packet.FixedHeader{Type: packet.Disconnect},
					ProtocolVersion: packet.Version5,
					ReasonCode:      packet.CodeKeepAliveTimeout,
				})
			}
		}
		return
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || c.isStopped() {
		return
	}
	s.log.Debug("read failed", "conn", c.id, "error", err)
}

// fail terminates the connection after a protocol error, notifying v5
// clients with a DISCONNECT carrying the reason code when possible.
func (c *conn) fail(err error) {
	s := c.server
	var code packet.Code
	if errors.As(err, &code) && c.protocolVersion() == packet.Version5 {
		ack := &packet.Packet{
			FixedHeader:     packet.FixedHeader{Type: packet.Disconnect},
			ProtocolVersion: packet.Version5,
			ReasonCode:      code.Code,
		}
		if !c.omitReasonString.Load() {
			ack.Properties.ReasonString = code.Reason
			ack.Properties.ReasonStringFlag = code.Reason != ""
		}
		c.enqueue(ack)
	}
	s.log.Debug("connection failed", "conn", c.id, "error", err)
	c.stop()
}

// writeLoop is the outbound goroutine: it consumes the queue, batches
// packets for up to FlushInterval and flushes (architecture decision A2).
func (c *conn) writeLoop() {
	defer c.server.wg.Done()
	defer c.closeSocket()
	defer c.stop()

	w := bufio.NewWriterSize(c.nc, c.server.opts.ReadBufferSize)
	flushInterval := c.server.opts.FlushInterval
	for {
		select {
		case <-c.done:
			c.drainQueue(w)
			return
		case pk := <-c.queue:
			if !c.encodePacket(w, pk) {
				return
			}
			timer := time.NewTimer(flushInterval)
			for batching := true; batching; {
				select {
				case pk := <-c.queue:
					if !c.encodePacket(w, pk) {
						timer.Stop()
						return
					}
				case <-timer.C:
					batching = false
				case <-c.done:
					timer.Stop()
					c.drainQueue(w)
					return
				}
			}
			if err := w.Flush(); err != nil {
				c.server.log.Debug("write failed", "conn", c.id, "error", err)
				return
			}
		}
	}
}

// encodePacket encodes pk into w, reporting whether the writer may
// continue. Encoding failures are counted and skipped; write failures end
// the loop.
func (c *conn) encodePacket(w *bufio.Writer, pk *packet.Packet) bool {
	b, err := pk.Encode()
	if err != nil {
		c.server.log.Warn("dropping packet that failed to encode",
			"conn", c.id, "type", packet.TypeNames[pk.FixedHeader.Type], "error", err)
		c.server.Stats.MessagesDropped.Add(1)
		return true
	}
	if _, err := w.Write(b); err != nil {
		c.server.log.Debug("write failed", "conn", c.id, "error", err)
		return false
	}
	c.server.Stats.BytesSent.Add(int64(len(b)))
	if pk.FixedHeader.Type == packet.Publish {
		c.server.Stats.AddSent(pk.FixedHeader.Qos)
	}
	return true
}

// drainQueue flushes whatever remains queued so final control packets (the
// shutdown or protocol error DISCONNECT) reach the peer before close.
func (c *conn) drainQueue(w *bufio.Writer) {
	_ = c.nc.SetWriteDeadline(c.server.now().Add(2 * time.Second))
	for {
		select {
		case pk := <-c.queue:
			if !c.encodePacket(w, pk) {
				return
			}
		default:
			_ = w.Flush()
			return
		}
	}
}

// enqueue queues pk for the write loop. QoS 0 PUBLISH packets are dropped
// with a counter bump when the queue is full; every other packet blocks
// until queued or the connection stops, because QoS 1/2 messages and
// control packets must not be lost (architecture section 3).
func (c *conn) enqueue(pk *packet.Packet) {
	if pk.FixedHeader.Type == packet.Publish && pk.FixedHeader.Qos == 0 {
		select {
		case c.queue <- pk:
		case <-c.done:
		default:
			c.server.Stats.MessagesDropped.Add(1)
		}
		return
	}
	select {
	case c.queue <- pk:
	case <-c.done:
	}
}
