// Package metrics aggregates the broker's observable state: atomic hot-path
// counters, low-frequency gauges, the $SYS retained publisher and the
// Prometheus exposition (docs/design/observability.md).
package metrics

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Metrics holds every broker counter and gauge. All counters are atomics
// safe for concurrent hot-path updates; the gauges are refreshed by the
// $SYS tick (broker.RefreshMetrics and SampleRuntime), never on the hot
// path.
type Metrics struct {
	startedAt time.Time
	version   string

	// ConnectionsTotal counts every accepted connection;
	// ConnectionsCurrent is the number currently open.
	ConnectionsTotal   atomic.Int64
	ConnectionsCurrent atomic.Int64

	messagesReceived [3]atomic.Int64 // by QoS
	messagesSent     [3]atomic.Int64 // by QoS

	// MessagesDropped counts backpressure drops (full outbound queue,
	// undeliverable offline messages); MessagesOversized counts drops
	// because the receiving client declared a smaller Maximum Packet Size.
	MessagesDropped   atomic.Int64
	MessagesOversized atomic.Int64

	// BytesReceived/BytesSent count control packet bytes on the wire.
	BytesReceived atomic.Int64
	BytesSent     atomic.Int64

	// AuthFailures counts rejected CONNECTs; AuthSucceeded counts accepted
	// ones. The per-source breakdown lives in auth.Manager.Stats.
	AuthSucceeded atomic.Int64
	AuthFailures  atomic.Int64

	// ACLAllowed/ACLDenied count publish/subscribe authorization verdicts.
	ACLAllowed atomic.Int64
	ACLDenied  atomic.Int64

	// StoreWriteErrors mirrors store.BatchWriter.WriteErrors (ops dropped
	// after the batch retries ran out), refreshed on the $SYS tick.
	StoreWriteErrors atomic.Int64

	// Gauges refreshed by broker.RefreshMetrics on the $SYS tick.
	Subscriptions atomic.Int64
	Retained      atomic.Int64
	Inflight      atomic.Int64

	// Runtime gauges refreshed by SampleRuntime on the $SYS tick — the
	// broker deliberately avoids a per-second ReadMemStats.
	Goroutines atomic.Int64
	HeapAlloc  atomic.Int64
}

// New returns a Metrics stamped with the broker version and started now.
func New(version string) *Metrics {
	return &Metrics{startedAt: time.Now(), version: version}
}

// Version reports the broker version stamp.
func (m *Metrics) Version() string { return m.version }

// Uptime reports how long the broker has been running at now.
func (m *Metrics) Uptime(now time.Time) time.Duration { return now.Sub(m.startedAt) }

// AddReceived counts one deliverable inbound PUBLISH by its QoS.
func (m *Metrics) AddReceived(qos byte) { m.messagesReceived[clampQoS(qos)].Add(1) }

// AddSent counts one outbound PUBLISH written to the wire by its QoS.
func (m *Metrics) AddSent(qos byte) { m.messagesSent[clampQoS(qos)].Add(1) }

// Received returns the inbound PUBLISH count for one QoS level.
func (m *Metrics) Received(qos byte) int64 { return m.messagesReceived[clampQoS(qos)].Load() }

// Sent returns the outbound PUBLISH count for one QoS level.
func (m *Metrics) Sent(qos byte) int64 { return m.messagesSent[clampQoS(qos)].Load() }

// clampQoS keeps a corrupt QoS byte from escaping the per-level arrays.
func clampQoS(qos byte) byte {
	if qos > 2 {
		return 2
	}
	return qos
}

// SampleRuntime refreshes the runtime gauges; it is called on the $SYS tick,
// never per second or per scrape (docs/design/observability.md section 2).
func (m *Metrics) SampleRuntime() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.HeapAlloc.Store(int64(ms.HeapAlloc)) //nolint:gosec // heap sizes fit int64
	m.Goroutines.Store(int64(runtime.NumGoroutine()))
}

// Snapshot is a consistent-enough readout of every counter and gauge for
// the $SYS publisher and tests.
type Snapshot struct {
	Version string
	Uptime  time.Duration

	ConnectionsTotal   int64
	ConnectionsCurrent int64

	MessagesReceived  int64
	MessagesSent      int64
	ReceivedByQoS     [3]int64
	SentByQoS         [3]int64
	MessagesDropped   int64
	MessagesOversized int64

	BytesReceived int64
	BytesSent     int64

	AuthSucceeded    int64
	AuthFailures     int64
	ACLAllowed       int64
	ACLDenied        int64
	StoreWriteErrors int64

	Subscriptions int64
	Retained      int64
	Inflight      int64
	Goroutines    int64
	HeapAlloc     int64
}

// Snapshot reads every counter and gauge once.
func (m *Metrics) Snapshot() Snapshot {
	s := Snapshot{
		Version:            m.version,
		Uptime:             m.Uptime(time.Now()),
		ConnectionsTotal:   m.ConnectionsTotal.Load(),
		ConnectionsCurrent: m.ConnectionsCurrent.Load(),
		MessagesDropped:    m.MessagesDropped.Load(),
		MessagesOversized:  m.MessagesOversized.Load(),
		BytesReceived:      m.BytesReceived.Load(),
		BytesSent:          m.BytesSent.Load(),
		AuthSucceeded:      m.AuthSucceeded.Load(),
		AuthFailures:       m.AuthFailures.Load(),
		ACLAllowed:         m.ACLAllowed.Load(),
		ACLDenied:          m.ACLDenied.Load(),
		StoreWriteErrors:   m.StoreWriteErrors.Load(),
		Subscriptions:      m.Subscriptions.Load(),
		Retained:           m.Retained.Load(),
		Inflight:           m.Inflight.Load(),
		Goroutines:         m.Goroutines.Load(),
		HeapAlloc:          m.HeapAlloc.Load(),
	}
	for qos := range 3 {
		s.ReceivedByQoS[qos] = m.messagesReceived[qos].Load()
		s.SentByQoS[qos] = m.messagesSent[qos].Load()
		s.MessagesReceived += s.ReceivedByQoS[qos]
		s.MessagesSent += s.SentByQoS[qos]
	}
	return s
}
