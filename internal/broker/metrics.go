package broker

import (
	"fmt"
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
	"github.com/ravenmk2/mayfly/internal/store"
	"github.com/ravenmk2/mayfly/internal/topics"
)

// Publish injects a broker-originated QoS 0 message into the retain and
// delivery path; the $SYS publisher goes through it. Internal traffic does
// not count towards the client message counters.
func (s *Server) Publish(topic string, payload []byte, retain bool) error {
	return s.PublishQoS(topic, payload, 0, retain)
}

// PublishQoS injects a broker-originated message with the requested QoS into
// the normal retain and delivery path (the admin REST API publishes through
// it). Each matching subscription receives the message at
// min(qos, subscription qos) [MQTT-3.8.4-3]; the QoS 1/2 acknowledgement
// handshake runs on the subscriber side like any routed publication.
// Internal traffic does not count towards the client message counters.
func (s *Server) PublishQoS(topic string, payload []byte, qos byte, retain bool) error {
	if !topics.IsValidTopicName(topic) {
		return fmt.Errorf("broker: invalid topic name %q", topic)
	}
	if qos > 2 {
		return fmt.Errorf("broker: invalid qos %d", qos)
	}
	now := s.now()
	pk := packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Publish, Qos: qos, Retain: retain},
		TopicName:   topic,
		Payload:     payload,
		Created:     now,
	}
	if retain {
		s.retain(pk)
	}
	s.deliver(nil, &pk, now)
	return nil
}

// RefreshMetrics copies the gauges that cannot be maintained on the hot path
// into Stats: subscription and retained counts live in the topics index,
// inflight counts live in the sessions, and store write errors live in the
// BatchWriter. The $SYS publisher calls it on every tick
// (docs/design/observability.md).
func (s *Server) RefreshMetrics() {
	s.Stats.Subscriptions.Store(s.topics.SubscriptionCount())
	s.Stats.Retained.Store(int64(s.topics.RetainedCount()))

	var inflight int64
	s.mu.RLock()
	for _, c := range s.clients {
		if cl := c.sessionClient(); cl != nil {
			inflight += int64(cl.OutboundLen() + cl.InboundLen())
		}
	}
	s.mu.RUnlock()
	s.Stats.Inflight.Store(inflight)

	if bw, ok := s.getStore().(*store.BatchWriter); ok {
		s.Stats.StoreWriteErrors.Store(bw.WriteErrors.Load())
	}
}

// Uptime reports how long the broker has been running.
func (s *Server) Uptime() time.Duration { return s.Stats.Uptime(s.now()) }
