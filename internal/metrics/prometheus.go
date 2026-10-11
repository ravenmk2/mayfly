package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ravenmk2/mayfly/internal/auth"
)

// PromOptions configures the Prometheus handler.
type PromOptions struct {
	// AuthStats reports the per-source authentication/authorization chain
	// outcomes (auth.Manager.Stats); nil exports the broker-level aggregate
	// mayfly_auth_results_total{result} instead.
	AuthStats func() []auth.SourceStat
}

// NewHandler returns the /metrics handler exposing the mayfly_* family plus
// the Go runtime collector (docs/design/observability.md section 3).
func NewHandler(m *Metrics, opts PromOptions) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newCollector(m, opts.AuthStats), collectors.NewGoCollector())
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// collector reads the Metrics atomics on every scrape.
type collector struct {
	m         *Metrics
	authStats func() []auth.SourceStat

	buildInfo      *prometheus.Desc
	uptime         *prometheus.Desc
	connections    *prometheus.Desc
	connectionsTot *prometheus.Desc
	received       *prometheus.Desc
	sent           *prometheus.Desc
	dropped        *prometheus.Desc
	oversized      *prometheus.Desc
	bytesTotal     *prometheus.Desc
	subscriptions  *prometheus.Desc
	retained       *prometheus.Desc
	inflight       *prometheus.Desc
	authResults    *prometheus.Desc
	authSrcResults *prometheus.Desc
	aclResults     *prometheus.Desc
	storeWriteErrs *prometheus.Desc
	goroutines     *prometheus.Desc
	heapAlloc      *prometheus.Desc
}

func newCollector(m *Metrics, authStats func() []auth.SourceStat) *collector {
	qos := []string{"qos"}
	return &collector{
		m:         m,
		authStats: authStats,

		buildInfo: prometheus.NewDesc("mayfly_build_info",
			"Build information; the value is always 1.", []string{"version"}, nil),
		uptime: prometheus.NewDesc("mayfly_uptime_seconds",
			"Seconds since the broker started.", nil, nil),
		connections: prometheus.NewDesc("mayfly_connections",
			"Currently open connections.", nil, nil),
		connectionsTot: prometheus.NewDesc("mayfly_connections_total",
			"Connections accepted since startup.", nil, nil),
		received: prometheus.NewDesc("mayfly_messages_received_total",
			"Deliverable PUBLISH packets received from clients.", qos, nil),
		sent: prometheus.NewDesc("mayfly_messages_sent_total",
			"PUBLISH packets written to clients.", qos, nil),
		dropped: prometheus.NewDesc("mayfly_messages_dropped_total",
			"Messages dropped to backpressure or authorization.", nil, nil),
		oversized: prometheus.NewDesc("mayfly_messages_oversized_total",
			"Messages dropped for exceeding the receiving client's maximum packet size.", nil, nil),
		bytesTotal: prometheus.NewDesc("mayfly_bytes_total",
			"Control packet bytes on the wire.", []string{"direction"}, nil),
		subscriptions: prometheus.NewDesc("mayfly_subscriptions",
			"Stored subscriptions.", nil, nil),
		retained: prometheus.NewDesc("mayfly_retained",
			"Stored retained messages.", nil, nil),
		inflight: prometheus.NewDesc("mayfly_inflight",
			"Unacknowledged QoS 1/2 messages across all sessions.", nil, nil),
		authResults: prometheus.NewDesc("mayfly_auth_results_total",
			"CONNECT authentication outcomes.", []string{"result"}, nil),
		authSrcResults: prometheus.NewDesc("mayfly_auth_source_results_total",
			"Per-source authentication/authorization chain outcomes.",
			[]string{"source", "chain", "result"}, nil),
		aclResults: prometheus.NewDesc("mayfly_acl_results_total",
			"Publish/subscribe authorization verdicts.", []string{"result"}, nil),
		storeWriteErrs: prometheus.NewDesc("mayfly_store_write_errors_total",
			"Store write ops dropped after the batch retries ran out.", nil, nil),
		goroutines: prometheus.NewDesc("mayfly_goroutines",
			"Goroutines, sampled on the $SYS tick.", nil, nil),
		heapAlloc: prometheus.NewDesc("mayfly_heap_alloc_bytes",
			"Heap bytes in use, sampled on the $SYS tick.", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	c.collect(ch, nil)
}

// Collect implements prometheus.Collector.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	c.collect(nil, ch)
}

// collect serves Describe (descs only) and Collect (metrics only).
func (c *collector) collect(descCh chan<- *prometheus.Desc, metricCh chan<- prometheus.Metric) {
	emit := func(d *prometheus.Desc, v prometheus.ValueType, value float64, labels ...string) {
		if descCh != nil {
			descCh <- d
			return
		}
		metricCh <- prometheus.MustNewConstMetric(d, v, value, labels...)
	}

	m := c.m
	emit(c.buildInfo, prometheus.GaugeValue, 1, m.Version())
	emit(c.uptime, prometheus.GaugeValue, m.Uptime(time.Now()).Seconds())
	emit(c.connections, prometheus.GaugeValue, float64(m.ConnectionsCurrent.Load()))
	emit(c.connectionsTot, prometheus.CounterValue, float64(m.ConnectionsTotal.Load()))
	for qos := range 3 {
		q := string(rune('0' + qos))
		emit(c.received, prometheus.CounterValue, float64(m.Received(byte(qos))), q)
		emit(c.sent, prometheus.CounterValue, float64(m.Sent(byte(qos))), q)
	}
	emit(c.dropped, prometheus.CounterValue, float64(m.MessagesDropped.Load()))
	emit(c.oversized, prometheus.CounterValue, float64(m.MessagesOversized.Load()))
	emit(c.bytesTotal, prometheus.CounterValue, float64(m.BytesReceived.Load()), "received")
	emit(c.bytesTotal, prometheus.CounterValue, float64(m.BytesSent.Load()), "sent")
	emit(c.subscriptions, prometheus.GaugeValue, float64(m.Subscriptions.Load()))
	emit(c.retained, prometheus.GaugeValue, float64(m.Retained.Load()))
	emit(c.inflight, prometheus.GaugeValue, float64(m.Inflight.Load()))
	if c.authStats != nil {
		for _, st := range c.authStats() {
			emit(c.authSrcResults, prometheus.CounterValue, float64(st.Allow), st.Source, st.Chain, "allow")
			emit(c.authSrcResults, prometheus.CounterValue, float64(st.Deny), st.Source, st.Chain, "deny")
			emit(c.authSrcResults, prometheus.CounterValue, float64(st.Miss), st.Source, st.Chain, "miss")
			emit(c.authSrcResults, prometheus.CounterValue, float64(st.Fail), st.Source, st.Chain, "fail")
		}
	} else {
		emit(c.authResults, prometheus.CounterValue, float64(m.AuthSucceeded.Load()), "success")
		emit(c.authResults, prometheus.CounterValue, float64(m.AuthFailures.Load()), "failure")
	}
	emit(c.aclResults, prometheus.CounterValue, float64(m.ACLAllowed.Load()), "allow")
	emit(c.aclResults, prometheus.CounterValue, float64(m.ACLDenied.Load()), "deny")
	emit(c.storeWriteErrs, prometheus.CounterValue, float64(m.StoreWriteErrors.Load()))
	emit(c.goroutines, prometheus.GaugeValue, float64(m.Goroutines.Load()))
	emit(c.heapAlloc, prometheus.GaugeValue, float64(m.HeapAlloc.Load()))
}
