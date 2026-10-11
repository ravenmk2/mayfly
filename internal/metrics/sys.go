package metrics

import (
	"context"
	"log/slog"
	"strconv"
	"time"
)

// DefaultSysInterval is the default $SYS publish cadence
// (docs/design/observability.md section 2).
const DefaultSysInterval = 10 * time.Second

// SysPrefix is the root of the broker status topics.
const SysPrefix = "$SYS/broker/"

// SysOptions configures the SysPublisher; zero values select the defaults.
type SysOptions struct {
	// Interval is the publish cadence; zero selects DefaultSysInterval and a
	// negative value disables the publisher (Run returns immediately).
	Interval time.Duration
	// Publish retains and delivers one message through the broker's internal
	// publish entry; it is required.
	Publish func(topic string, payload []byte) error
	// Refresh copies the off-hot-path gauges (subscriptions, retained,
	// inflight, store write errors) into the Metrics before each round; it
	// may be nil.
	Refresh func()
	// Logger receives publish failure records; nil selects slog.Default().
	Logger *slog.Logger
}

// SysPublisher periodically republishes the broker status as retained
// $SYS/broker/... messages (docs/design/observability.md section 2).
// Subscribers to $SYS/broker/# see every round live; late subscribers read
// the retained values. $SYS topics are never matched by top-level
// wildcards [MQTT-4.7.2-1].
type SysPublisher struct {
	m      *Metrics
	opts   SysOptions
	log    *slog.Logger
	topics []sysTopic
}

// sysTopic binds one $SYS topic to its value extractor.
type sysTopic struct {
	name  string
	value func(s Snapshot) string
}

// NewSysPublisher builds the publisher over m.
func NewSysPublisher(m *Metrics, opts SysOptions) *SysPublisher {
	if opts.Interval == 0 {
		opts.Interval = DefaultSysInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &SysPublisher{m: m, opts: opts, log: opts.Logger, topics: sysTopics(m)}
}

// sysTopics is the fixed $SYS topic set; values are decimal integers except
// the version string.
func sysTopics(m *Metrics) []sysTopic {
	i := func(f func(s Snapshot) int64) func(Snapshot) string {
		return func(s Snapshot) string { return strconv.FormatInt(f(s), 10) }
	}
	return []sysTopic{
		{"uptime", func(s Snapshot) string { return strconv.FormatInt(int64(s.Uptime/time.Second), 10) }},
		{"version", func(Snapshot) string { return m.Version() }},
		{"clients/connected", i(func(s Snapshot) int64 { return s.ConnectionsCurrent })},
		{"clients/total", i(func(s Snapshot) int64 { return s.ConnectionsTotal })},
		{"messages/received", i(func(s Snapshot) int64 { return s.MessagesReceived })},
		{"messages/sent", i(func(s Snapshot) int64 { return s.MessagesSent })},
		{"messages/dropped", i(func(s Snapshot) int64 { return s.MessagesDropped })},
		{"messages/oversized", i(func(s Snapshot) int64 { return s.MessagesOversized })},
		{"messages/inflight", i(func(s Snapshot) int64 { return s.Inflight })},
		{"bytes/received", i(func(s Snapshot) int64 { return s.BytesReceived })},
		{"bytes/sent", i(func(s Snapshot) int64 { return s.BytesSent })},
		{"subscriptions/count", i(func(s Snapshot) int64 { return s.Subscriptions })},
		{"retained/count", i(func(s Snapshot) int64 { return s.Retained })},
		{"auth/succeeded", i(func(s Snapshot) int64 { return s.AuthSucceeded })},
		{"auth/failed", i(func(s Snapshot) int64 { return s.AuthFailures })},
		{"acl/allowed", i(func(s Snapshot) int64 { return s.ACLAllowed })},
		{"acl/denied", i(func(s Snapshot) int64 { return s.ACLDenied })},
		{"store/write_errors", i(func(s Snapshot) int64 { return s.StoreWriteErrors })},
		{"runtime/goroutines", i(func(s Snapshot) int64 { return s.Goroutines })},
		{"runtime/heap_alloc", i(func(s Snapshot) int64 { return s.HeapAlloc })},
	}
}

// Topics returns the published $SYS topic names (without the prefix).
func (p *SysPublisher) Topics() []string {
	out := make([]string, len(p.topics))
	for i, t := range p.topics {
		out[i] = SysPrefix + t.name
	}
	return out
}

// Disabled reports whether the publisher was configured off.
func (p *SysPublisher) Disabled() bool { return p.opts.Interval < 0 }

// Run publishes one round immediately and then on every tick until ctx is
// cancelled. A negative interval disables the publisher entirely.
func (p *SysPublisher) Run(ctx context.Context) {
	if p.Disabled() || p.opts.Publish == nil {
		return
	}
	p.PublishNow()
	t := time.NewTicker(p.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.PublishNow()
		}
	}
}

// PublishNow refreshes the gauges — runtime sampling included, so
// ReadMemStats runs only on this cadence — and publishes one round.
func (p *SysPublisher) PublishNow() {
	p.m.SampleRuntime()
	if p.opts.Refresh != nil {
		p.opts.Refresh()
	}
	snap := p.m.Snapshot()
	for _, t := range p.topics {
		if err := p.opts.Publish(SysPrefix+t.name, []byte(t.value(snap))); err != nil {
			p.log.Warn("$SYS publish failed", "topic", SysPrefix+t.name, "error", err)
		}
	}
}
