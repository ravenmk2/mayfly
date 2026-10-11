package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqldrv "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// Backend identifies the SQL dialect of a store database. MySQL and SQLite
// share one SQL logic; dialect differences are confined to the DDL and the
// upsert statement prefix (docs/design/persistence.md section 2).
type Backend int

const (
	BackendSQLite Backend = iota
	BackendMySQL
)

// String returns the backend name for logs and config.
func (b Backend) String() string {
	switch b {
	case BackendSQLite:
		return "sqlite"
	case BackendMySQL:
		return "mysql"
	default:
		return "unknown"
	}
}

// openDB parses a DSN of the form "sqlite:<path-or-file-URI>" or
// "mysql:<go-sql-driver DSN>" and opens the pool, mirroring the auth source
// DSN handling (docs/design/auth.md section 1).
func openDB(dsn string) (*sql.DB, Backend, error) {
	scheme, rest, ok := strings.Cut(dsn, ":")
	if !ok || rest == "" {
		return nil, 0, fmt.Errorf("store: dsn %q lacks a sqlite:/mysql: scheme", dsn)
	}
	switch scheme {
	case "sqlite":
		db, err := sql.Open("sqlite", rest)
		if err != nil {
			return nil, 0, fmt.Errorf("store: open sqlite: %w", err)
		}
		if rest == ":memory:" || strings.HasPrefix(rest, "file::memory:") {
			// A pooled in-memory database vanishes per connection.
			db.SetMaxOpenConns(1)
		}
		return db, BackendSQLite, nil
	case "mysql":
		cfg, err := mysqldrv.ParseDSN(rest)
		if err != nil {
			return nil, 0, fmt.Errorf("store: parse mysql dsn: %w", err)
		}
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return nil, 0, fmt.Errorf("store: open mysql: %w", err)
		}
		return db, BackendMySQL, nil
	default:
		return nil, 0, fmt.Errorf("store: unsupported dsn scheme %q (want sqlite|mysql)", scheme)
	}
}

// Schema DDL per dialect (docs/design/persistence.md section 4). Migrations
// are recorded in mf_schema_migrations — a table private to the store so a
// database shared with the auth source, which versions its own
// schema_migrations, migrates independently — and applied at most once, so
// startup is idempotent. MySQL widens the packet columns to MEDIUMBLOB
// because a BLOB tops out at 64 KiB while the broker default maximum packet
// size is 1 MiB.
var migrations = map[Backend][]string{
	BackendSQLite: {
		`CREATE TABLE mf_session (
			client_id      VARCHAR(255) PRIMARY KEY,
			session_expiry INT NOT NULL,
			created_at     BIGINT NOT NULL,
			stopped_at     BIGINT NOT NULL
		)`,
		`CREATE TABLE mf_subscription (
			client_id VARCHAR(255) NOT NULL,
			filter    VARCHAR(512) NOT NULL,
			qos       TINYINT NOT NULL,
			no_local  BOOLEAN NOT NULL DEFAULT FALSE,
			retain_as_published BOOLEAN NOT NULL DEFAULT FALSE,
			retain_handling TINYINT NOT NULL DEFAULT 0,
			sub_id    INT NULL,
			PRIMARY KEY (client_id, filter)
		)`,
		`CREATE TABLE mf_inflight (
			client_id VARCHAR(255) NOT NULL,
			packet_id INT NOT NULL,
			direction TINYINT NOT NULL,
			packet    BLOB NOT NULL,
			expiry    BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (client_id, direction, packet_id)
		)`,
		`CREATE TABLE mf_retained (
			topic  VARCHAR(512) PRIMARY KEY,
			packet BLOB NOT NULL,
			expiry BIGINT NOT NULL DEFAULT 0
		)`,
	},
	BackendMySQL: {
		`CREATE TABLE mf_session (
			client_id      VARCHAR(255) PRIMARY KEY,
			session_expiry INT NOT NULL,
			created_at     BIGINT NOT NULL,
			stopped_at     BIGINT NOT NULL
		)`,
		`CREATE TABLE mf_subscription (
			client_id VARCHAR(255) NOT NULL,
			filter    VARCHAR(512) NOT NULL,
			qos       TINYINT NOT NULL,
			no_local  BOOLEAN NOT NULL DEFAULT FALSE,
			retain_as_published BOOLEAN NOT NULL DEFAULT FALSE,
			retain_handling TINYINT NOT NULL DEFAULT 0,
			sub_id    INT NULL,
			PRIMARY KEY (client_id, filter)
		)`,
		`CREATE TABLE mf_inflight (
			client_id VARCHAR(255) NOT NULL,
			packet_id INT NOT NULL,
			direction TINYINT NOT NULL,
			packet    MEDIUMBLOB NOT NULL,
			expiry    BIGINT NOT NULL DEFAULT 0,
			PRIMARY KEY (client_id, direction, packet_id)
		)`,
		`CREATE TABLE mf_retained (
			topic  VARCHAR(512) PRIMARY KEY,
			packet MEDIUMBLOB NOT NULL,
			expiry BIGINT NOT NULL DEFAULT 0
		)`,
	},
}

// migrate applies pending schema migrations inside one transaction.
func migrate(ctx context.Context, db *sql.DB, backend Backend) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS mf_schema_migrations (
		version    INTEGER PRIMARY KEY,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("store: create mf_schema_migrations: %w", err)
	}
	var applied int
	row := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM mf_schema_migrations`)
	if err := row.Scan(&applied); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	if applied >= 1 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range migrations[backend] {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("store: apply migration 1: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mf_schema_migrations (version) VALUES (1)`); err != nil {
		return fmt.Errorf("store: record migration 1: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit migration: %w", err)
	}
	return nil
}

// SQLStore is the MySQL/SQLite backed Store. Both dialects accept '?'
// placeholders; the only divergence is the upsert prefix (INSERT OR REPLACE
// vs REPLACE INTO) and the DDL above.
type SQLStore struct {
	db      *sql.DB
	backend Backend
	replace string // dialect upsert prefix
}

// Open parses the DSN, opens the pool and migrates the schema.
func Open(ctx context.Context, dsn string) (*SQLStore, error) {
	db, backend, err := openDB(dsn)
	if err != nil {
		return nil, err
	}
	if err := migrate(ctx, db, backend); err != nil {
		_ = db.Close()
		return nil, err
	}
	replace := "INSERT OR REPLACE"
	if backend == BackendMySQL {
		replace = "REPLACE"
	}
	return &SQLStore{db: db, backend: backend, replace: replace}, nil
}

// Backend reports the SQL dialect in use.
func (s *SQLStore) Backend() Backend { return s.backend }

// Close releases the connection pool.
func (s *SQLStore) Close() error { return s.db.Close() }

func (s *SQLStore) Sessions() SessionStore           { return sqlSessions{s} }
func (s *SQLStore) Subscriptions() SubscriptionStore { return sqlSubs{s} }
func (s *SQLStore) Inflight() InflightStore          { return sqlInflight{s} }
func (s *SQLStore) Retained() RetainedStore          { return sqlRetained{s} }

// execer is satisfied by *sql.DB and *sql.Tx so single writes and batched
// writes share one statement set.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// applyOps commits a batch of write ops in one transaction (batchStore).
func (s *SQLStore) applyOps(ctx context.Context, ops []op) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin batch: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, o := range ops {
		if err := s.execOp(ctx, tx, o); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit batch: %w", err)
	}
	return nil
}

// execOp applies one write op.
func (s *SQLStore) execOp(ctx context.Context, e execer, o op) error {
	var err error
	switch o.kind {
	case opPutSession:
		_, err = e.ExecContext(ctx,
			s.replace+` INTO mf_session (client_id, session_expiry, created_at, stopped_at) VALUES (?, ?, ?, ?)`,
			o.session.ClientID, o.session.Expiry, unixNanos(o.session.CreatedAt), unixNanos(o.session.StoppedAt))
	case opDeleteSession:
		_, err = e.ExecContext(ctx, `DELETE FROM mf_session WHERE client_id = ?`, o.session.ClientID)
	case opPutSubscription:
		var subID any
		if o.sub.Identifier != 0 {
			subID = o.sub.Identifier
		}
		_, err = e.ExecContext(ctx,
			s.replace+` INTO mf_subscription (client_id, filter, qos, no_local, retain_as_published, retain_handling, sub_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			o.sub.ClientID, o.sub.Filter, o.sub.Qos, o.sub.NoLocal, o.sub.RetainAsPublished, o.sub.RetainHandling, subID)
	case opDeleteSubscription:
		_, err = e.ExecContext(ctx, `DELETE FROM mf_subscription WHERE client_id = ? AND filter = ?`,
			o.sub.ClientID, o.sub.Filter)
	case opDeleteSubscriptionsByClient:
		_, err = e.ExecContext(ctx, `DELETE FROM mf_subscription WHERE client_id = ?`, o.sub.ClientID)
	case opPutInflight:
		var raw []byte
		raw, err = encodeInflight(o.inflight)
		if err != nil {
			return fmt.Errorf("store: encode inflight packet: %w", err)
		}
		_, err = e.ExecContext(ctx,
			s.replace+` INTO mf_inflight (client_id, packet_id, direction, packet, expiry) VALUES (?, ?, ?, ?, ?)`,
			o.inflight.ClientID, o.inflight.PacketID, o.inflight.Direction, raw, unixNanos(o.inflight.Expiry))
	case opDeleteInflight:
		_, err = e.ExecContext(ctx,
			`DELETE FROM mf_inflight WHERE client_id = ? AND direction = ? AND packet_id = ?`,
			o.inflight.ClientID, o.inflight.Direction, o.inflight.PacketID)
	case opDeleteInflightByClient:
		_, err = e.ExecContext(ctx, `DELETE FROM mf_inflight WHERE client_id = ?`, o.inflight.ClientID)
	case opPutRetained:
		// A retained packet carries no Packet Identifier (one is assigned per
		// subscriber at delivery); the wire codec requires one for QoS 1/2, so
		// a placeholder is encoded and ignored on restore.
		pk := o.retained.Packet
		if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
			pk.PacketID = 1
		}
		var raw []byte
		raw, err = encodePacket(pk)
		if err != nil {
			return fmt.Errorf("store: encode retained packet: %w", err)
		}
		_, err = e.ExecContext(ctx,
			s.replace+` INTO mf_retained (topic, packet, expiry) VALUES (?, ?, ?)`,
			o.retained.Topic, raw, unixNanos(o.retained.Expiry))
	case opDeleteRetained:
		_, err = e.ExecContext(ctx, `DELETE FROM mf_retained WHERE topic = ?`, o.retained.Topic)
	}
	if err != nil {
		return fmt.Errorf("store: apply %s: %w", o.kind, err)
	}
	return nil
}

// unixNanos converts a time to its stored BIGINT form; the zero time maps
// to 0 (no timestamp / no expiry).
func unixNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// fromUnixNanos is the inverse of unixNanos.
func fromUnixNanos(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

type sqlSessions struct{ s *SQLStore }

func (ss sqlSessions) Put(ctx context.Context, sess Session) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opPutSession, session: sess})
}

func (ss sqlSessions) Delete(ctx context.Context, clientID string) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteSession, session: Session{ClientID: clientID}})
}

func (ss sqlSessions) Get(ctx context.Context, clientID string) (Session, bool, error) {
	var sess Session
	var created, stopped int64
	err := ss.s.db.QueryRowContext(ctx,
		`SELECT client_id, session_expiry, created_at, stopped_at FROM mf_session WHERE client_id = ?`,
		clientID).Scan(&sess.ClientID, &sess.Expiry, &created, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, false, nil
	}
	if err != nil {
		return Session{}, false, fmt.Errorf("store: get session: %w", err)
	}
	sess.CreatedAt = fromUnixNanos(created)
	sess.StoppedAt = fromUnixNanos(stopped)
	return sess, true, nil
}

func (ss sqlSessions) Iterate(ctx context.Context, fn func(Session) bool) error {
	rows, err := ss.s.db.QueryContext(ctx,
		`SELECT client_id, session_expiry, created_at, stopped_at FROM mf_session ORDER BY client_id`)
	if err != nil {
		return fmt.Errorf("store: iterate sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sess Session
		var created, stopped int64
		if err := rows.Scan(&sess.ClientID, &sess.Expiry, &created, &stopped); err != nil {
			return fmt.Errorf("store: scan session: %w", err)
		}
		sess.CreatedAt = fromUnixNanos(created)
		sess.StoppedAt = fromUnixNanos(stopped)
		if !fn(sess) {
			return nil
		}
	}
	return rows.Err()
}

type sqlSubs struct{ s *SQLStore }

func (ss sqlSubs) Put(ctx context.Context, sub Subscription) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opPutSubscription, sub: sub})
}

func (ss sqlSubs) Delete(ctx context.Context, clientID, filter string) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteSubscription,
		sub: Subscription{ClientID: clientID, Filter: filter}})
}

func (ss sqlSubs) DeleteByClient(ctx context.Context, clientID string) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteSubscriptionsByClient,
		sub: Subscription{ClientID: clientID}})
}

func (ss sqlSubs) Iterate(ctx context.Context, fn func(Subscription) bool) error {
	rows, err := ss.s.db.QueryContext(ctx,
		`SELECT client_id, filter, qos, no_local, retain_as_published, retain_handling, sub_id
		 FROM mf_subscription ORDER BY client_id, filter`)
	if err != nil {
		return fmt.Errorf("store: iterate subscriptions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sub Subscription
		var subID sql.NullInt64
		if err := rows.Scan(&sub.ClientID, &sub.Filter, &sub.Qos,
			&sub.NoLocal, &sub.RetainAsPublished, &sub.RetainHandling, &subID); err != nil {
			return fmt.Errorf("store: scan subscription: %w", err)
		}
		if subID.Valid && subID.Int64 > 0 {
			sub.Identifier = uint32(subID.Int64)
		}
		if !fn(sub) {
			return nil
		}
	}
	return rows.Err()
}

// encodeInflight serializes an inflight row: the version-prefixed wire
// packet followed by the outbound bookkeeping flags (sent, PUBREL stage)
// packed into a trailer byte.
func encodeInflight(msg InflightMessage) ([]byte, error) {
	raw, err := encodePacket(msg.Packet)
	if err != nil {
		return nil, err
	}
	var flags byte
	if msg.Sent {
		flags |= 1
	}
	if msg.Pubrel {
		flags |= 2
	}
	return append(raw, flags), nil
}

func decodeInflight(b []byte) (pk packet.Packet, sent, pubrel bool, err error) {
	if len(b) < 3 {
		return packet.Packet{}, false, false, errors.New("store: persisted inflight packet truncated")
	}
	flags := b[len(b)-1]
	pk, err = decodePacket(b[:len(b)-1])
	if err != nil {
		return packet.Packet{}, false, false, err
	}
	return pk, flags&1 != 0, flags&2 != 0, nil
}

type sqlInflight struct{ s *SQLStore }

func (ss sqlInflight) Put(ctx context.Context, msg InflightMessage) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opPutInflight, inflight: msg})
}

func (ss sqlInflight) Delete(ctx context.Context, clientID string, direction Direction, packetID uint16) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteInflight,
		inflight: InflightMessage{ClientID: clientID, Direction: direction, PacketID: packetID}})
}

func (ss sqlInflight) DeleteByClient(ctx context.Context, clientID string) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteInflightByClient,
		inflight: InflightMessage{ClientID: clientID}})
}

func (ss sqlInflight) Iterate(ctx context.Context, fn func(InflightMessage) bool) error {
	rows, err := ss.s.db.QueryContext(ctx,
		`SELECT client_id, packet_id, direction, packet, expiry FROM mf_inflight
		 ORDER BY client_id, direction, packet_id`)
	if err != nil {
		return fmt.Errorf("store: iterate inflight: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var msg InflightMessage
		var raw []byte
		var expiry int64
		if err := rows.Scan(&msg.ClientID, &msg.PacketID, &msg.Direction, &raw, &expiry); err != nil {
			return fmt.Errorf("store: scan inflight: %w", err)
		}
		pk, sent, pubrel, err := decodeInflight(raw)
		if err != nil {
			return err
		}
		msg.Packet = pk
		msg.Sent = sent
		msg.Pubrel = pubrel
		msg.Expiry = fromUnixNanos(expiry)
		if !fn(msg) {
			return nil
		}
	}
	return rows.Err()
}

type sqlRetained struct{ s *SQLStore }

func (ss sqlRetained) Put(ctx context.Context, msg RetainedMessage) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opPutRetained, retained: msg})
}

func (ss sqlRetained) Delete(ctx context.Context, topic string) error {
	return ss.s.execOp(ctx, ss.s.db, op{kind: opDeleteRetained, retained: RetainedMessage{Topic: topic}})
}

func (ss sqlRetained) Iterate(ctx context.Context, fn func(RetainedMessage) bool) error {
	rows, err := ss.s.db.QueryContext(ctx,
		`SELECT topic, packet, expiry FROM mf_retained ORDER BY topic`)
	if err != nil {
		return fmt.Errorf("store: iterate retained: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var msg RetainedMessage
		var raw []byte
		var expiry int64
		if err := rows.Scan(&msg.Topic, &raw, &expiry); err != nil {
			return fmt.Errorf("store: scan retained: %w", err)
		}
		pk, err := decodePacket(raw)
		if err != nil {
			return err
		}
		msg.Packet = pk
		msg.Expiry = fromUnixNanos(expiry)
		if !fn(msg) {
			return nil
		}
	}
	return rows.Err()
}
