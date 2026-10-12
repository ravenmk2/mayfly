package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqldrv "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// Backend identifies the SQL dialect of a db source. MySQL and SQLite share
// one SQL logic; dialect differences are confined to the schema DDL
// (docs/design/auth.md section 1).
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

// OpenDB parses a DSN of the form "sqlite:<path-or-file-URI>" or
// "mysql:<go-sql-driver DSN>" and opens the pool. MySQL DSNs are normalized
// with parseTime so created_at columns scan into time.Time.
func OpenDB(dsn string) (*sql.DB, Backend, error) {
	scheme, rest, ok := strings.Cut(dsn, ":")
	if !ok || rest == "" {
		return nil, 0, fmt.Errorf("auth: db dsn %q lacks a sqlite:/mysql: scheme", dsn)
	}
	switch scheme {
	case "sqlite":
		db, err := sql.Open("sqlite", rest)
		if err != nil {
			return nil, 0, fmt.Errorf("auth: open sqlite: %w", err)
		}
		// Serialize access on one connection: the broker store, the auth
		// source and the admin store may share one database file, and
		// concurrent pooled writers race into SQLITE_BUSY errors. WAL plus a
		// busy timeout guard against external lockers. A pooled in-memory
		// database also vanishes per connection, so it needs this too.
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			_ = db.Close()
			return nil, 0, fmt.Errorf("auth: enable WAL: %w", err)
		}
		if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
			_ = db.Close()
			return nil, 0, fmt.Errorf("auth: set busy timeout: %w", err)
		}
		return db, BackendSQLite, nil
	case "mysql":
		cfg, err := mysqldrv.ParseDSN(rest)
		if err != nil {
			return nil, 0, fmt.Errorf("auth: parse mysql dsn: %w", err)
		}
		cfg.ParseTime = true
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return nil, 0, fmt.Errorf("auth: open mysql: %w", err)
		}
		return db, BackendMySQL, nil
	default:
		return nil, 0, fmt.Errorf("auth: unsupported db scheme %q (want sqlite|mysql)", scheme)
	}
}

// Schema DDL per dialect (docs/design/auth.md section 4). Migrations are
// recorded in schema_migrations and applied at most once, so startup is
// idempotent.
var migrations = map[Backend][]string{
	BackendSQLite: {
		`CREATE TABLE mqtt_user (
			username      VARCHAR(255) PRIMARY KEY,
			password_hash VARCHAR(255) NOT NULL,
			superuser     BOOLEAN NOT NULL DEFAULT FALSE,
			created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE mqtt_acl (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			seq        INT NOT NULL,
			username   VARCHAR(255) NULL,
			clientid   VARCHAR(255) NULL,
			action     VARCHAR(8)  NOT NULL,
			permission VARCHAR(8)  NOT NULL,
			topic      VARCHAR(512) NOT NULL
		)`,
		`CREATE INDEX idx_acl_user ON mqtt_acl(username)`,
	},
	BackendMySQL: {
		`CREATE TABLE mqtt_user (
			username      VARCHAR(255) PRIMARY KEY,
			password_hash VARCHAR(255) NOT NULL,
			superuser     BOOLEAN NOT NULL DEFAULT FALSE,
			created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE mqtt_acl (
			id         BIGINT AUTO_INCREMENT PRIMARY KEY,
			seq        INT NOT NULL,
			username   VARCHAR(255) NULL,
			clientid   VARCHAR(255) NULL,
			action     VARCHAR(8)  NOT NULL,
			permission VARCHAR(8)  NOT NULL,
			topic      VARCHAR(512) NOT NULL
		)`,
		`CREATE INDEX idx_acl_user ON mqtt_acl(username)`,
	},
}

// migrate applies pending schema migrations inside one transaction.
func migrate(ctx context.Context, db *sql.DB, backend Backend) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("auth: create schema_migrations: %w", err)
	}
	// Check for the specific version row instead of MAX(version): the table
	// is shared with the admin schema, which records its own higher version
	// (docs/design/admin-api.md section 2).
	var applied int
	row := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = 1`)
	if err := row.Scan(&applied); err != nil {
		return fmt.Errorf("auth: read schema version: %w", err)
	}
	if applied > 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("auth: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range migrations[backend] {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("auth: apply migration 1: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (1)`); err != nil {
		return fmt.Errorf("auth: record migration 1: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("auth: commit migration: %w", err)
	}
	return nil
}

// User is the management view of one mqtt_user row.
type User struct {
	Username  string
	Superuser bool
	CreatedAt time.Time
}

// ACLRule is the management view of one mqtt_acl row. Empty Username and
// ClientID map to NULL (both empty = global rule).
type ACLRule struct {
	ID         int64
	Seq        int
	Username   string
	ClientID   string
	Action     Action
	Permission Permission
	Topic      string
}

// DBSource is a MySQL/SQLite backed source serving both chains. Reload
// rebuilds the user table and the compiled RuleSet from the database and
// swaps them atomically (RCU); when the database is unreachable the last
// good snapshot keeps serving, and a source that never loaded fails closed
// (docs/design/auth.md section 6). It also implements the management write
// interface consumed by the admin API.
type DBSource struct {
	db       *sql.DB
	backend  Backend
	migrated atomic.Bool
	users    atomic.Pointer[map[string]userRecord]
	rules    atomic.Pointer[RuleSet]
	gen      atomic.Uint64
}

// NewDBSource opens the pool and performs the initial migration and load.
// DSN parsing errors are fatal (nil source); migration and load failures
// are returned with a live fail-closed source so a later Reload can recover
// once the database comes back.
func NewDBSource(ctx context.Context, dsn string) (*DBSource, error) {
	db, backend, err := OpenDB(dsn)
	if err != nil {
		return nil, err
	}
	s := &DBSource{db: db, backend: backend}
	return s, s.Reload(ctx)
}

// Migrate applies the schema migrations to the database at dsn and closes
// the pool again. It serves setup flows that only need the schema, not a
// live source.
func Migrate(ctx context.Context, dsn string) error {
	db, backend, err := OpenDB(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return migrate(ctx, db, backend)
}

// Backend reports the SQL dialect in use.
func (s *DBSource) Backend() Backend { return s.backend }

// Close releases the connection pool.
func (s *DBSource) Close() error { return s.db.Close() }

// Reload migrates the schema when needed, rebuilds the in-memory snapshots
// from the database and swaps them atomically. On error the previous
// snapshots keep serving.
func (s *DBSource) Reload(ctx context.Context) error {
	if !s.migrated.Load() {
		if err := migrate(ctx, s.db, s.backend); err != nil {
			return err
		}
		s.migrated.Store(true)
	}
	users, err := s.loadUsers(ctx)
	if err != nil {
		return err
	}
	rules, err := s.loadRules(ctx, s.gen.Add(1))
	if err != nil {
		return err
	}
	s.users.Store(&users)
	s.rules.Store(rules)
	return nil
}

// Authenticate implements hook.Authenticator.
func (s *DBSource) Authenticate(_ context.Context, info hook.ConnectInfo) (hook.Result, error) {
	users := s.users.Load()
	if users == nil {
		return hook.Result{OK: false, Reason: "authentication source failure"}, errSourceUnavailable
	}
	return authenticateUsers(*users, info)
}

// Authorize implements hook.Authorizer.
func (s *DBSource) Authorize(_ context.Context, client hook.ClientInfo, op hook.Op, topic string) hook.Verdict {
	return authorizeRules(s.rules.Load(), client, op, topic)
}

func (s *DBSource) loadUsers(ctx context.Context) (map[string]userRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT username, password_hash, superuser FROM mqtt_user`)
	if err != nil {
		return nil, fmt.Errorf("auth: load users: %w", err)
	}
	defer func() { _ = rows.Close() }()
	users := map[string]userRecord{}
	for rows.Next() {
		var name, hash string
		var super bool
		if err := rows.Scan(&name, &hash, &super); err != nil {
			return nil, fmt.Errorf("auth: scan user: %w", err)
		}
		users[name] = userRecord{hash: hash, superuser: super}
	}
	return users, rows.Err()
}

func (s *DBSource) loadRules(ctx context.Context, generation uint64) (*RuleSet, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, username, clientid, action, permission, topic FROM mqtt_acl ORDER BY seq, id`)
	if err != nil {
		return nil, fmt.Errorf("auth: load acl: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var rules []Rule
	for rows.Next() {
		var (
			seq                int
			username, clientid sql.NullString
			action, perm       string
			topic              string
		)
		if err := rows.Scan(&seq, &username, &clientid, &action, &perm, &topic); err != nil {
			return nil, fmt.Errorf("auth: scan acl rule: %w", err)
		}
		a, err := parseAction(action)
		if err != nil {
			return nil, err
		}
		p, err := parsePermission(perm)
		if err != nil {
			return nil, err
		}
		rules = append(rules, Rule{
			Seq: seq, Username: username.String, ClientID: clientid.String,
			Action: a, Permission: p, Topic: topic,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return NewRuleSet(rules, generation)
}

// CreateUser inserts one mqtt_user row, hashing the plaintext password with
// bcrypt, and refreshes the in-memory snapshot.
func (s *DBSource) CreateUser(ctx context.Context, username, password string, superuser bool) error {
	if username == "" {
		return errors.New("auth: username is empty")
	}
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO mqtt_user (username, password_hash, superuser) VALUES (?, ?, ?)`,
		username, hash, superuser); err != nil {
		return fmt.Errorf("auth: create user: %w", err)
	}
	return s.Reload(ctx)
}

// UpdateUser changes the password (plaintext, re-hashed) and/or the
// superuser flag of an existing user. Nil fields are left unchanged.
func (s *DBSource) UpdateUser(ctx context.Context, username string, password *string, superuser *bool) error {
	if password != nil {
		hash, err := HashPassword(*password)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx,
			`UPDATE mqtt_user SET password_hash = ? WHERE username = ?`, hash, username); err != nil {
			return fmt.Errorf("auth: update user password: %w", err)
		}
	}
	if superuser != nil {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE mqtt_user SET superuser = ? WHERE username = ?`, *superuser, username); err != nil {
			return fmt.Errorf("auth: update user superuser: %w", err)
		}
	}
	return s.Reload(ctx)
}

// DeleteUser removes one user and refreshes the snapshot.
func (s *DBSource) DeleteUser(ctx context.Context, username string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM mqtt_user WHERE username = ?`, username); err != nil {
		return fmt.Errorf("auth: delete user: %w", err)
	}
	return s.Reload(ctx)
}

// GetUser returns one user by name.
func (s *DBSource) GetUser(ctx context.Context, username string) (User, error) {
	var u User
	err := s.db.QueryRowContext(ctx,
		`SELECT username, superuser, created_at FROM mqtt_user WHERE username = ?`, username).
		Scan(&u.Username, &u.Superuser, &u.CreatedAt)
	if err != nil {
		return User{}, fmt.Errorf("auth: get user: %w", err)
	}
	return u, nil
}

// ListUsers returns all users ordered by name.
func (s *DBSource) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT username, superuser, created_at FROM mqtt_user ORDER BY username`)
	if err != nil {
		return nil, fmt.Errorf("auth: list users: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.Username, &u.Superuser, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("auth: scan user row: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// CreateACLRule validates and inserts one rule, returning its id, and
// refreshes the compiled rule set.
func (s *DBSource) CreateACLRule(ctx context.Context, rule ACLRule) (int64, error) {
	if err := rule.validate(); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO mqtt_acl (seq, username, clientid, action, permission, topic) VALUES (?, ?, ?, ?, ?, ?)`,
		rule.Seq, nullString(rule.Username), nullString(rule.ClientID),
		actionString(rule.Action), permissionString(rule.Permission), rule.Topic)
	if err != nil {
		return 0, fmt.Errorf("auth: create acl rule: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("auth: acl rule id: %w", err)
	}
	return id, s.Reload(ctx)
}

// UpdateACLRule replaces the mutable fields of one rule and refreshes the
// compiled rule set.
func (s *DBSource) UpdateACLRule(ctx context.Context, rule ACLRule) error {
	if err := rule.validate(); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE mqtt_acl SET seq = ?, username = ?, clientid = ?, action = ?, permission = ?, topic = ? WHERE id = ?`,
		rule.Seq, nullString(rule.Username), nullString(rule.ClientID),
		actionString(rule.Action), permissionString(rule.Permission), rule.Topic, rule.ID); err != nil {
		return fmt.Errorf("auth: update acl rule: %w", err)
	}
	return s.Reload(ctx)
}

// DeleteACLRule removes one rule and refreshes the compiled rule set.
func (s *DBSource) DeleteACLRule(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM mqtt_acl WHERE id = ?`, id); err != nil {
		return fmt.Errorf("auth: delete acl rule: %w", err)
	}
	return s.Reload(ctx)
}

// ListACLRules returns all rules ordered by seq then id.
func (s *DBSource) ListACLRules(ctx context.Context) ([]ACLRule, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, seq, username, clientid, action, permission, topic FROM mqtt_acl ORDER BY seq, id`)
	if err != nil {
		return nil, fmt.Errorf("auth: list acl rules: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ACLRule
	for rows.Next() {
		var (
			r                  ACLRule
			username, clientid sql.NullString
			action, perm       string
		)
		if err := rows.Scan(&r.ID, &r.Seq, &username, &clientid, &action, &perm, &r.Topic); err != nil {
			return nil, fmt.Errorf("auth: scan acl row: %w", err)
		}
		a, err := parseAction(action)
		if err != nil {
			return nil, err
		}
		p, err := parsePermission(perm)
		if err != nil {
			return nil, err
		}
		r.Username, r.ClientID, r.Action, r.Permission = username.String, clientid.String, a, p
		out = append(out, r)
	}
	return out, rows.Err()
}

// validate checks a management-supplied rule, compiling the topic filter.
func (r ACLRule) validate() error {
	if r.Action != ActionAllow && r.Action != ActionDeny {
		return fmt.Errorf("auth: invalid acl action %d", r.Action)
	}
	if r.Permission != PermissionPublish && r.Permission != PermissionSubscribe && r.Permission != PermissionPubsub {
		return fmt.Errorf("auth: invalid acl permission %d", r.Permission)
	}
	if _, err := compileFilter(r.Topic); err != nil {
		return err
	}
	return nil
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func actionString(a Action) string {
	if a == ActionDeny {
		return "deny"
	}
	return "allow"
}

func permissionString(p Permission) string {
	switch p {
	case PermissionPublish:
		return "publish"
	case PermissionSubscribe:
		return "subscribe"
	default:
		return "pubsub"
	}
}
