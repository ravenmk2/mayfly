package admin

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ravenmk2/mayfly/internal/auth"
)

// schemaVersion is the admin migration version recorded in the shared
// schema_migrations table (version 1 belongs to the auth schema,
// docs/design/auth.md section 4).
const schemaVersion = 2

// adminSchema is the DDL per dialect (docs/design/admin-api.md section 2).
var adminSchema = map[auth.Backend][]string{
	auth.BackendSQLite: {
		`CREATE TABLE admin_user (
			username      VARCHAR(255) PRIMARY KEY,
			password_hash VARCHAR(255) NOT NULL,
			created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE api_key (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			name       VARCHAR(255) NOT NULL,
			key_hash   VARCHAR(64) NOT NULL UNIQUE,
			prefix     VARCHAR(32) NOT NULL,
			scopes     VARCHAR(64) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NULL
		)`,
	},
	auth.BackendMySQL: {
		`CREATE TABLE admin_user (
			username      VARCHAR(255) PRIMARY KEY,
			password_hash VARCHAR(255) NOT NULL,
			created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE api_key (
			id         BIGINT AUTO_INCREMENT PRIMARY KEY,
			name       VARCHAR(255) NOT NULL,
			key_hash   VARCHAR(64) NOT NULL UNIQUE,
			prefix     VARCHAR(32) NOT NULL,
			scopes     VARCHAR(64) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			expires_at TIMESTAMP NULL
		)`,
	},
}

// SQLStore is the Store backed by the configured primary database
// (docs/design/admin-api.md section 2). The schema migrates lazily on first
// use, so a database that is down at startup recovers without a restart.
type SQLStore struct {
	db       *sql.DB
	backend  auth.Backend
	migrated atomic.Bool
}

// NewSQLStore wraps an open pool on the primary database. The DSN is parsed
// with auth.OpenDB; migration happens on first use.
func NewSQLStore(db *sql.DB, backend auth.Backend) *SQLStore {
	return &SQLStore{db: db, backend: backend}
}

// OpenSQLStore parses the DSN and opens the pool.
func OpenSQLStore(dsn string) (*SQLStore, error) {
	db, backend, err := auth.OpenDB(dsn)
	if err != nil {
		return nil, err
	}
	return NewSQLStore(db, backend), nil
}

// Close implements Store.
func (s *SQLStore) Close() error { return s.db.Close() }

// migrate creates the admin tables once, recording the version in the shared
// schema_migrations table.
func (s *SQLStore) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("admin: create schema_migrations: %w", err)
	}
	var applied int
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, schemaVersion)
	if err := row.Scan(&applied); err != nil {
		return fmt.Errorf("admin: read schema version: %w", err)
	}
	if applied > 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("admin: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range adminSchema[s.backend] {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("admin: apply migration %d: %w", schemaVersion, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, schemaVersion); err != nil {
		return fmt.Errorf("admin: record migration %d: %w", schemaVersion, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("admin: commit migration: %w", err)
	}
	return nil
}

// ensureMigrated runs the migration at most once per process; failures are
// retried on the next call.
func (s *SQLStore) ensureMigrated(ctx context.Context) error {
	if s.migrated.Load() {
		return nil
	}
	if err := s.migrate(ctx); err != nil {
		return err
	}
	s.migrated.Store(true)
	return nil
}

// UserCount implements Store.
func (s *SQLStore) UserCount(ctx context.Context) (int, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return 0, err
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM admin_user`).Scan(&n); err != nil {
		return 0, fmt.Errorf("admin: count users: %w", err)
	}
	return n, nil
}

// CreateUser implements Store.
func (s *SQLStore) CreateUser(ctx context.Context, username, passwordHash string) error {
	if err := s.ensureMigrated(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO admin_user (username, password_hash) VALUES (?, ?)`, username, passwordHash); err != nil {
		if isDuplicate(err) {
			return ErrExists
		}
		return fmt.Errorf("admin: create user: %w", err)
	}
	return nil
}

// UserHash implements Store.
func (s *SQLStore) UserHash(ctx context.Context, username string) (string, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return "", err
	}
	var hash string
	err := s.db.QueryRowContext(ctx,
		`SELECT password_hash FROM admin_user WHERE username = ?`, username).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("admin: read user: %w", err)
	}
	return hash, nil
}

// ListUsers implements Store.
func (s *SQLStore) ListUsers(ctx context.Context) ([]User, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT username, created_at FROM admin_user ORDER BY username`)
	if err != nil {
		return nil, fmt.Errorf("admin: list users: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.Username, &u.CreatedAt); err != nil {
			return nil, fmt.Errorf("admin: scan user: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// CreateAPIKey implements Store.
func (s *SQLStore) CreateAPIKey(ctx context.Context, key APIKey) (int64, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO api_key (name, key_hash, prefix, scopes, expires_at) VALUES (?, ?, ?, ?, ?)`,
		key.Name, key.KeyHash, key.Prefix, strings.Join(key.Scopes, ","), nullTime(key.ExpiresAt))
	if err != nil {
		return 0, fmt.Errorf("admin: create api key: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("admin: api key id: %w", err)
	}
	return id, nil
}

// APIKeyByHash implements Store.
func (s *SQLStore) APIKeyByHash(ctx context.Context, keyHash string) (APIKey, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return APIKey{}, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, key_hash, prefix, scopes, created_at, expires_at FROM api_key WHERE key_hash = ?`, keyHash)
	k, err := scanAPIKey(row)
	if err == sql.ErrNoRows {
		return APIKey{}, ErrNotFound
	}
	if err != nil {
		return APIKey{}, fmt.Errorf("admin: read api key: %w", err)
	}
	return k, nil
}

// ListAPIKeys implements Store.
func (s *SQLStore) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, key_hash, prefix, scopes, created_at, expires_at FROM api_key ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("admin: list api keys: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// DeleteAPIKey implements Store.
func (s *SQLStore) DeleteAPIKey(ctx context.Context, id int64) (bool, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM api_key WHERE id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("admin: delete api key: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("admin: delete api key: %w", err)
	}
	return n > 0, nil
}

// scanner abstracts *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanAPIKey reads one api_key row.
func scanAPIKey(s scanner) (APIKey, error) {
	var (
		k       APIKey
		scopes  string
		expires sql.NullTime
	)
	if err := s.Scan(&k.ID, &k.Name, &k.KeyHash, &k.Prefix, &scopes, &k.CreatedAt, &expires); err != nil {
		return APIKey{}, err
	}
	if scopes != "" {
		k.Scopes = strings.Split(scopes, ",")
	}
	k.ExpiresAt = expires.Time
	return k, nil
}

// nullTime maps the zero time to NULL.
func nullTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}

// isDuplicate reports a unique-constraint violation across both drivers.
func isDuplicate(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || // MySQL 1062
		strings.Contains(msg, "UNIQUE constraint failed") // SQLite
}
