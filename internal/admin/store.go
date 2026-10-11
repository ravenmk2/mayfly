// Package admin implements the management plane (docs/design/admin-api.md):
// the REST API v1, console authentication (admin account and API keys) and
// the first-run setup wizard (docs/design/setup.md). Administrative data
// (console users, API keys) lives in the primary database when one is
// configured, in memory otherwise.
package admin

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Scope labels the capabilities an API key may carry (docs/design/admin-api.md
// section 2). The scopes form a hierarchy: admin implies write, write
// implies read.
const (
	ScopeRead  = "read"
	ScopeWrite = "write"
	ScopeAdmin = "admin"
)

// scopeRank orders the scopes so a higher-ranked scope satisfies a lower
// requirement.
func scopeRank(scope string) int {
	switch scope {
	case ScopeRead:
		return 1
	case ScopeWrite:
		return 2
	case ScopeAdmin:
		return 3
	default:
		return 0
	}
}

// ScopeSatisfies reports whether the held scopes satisfy the required one.
func ScopeSatisfies(have []string, required string) bool {
	need := scopeRank(required)
	for _, h := range have {
		if scopeRank(h) >= need && need > 0 {
			return true
		}
	}
	return false
}

// ValidScopes reports whether every entry is a known scope.
func ValidScopes(scopes []string) bool {
	if len(scopes) == 0 {
		return false
	}
	for _, s := range scopes {
		if scopeRank(s) == 0 {
			return false
		}
	}
	return true
}

// ErrNotFound reports a missing admin user or API key.
var ErrNotFound = errors.New("admin: not found")

// ErrExists reports a duplicate admin user.
var ErrExists = errors.New("admin: already exists")

// User is the management view of one admin_user row.
type User struct {
	Username  string
	CreatedAt time.Time
}

// APIKey is the management view of one api_key row. KeyHash is the hex
// SHA-256 of the full key; the plaintext key is only ever returned once, at
// creation time.
type APIKey struct {
	ID        int64
	Name      string
	KeyHash   string
	Prefix    string
	Scopes    []string
	CreatedAt time.Time
	// ExpiresAt is the expiry instant; the zero value means the key never
	// expires.
	ExpiresAt time.Time
}

// Expired reports whether the key is past its expiry at now.
func (k APIKey) Expired(now time.Time) bool {
	return !k.ExpiresAt.IsZero() && !now.Before(k.ExpiresAt)
}

// Store persists console users and API keys (docs/design/admin-api.md
// section 2). Implementations: SQLStore (the configured primary database,
// MySQL or SQLite) and MemoryStore (memory-only runs).
type Store interface {
	// UserCount reports how many console users exist.
	UserCount(ctx context.Context) (int, error)
	// CreateUser inserts one console user with a pre-hashed password.
	CreateUser(ctx context.Context, username, passwordHash string) error
	// UserHash returns the bcrypt hash of one user's password.
	UserHash(ctx context.Context, username string) (string, error)
	// ListUsers returns every console user ordered by name.
	ListUsers(ctx context.Context) ([]User, error)

	// CreateAPIKey inserts one API key and returns its assigned ID.
	CreateAPIKey(ctx context.Context, key APIKey) (int64, error)
	// APIKeyByHash looks up one key by its SHA-256 hash.
	APIKeyByHash(ctx context.Context, keyHash string) (APIKey, error)
	// ListAPIKeys returns every API key ordered by ID.
	ListAPIKeys(ctx context.Context) ([]APIKey, error)
	// DeleteAPIKey removes one key, reporting whether it existed.
	DeleteAPIKey(ctx context.Context, id int64) (bool, error)

	Close() error
}

// userRecord is the in-memory form of one console user.
type userRecord struct {
	hash      string
	createdAt time.Time
}

// MemoryStore is the in-memory Store for runs without a configured primary
// database; its content vanishes on restart (docs/design/setup.md).
type MemoryStore struct {
	mu      sync.RWMutex
	users   map[string]userRecord
	keys    map[int64]APIKey
	byHash  map[string]int64
	nextKey int64
	now     func() time.Time
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		users:  map[string]userRecord{},
		keys:   map[int64]APIKey{},
		byHash: map[string]int64{},
		now:    time.Now,
	}
}

// Close implements Store.
func (s *MemoryStore) Close() error { return nil }

// UserCount implements Store.
func (s *MemoryStore) UserCount(context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users), nil
}

// CreateUser implements Store.
func (s *MemoryStore) CreateUser(_ context.Context, username, passwordHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[username]; ok {
		return ErrExists
	}
	s.users[username] = userRecord{hash: passwordHash, createdAt: s.now()}
	return nil
}

// UserHash implements Store.
func (s *MemoryStore) UserHash(_ context.Context, username string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[username]
	if !ok {
		return "", ErrNotFound
	}
	return u.hash, nil
}

// ListUsers implements Store.
func (s *MemoryStore) ListUsers(context.Context) ([]User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]User, 0, len(s.users))
	for name, u := range s.users {
		out = append(out, User{Username: name, CreatedAt: u.createdAt})
	}
	return out, nil
}

// CreateAPIKey implements Store.
func (s *MemoryStore) CreateAPIKey(_ context.Context, key APIKey) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextKey++
	key.ID = s.nextKey
	if key.CreatedAt.IsZero() {
		key.CreatedAt = s.now()
	}
	s.keys[key.ID] = key
	s.byHash[key.KeyHash] = key.ID
	return key.ID, nil
}

// APIKeyByHash implements Store.
func (s *MemoryStore) APIKeyByHash(_ context.Context, keyHash string) (APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byHash[keyHash]
	if !ok {
		return APIKey{}, ErrNotFound
	}
	return s.keys[id], nil
}

// ListAPIKeys implements Store.
func (s *MemoryStore) ListAPIKeys(context.Context) ([]APIKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]APIKey, 0, len(s.keys))
	for _, k := range s.keys {
		out = append(out, k)
	}
	return out, nil
}

// DeleteAPIKey implements Store.
func (s *MemoryStore) DeleteAPIKey(_ context.Context, id int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.keys[id]
	if !ok {
		return false, nil
	}
	delete(s.byHash, k.KeyHash)
	delete(s.keys, id)
	return true, nil
}
