package admin

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeFactories lists every Store implementation the contract tests run
// against; the SQL variant uses an in-memory SQLite database.
func storeFactories(t *testing.T) map[string]func(t *testing.T) Store {
	return map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store { return NewMemoryStore() },
		"sqlite": func(t *testing.T) Store {
			st, err := OpenSQLStore("sqlite::memory:")
			require.NoError(t, err)
			return st
		},
	}
}

func TestStoreContract(t *testing.T) {
	for name, factory := range storeFactories(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st := factory(t)
			t.Cleanup(func() { _ = st.Close() })

			// Users.
			n, err := st.UserCount(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, n)

			_, err = st.UserHash(ctx, "admin")
			require.ErrorIs(t, err, ErrNotFound)

			require.NoError(t, st.CreateUser(ctx, "admin", "hash-1"))
			require.ErrorIs(t, st.CreateUser(ctx, "admin", "hash-2"), ErrExists)

			hash, err := st.UserHash(ctx, "admin")
			require.NoError(t, err)
			assert.Equal(t, "hash-1", hash)

			n, err = st.UserCount(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)

			users, err := st.ListUsers(ctx)
			require.NoError(t, err)
			require.Len(t, users, 1)
			assert.Equal(t, "admin", users[0].Username)
			assert.False(t, users[0].CreatedAt.IsZero())

			// API keys.
			future := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
			id, err := st.CreateAPIKey(ctx, APIKey{
				Name: "ci", KeyHash: "aa", Prefix: "mfk_aaaa",
				Scopes: []string{ScopeRead, ScopeWrite}, ExpiresAt: future,
			})
			require.NoError(t, err)
			require.Positive(t, id)

			k, err := st.APIKeyByHash(ctx, "aa")
			require.NoError(t, err)
			assert.Equal(t, id, k.ID)
			assert.Equal(t, "ci", k.Name)
			assert.Equal(t, "mfk_aaaa", k.Prefix)
			assert.Equal(t, []string{ScopeRead, ScopeWrite}, k.Scopes)
			assert.WithinDuration(t, future, k.ExpiresAt, 2*time.Second)
			assert.False(t, k.Expired(time.Now()))

			_, err = st.APIKeyByHash(ctx, "missing")
			require.ErrorIs(t, err, ErrNotFound)

			id2, err := st.CreateAPIKey(ctx, APIKey{Name: "monitor", KeyHash: "bb", Prefix: "mfk_bbbb", Scopes: []string{ScopeRead}})
			require.NoError(t, err)

			keys, err := st.ListAPIKeys(ctx)
			require.NoError(t, err)
			require.Len(t, keys, 2)
			assert.Equal(t, id, keys[0].ID)
			assert.Equal(t, id2, keys[1].ID)
			assert.True(t, keys[1].ExpiresAt.IsZero())

			ok, err := st.DeleteAPIKey(ctx, id)
			require.NoError(t, err)
			assert.True(t, ok)
			ok, err = st.DeleteAPIKey(ctx, id)
			require.NoError(t, err)
			assert.False(t, ok)

			_, err = st.APIKeyByHash(ctx, "aa")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestScopeSatisfies(t *testing.T) {
	tests := []struct {
		name     string
		have     []string
		required string
		want     bool
	}{
		{"read needs read", []string{ScopeRead}, ScopeRead, true},
		{"write implies read", []string{ScopeWrite}, ScopeRead, true},
		{"admin implies write", []string{ScopeAdmin}, ScopeWrite, true},
		{"admin implies read", []string{ScopeAdmin}, ScopeRead, true},
		{"read does not imply write", []string{ScopeRead}, ScopeWrite, false},
		{"write does not imply admin", []string{ScopeWrite}, ScopeAdmin, false},
		{"none", nil, ScopeRead, false},
		{"unknown scope", []string{"bogus"}, ScopeRead, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ScopeSatisfies(tt.have, tt.required))
		})
	}
}

func TestValidScopes(t *testing.T) {
	assert.True(t, ValidScopes([]string{ScopeRead}))
	assert.True(t, ValidScopes([]string{ScopeRead, ScopeWrite, ScopeAdmin}))
	assert.False(t, ValidScopes(nil))
	assert.False(t, ValidScopes([]string{ScopeRead, "bogus"}))
}
