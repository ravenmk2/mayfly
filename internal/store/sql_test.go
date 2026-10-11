package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSQLiteStore(t *testing.T) Store {
	t.Helper()
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "store.db")
	s, err := Open(context.Background(), dsn)
	require.NoError(t, err)
	return s
}

func TestSQLStoreContract(t *testing.T) {
	runContractTests(t, newSQLiteStore)
}

func TestSQLStoreMigrationIdempotent(t *testing.T) {
	ctx := context.Background()
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "store.db")

	s1, err := Open(ctx, dsn)
	require.NoError(t, err)
	require.Equal(t, BackendSQLite, s1.Backend())
	require.NoError(t, s1.Sessions().Put(ctx, Session{ClientID: "c", Expiry: 1}))
	require.NoError(t, s1.Close())

	// Reopening the same database applies no migration and keeps the data.
	s2, err := Open(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()
	got, ok, err := s2.Sessions().Get(ctx, "c")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint32(1), got.Expiry)
}

func TestOpenDSNValidation(t *testing.T) {
	ctx := context.Background()
	for _, dsn := range []string{"", "nosqlite", "postgres://x", "sqlite:"} {
		_, err := Open(ctx, dsn)
		assert.Error(t, err, "dsn %q", dsn)
	}
}
