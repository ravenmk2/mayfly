//go:build integration

package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// MySQL integration tests run only with -tags integration and a DSN in
// MAYFLY_TEST_MYSQL_DSN (without the "mysql:" scheme prefix), e.g.
// "root:secret@tcp(127.0.0.1:3306)/mayfly_test".
func newMySQLStore(t *testing.T) Store {
	t.Helper()
	dsn := os.Getenv("MAYFLY_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("MAYFLY_TEST_MYSQL_DSN not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, "mysql:"+dsn)
	require.NoError(t, err)
	require.Equal(t, BackendMySQL, s.Backend())

	// Isolate the test from shared database content.
	for _, table := range []string{"mf_session", "mf_subscription", "mf_inflight", "mf_retained"} {
		_, err = s.db.ExecContext(ctx, `DELETE FROM `+table)
		require.NoError(t, err)
	}
	return s
}

func TestMySQLStoreContract(t *testing.T) {
	runContractTests(t, newMySQLStore)
}

func TestMySQLStoreMigrationIdempotent(t *testing.T) {
	dsn := os.Getenv("MAYFLY_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("MAYFLY_TEST_MYSQL_DSN not set")
	}
	ctx := context.Background()
	s1 := newMySQLStore(t)
	require.NoError(t, s1.Sessions().Put(ctx, Session{ClientID: "c", Expiry: 1}))
	require.NoError(t, s1.Close())

	s2, err := Open(ctx, "mysql:"+dsn)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()
	got, ok, err := s2.Sessions().Get(ctx, "c")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), got.Expiry)
}
