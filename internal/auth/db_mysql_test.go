//go:build integration

package auth

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// MySQL integration tests run only with -tags integration and a DSN in
// MAYFLY_TEST_MYSQL_DSN (without the "mysql:" scheme prefix), e.g.
// "root:secret@tcp(127.0.0.1:3306)/mayfly_test".
func newMySQLSource(t *testing.T) *DBSource {
	t.Helper()
	dsn := os.Getenv("MAYFLY_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("MAYFLY_TEST_MYSQL_DSN not set")
	}
	ctx := context.Background()
	s, err := NewDBSource(ctx, "mysql:"+dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.Equal(t, BackendMySQL, s.Backend())

	// Isolate the test from shared database content.
	_, err = s.db.ExecContext(ctx, `DELETE FROM mqtt_acl`)
	require.NoError(t, err)
	_, err = s.db.ExecContext(ctx, `DELETE FROM mqtt_user`)
	require.NoError(t, err)
	require.NoError(t, s.Reload(ctx))
	return s
}

func TestMySQLEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := newMySQLSource(t)

	// Idempotent migration: a second source over the same schema opens fine.
	s2, err := NewDBSource(ctx, "mysql:"+os.Getenv("MAYFLY_TEST_MYSQL_DSN"))
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	require.NoError(t, s.CreateUser(ctx, "sensor1", "secret", false))
	res, err := s.Authenticate(ctx, hook.ConnectInfo{Username: "sensor1", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK)

	_, err = s.CreateACLRule(ctx, ACLRule{Seq: 1, Action: ActionAllow, Permission: PermissionPublish, Topic: "up/{user}"})
	require.NoError(t, err)
	client := hook.ClientInfo{ClientID: "c", Username: "sensor1"}
	assert.Equal(t, hook.VerdictAllow, s.Authorize(ctx, client, hook.OpPublish, "up/sensor1"))
	assert.Equal(t, hook.VerdictIgnore, s.Authorize(ctx, client, hook.OpPublish, "up/other"))

	users, err := s.ListUsers(ctx)
	require.NoError(t, err)
	assert.Len(t, users, 1)
	rules, err := s.ListACLRules(ctx)
	require.NoError(t, err)
	assert.Len(t, rules, 1)
}
