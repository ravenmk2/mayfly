package auth

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
)

func newSQLiteSource(t *testing.T) *DBSource {
	t.Helper()
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "auth.db")
	s, err := NewDBSource(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.Equal(t, BackendSQLite, s.Backend())
	return s
}

// TestDBMigrateIdempotent: opening a source twice over the same database
// file must not fail or re-apply migrations.
func TestDBMigrateIdempotent(t *testing.T) {
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "auth.db")
	ctx := context.Background()

	s1, err := NewDBSource(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, s1.CreateUser(ctx, "u1", "secret", false))
	require.NoError(t, s1.Close())

	s2, err := NewDBSource(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	users, err := s2.ListUsers(ctx)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "u1", users[0].Username)
}

// TestDBUserCRUD covers the management write interface over SQLite.
func TestDBUserCRUD(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteSource(t)

	require.NoError(t, s.CreateUser(ctx, "alice", "pw1", false))
	require.NoError(t, s.CreateUser(ctx, "admin", "pw2", true))
	require.Error(t, s.CreateUser(ctx, "", "pw", false))

	u, err := s.GetUser(ctx, "admin")
	require.NoError(t, err)
	assert.True(t, u.Superuser)
	assert.False(t, u.CreatedAt.IsZero())

	users, err := s.ListUsers(ctx)
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, "admin", users[0].Username) // ordered by name
	assert.Equal(t, "alice", users[1].Username)

	// Update password and flag.
	require.NoError(t, s.UpdateUser(ctx, "alice", ptr("pw3"), ptr(true)))
	res, err := s.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("pw1")})
	require.NoError(t, err)
	assert.False(t, res.OK, "old password must stop working")
	res, err = s.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("pw3")})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.True(t, res.Superuser)

	require.NoError(t, s.DeleteUser(ctx, "alice"))
	_, err = s.GetUser(ctx, "alice")
	assert.Error(t, err)
}

// TestDBAuthN: bcrypt verification end-to-end against a real database,
// including chain fall-through for unknown users.
func TestDBAuthN(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteSource(t)
	require.NoError(t, s.CreateUser(ctx, "sensor1", "secret", false))

	res, err := s.Authenticate(ctx, hook.ConnectInfo{Username: "sensor1", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.False(t, res.Superuser)

	res, err = s.Authenticate(ctx, hook.ConnectInfo{Username: "sensor1", Password: []byte("bad")})
	require.NoError(t, err)
	assert.False(t, res.OK)

	_, err = s.Authenticate(ctx, hook.ConnectInfo{Username: "ghost", Password: []byte("x")})
	assert.ErrorIs(t, err, hook.ErrNotRegistered)
}

// TestDBACLCRUD covers rule writes, listing and validation over SQLite.
func TestDBACLCRUD(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteSource(t)

	id, err := s.CreateACLRule(ctx, ACLRule{Seq: 1, Action: ActionDeny, Permission: PermissionPubsub, Topic: "$SYS/#"})
	require.NoError(t, err)
	id2, err := s.CreateACLRule(ctx, ACLRule{
		Seq: 2, Username: "alice", Action: ActionAllow, Permission: PermissionPublish, Topic: "up/{user}",
	})
	require.NoError(t, err)

	rules, err := s.ListACLRules(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 2)
	assert.Equal(t, id, rules[0].ID)
	assert.Equal(t, ActionDeny, rules[0].Action)
	assert.Equal(t, "alice", rules[1].Username)
	assert.Equal(t, PermissionPublish, rules[1].Permission)

	// Invalid rules are rejected before hitting the database.
	_, err = s.CreateACLRule(ctx, ACLRule{Seq: 3, Action: ActionAllow, Permission: PermissionPublish, Topic: "a/#/b"})
	assert.Error(t, err)
	_, err = s.CreateACLRule(ctx, ACLRule{Seq: 3, Action: Action(9), Permission: PermissionPublish, Topic: "a/b"})
	assert.Error(t, err)

	require.NoError(t, s.UpdateACLRule(ctx, ACLRule{
		ID: id2, Seq: 3, ClientID: "dev1", Action: ActionAllow, Permission: PermissionSubscribe, Topic: "down/{clientid}",
	}))
	rules, err = s.ListACLRules(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 2)
	assert.Equal(t, 3, rules[1].Seq)
	assert.Equal(t, "dev1", rules[1].ClientID)
	assert.Equal(t, "down/{clientid}", rules[1].Topic)

	require.NoError(t, s.DeleteACLRule(ctx, id))
	rules, err = s.ListACLRules(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 1)
}

// TestDBAuthZEndToEnd: writes become authorization decisions immediately
// (writes trigger a reload).
func TestDBAuthZEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteSource(t)

	_, err := s.CreateACLRule(ctx, ACLRule{Seq: 1, Action: ActionDeny, Permission: PermissionPubsub, Topic: "$SYS/#"})
	require.NoError(t, err)
	_, err = s.CreateACLRule(ctx, ACLRule{Seq: 2, Action: ActionAllow, Permission: PermissionPublish, Topic: "up/{user}"})
	require.NoError(t, err)

	alice := hook.ClientInfo{ClientID: "c1", Username: "alice"}
	assert.Equal(t, hook.VerdictAllow, s.Authorize(ctx, alice, hook.OpPublish, "up/alice"))
	assert.Equal(t, hook.VerdictIgnore, s.Authorize(ctx, alice, hook.OpPublish, "up/bob"))
	assert.Equal(t, hook.VerdictDeny, s.Authorize(ctx, alice, hook.OpPublish, "$SYS/broker"))
	assert.Equal(t, hook.VerdictIgnore, s.Authorize(ctx, alice, hook.OpSubscribe, "up/alice"))
}

// TestDBFailClosed: a source whose initial load failed denies both chains
// until a Reload succeeds.
func TestDBFailClosed(t *testing.T) {
	ctx := context.Background()
	// Migrate a database, then drop the tables behind the source's back.
	path := filepath.Join(t.TempDir(), "auth.db")
	s, err := NewDBSource(ctx, "sqlite:"+path)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, err = s.db.ExecContext(ctx, `DROP TABLE mqtt_user`)
	require.NoError(t, err)
	_, err = s.db.ExecContext(ctx, `DROP TABLE mqtt_acl`)
	require.NoError(t, err)

	require.Error(t, s.Reload(ctx))
	_, err = s.Authenticate(ctx, hook.ConnectInfo{Username: "u", Password: []byte("p")})
	// The initial load succeeded, so a stale (empty) snapshot serves:
	// unknown user falls through, and ACL decisions use the stale set.
	assert.ErrorIs(t, err, hook.ErrNotRegistered)

	// Simulate a source that never loaded: fresh struct over a broken db.
	broken := &DBSource{db: s.db, backend: BackendSQLite}
	_, err = broken.Authenticate(ctx, hook.ConnectInfo{Username: "u", Password: []byte("p")})
	assert.ErrorIs(t, err, errSourceUnavailable)
	assert.Equal(t, hook.VerdictDeny,
		broken.Authorize(ctx, hook.ClientInfo{Username: "u"}, hook.OpPublish, "a/b"))
}

// TestDBReloadStaleness: while the database is broken the last good
// snapshot keeps serving existing decisions.
func TestDBReloadStaleness(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteSource(t)
	require.NoError(t, s.CreateUser(ctx, "alice", "secret", false))
	_, err := s.CreateACLRule(ctx, ACLRule{Seq: 1, Action: ActionAllow, Permission: PermissionPubsub, Topic: "t/#"})
	require.NoError(t, err)

	// Break the database: reload fails, stale snapshot still serves.
	_, err = s.db.ExecContext(ctx, `DROP TABLE mqtt_acl`)
	require.NoError(t, err)
	assert.Error(t, s.Reload(ctx))

	alice := hook.ClientInfo{ClientID: "c", Username: "alice"}
	assert.Equal(t, hook.VerdictAllow, s.Authorize(ctx, alice, hook.OpPublish, "t/x"))
	res, err := s.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK)
}

func ptr[T any](v T) *T { return &v }
