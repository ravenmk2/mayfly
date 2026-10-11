package auth

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func writeUsersYAML(t *testing.T, user, password string, superuser bool) string {
	t.Helper()
	hash, err := HashPassword(password)
	require.NoError(t, err)
	return writeFile(t, "users.yaml",
		"users:\n  "+user+": { password: \""+hash+"\", superuser: "+map[bool]string{true: "true", false: "false"}[superuser]+" }\n")
}

func TestManagerValidate(t *testing.T) {
	ctx := context.Background()

	_, err := NewManager(ctx, Config{Authentication: []SourceConfig{
		{Type: "db", Enabled: true, DSN: "sqlite::memory:"},
		{Type: "db", Enabled: true, DSN: "sqlite::memory:"},
	}}, testLogger())
	assert.ErrorContains(t, err, "at most one db source")

	// A disabled second db source is tolerated.
	m, err := NewManager(ctx, Config{Authentication: []SourceConfig{
		{Type: "db", Enabled: true, DSN: "sqlite::memory:"},
		{Type: "db", Enabled: false, DSN: "sqlite::memory:"},
	}}, testLogger())
	require.NoError(t, err)
	require.NoError(t, m.Close())

	_, err = NewManager(ctx, Config{Authentication: []SourceConfig{{Type: "ldap", Enabled: true}}}, testLogger())
	assert.ErrorContains(t, err, "unknown type")

	_, err = NewManager(ctx, Config{Authorization: []SourceConfig{{Type: "file", Enabled: true}}}, testLogger())
	assert.ErrorContains(t, err, "file path is empty")
}

// TestManagerChainSemantics pins fall-through, deny-stops-chain and
// superuser behaviour across two real sources.
func TestManagerChainSemantics(t *testing.T) {
	ctx := context.Background()
	usersA := writeUsersYAML(t, "alice", "secret-a", false)
	usersB := writeUsersYAML(t, "bob", "secret-b", true)

	m, err := NewManager(ctx, Config{
		Authentication: []SourceConfig{
			{Type: "file", Enabled: true, Path: usersA},
			{Type: "file", Enabled: true, Path: usersB},
		},
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	// Fall-through: alice only exists in the first source, bob in the second.
	res, err := m.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("secret-a")})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.False(t, res.Superuser)

	res, err = m.Authenticate(ctx, hook.ConnectInfo{Username: "bob", Password: []byte("secret-b")})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.True(t, res.Superuser)

	// Wrong password stops the chain at the first source that knows the user.
	res, err = m.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("wrong")})
	require.NoError(t, err)
	assert.False(t, res.OK)

	// Unknown to every source: deny.
	res, err = m.Authenticate(ctx, hook.ConnectInfo{Username: "mallory", Password: []byte("x")})
	require.NoError(t, err)
	assert.False(t, res.OK)

	stats := m.Stats()
	require.Len(t, stats, 2)
	assert.Equal(t, int64(1), stats[0].Allow) // alice
	assert.Equal(t, int64(1), stats[0].Deny)  // alice wrong password
	assert.Equal(t, int64(2), stats[0].Miss)  // bob, mallory
	assert.Equal(t, int64(1), stats[1].Allow) // bob
	assert.Equal(t, "authentication", stats[0].Chain)
}

// TestManagerAnonymous: anonymous_allow admits passwordless CONNECTs that
// every source missed, never as superuser; with a password present the
// all-miss outcome stays a denial.
func TestManagerAnonymous(t *testing.T) {
	ctx := context.Background()
	users := writeUsersYAML(t, "alice", "secret", false)

	m, err := NewManager(ctx, Config{
		AnonymousAllow: true,
		Authentication: []SourceConfig{{Type: "file", Enabled: true, Path: users}},
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	res, err := m.Authenticate(ctx, hook.ConnectInfo{ClientID: "anon1"})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.False(t, res.Superuser)

	res, err = m.Authenticate(ctx, hook.ConnectInfo{ClientID: "anon2", Password: []byte("x")})
	require.NoError(t, err)
	assert.False(t, res.OK, "anonymous allowance must not cover passworded attempts")
}

// TestManagerAuthorizeChain: superuser skips, first verdict wins, all-miss
// denies, and a disabled source is ignored.
func TestManagerAuthorizeChain(t *testing.T) {
	ctx := context.Background()
	aclA := writeFile(t, "a.yaml", "rules:\n  - { username: \"*\", action: allow, permission: publish, topic: \"up/{user}\" }\n")
	aclB := writeFile(t, "b.yaml", "rules:\n  - { username: \"*\", action: allow, permission: pubsub, topic: \"#\" }\n")

	m, err := NewManager(ctx, Config{Authorization: []SourceConfig{
		{Type: "file", Enabled: true, Path: aclA},
		{Type: "file", Enabled: false, Path: aclB}, // disabled: must not fire
	}}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	alice := hook.ClientInfo{ClientID: "c", Username: "alice"}
	assert.True(t, m.Allow(ctx, alice, hook.OpPublish, "up/alice"))
	assert.False(t, m.Allow(ctx, alice, hook.OpPublish, "other/topic"),
		"all-miss defaults to deny; the disabled catch-all must not fire")
	assert.True(t, m.Allow(ctx, hook.ClientInfo{Username: "root", Superuser: true}, hook.OpPublish, "anything"))
}

// TestManagerRefreshAppliesChanges: manual Refresh picks up file edits and
// swaps generations atomically.
func TestManagerRefresh(t *testing.T) {
	ctx := context.Background()
	acl := writeFile(t, "acl.yaml", "rules:\n  - { username: \"*\", action: allow, permission: pubsub, topic: \"t/#\" }\n")

	m, err := NewManager(ctx, Config{Authorization: []SourceConfig{
		{Type: "file", Enabled: true, Path: acl},
	}}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	client := hook.ClientInfo{ClientID: "c", Username: "u"}
	assert.True(t, m.Allow(ctx, client, hook.OpPublish, "t/x"))

	require.NoError(t, os.WriteFile(acl, []byte(
		"rules:\n  - { username: \"*\", action: deny, permission: pubsub, topic: \"t/#\" }\n"), 0o600))
	// Before Refresh the old generation still serves.
	assert.True(t, m.Allow(ctx, client, hook.OpPublish, "t/x"))
	require.NoError(t, m.Refresh(ctx))
	assert.False(t, m.Allow(ctx, client, hook.OpPublish, "t/x"))
}

// TestManagerRefreshFailClosed: a corrupt reload keeps the last good
// snapshot and Refresh reports the error.
func TestManagerRefreshFailClosed(t *testing.T) {
	ctx := context.Background()
	acl := writeFile(t, "acl.yaml", "rules:\n  - { username: \"*\", action: allow, permission: pubsub, topic: \"t/#\" }\n")

	m, err := NewManager(ctx, Config{Authorization: []SourceConfig{
		{Type: "file", Enabled: true, Path: acl},
	}}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	require.NoError(t, os.WriteFile(acl, []byte("rules: [corrupt"), 0o600))
	assert.Error(t, m.Refresh(ctx))
	assert.True(t, m.Allow(ctx, hook.ClientInfo{Username: "u"}, hook.OpPublish, "t/x"))
}

// TestManagerDBSourceAcrossChains: one db entry per chain shares a single
// pool and serves both chains.
func TestManagerDBSourceAcrossChains(t *testing.T) {
	ctx := context.Background()
	dsn := "sqlite:" + filepath.Join(t.TempDir(), "auth.db")

	// Seed the database through a standalone source.
	seed, err := NewDBSource(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, seed.CreateUser(ctx, "alice", "secret", false))
	_, err = seed.CreateACLRule(ctx, ACLRule{Seq: 1, Action: ActionAllow, Permission: PermissionPublish, Topic: "up/{user}"})
	require.NoError(t, err)
	require.NoError(t, seed.Close())

	m, err := NewManager(ctx, Config{
		Authentication: []SourceConfig{{Type: "db", Enabled: true, DSN: dsn}},
		Authorization:  []SourceConfig{{Type: "db", Enabled: true, DSN: dsn}},
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	res, err := m.Authenticate(ctx, hook.ConnectInfo{Username: "alice", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.True(t, m.Allow(ctx, hook.ClientInfo{ClientID: "c", Username: "alice"}, hook.OpPublish, "up/alice"))
	assert.False(t, m.Allow(ctx, hook.ClientInfo{ClientID: "c", Username: "alice"}, hook.OpPublish, "up/bob"))
}

// TestManagerBackgroundRefresh: the ticker applies file changes without a
// manual Refresh.
func TestManagerBackgroundRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acl := writeFile(t, "acl.yaml", "rules:\n  - { username: \"*\", action: deny, permission: pubsub, topic: \"t/#\" }\n")

	m, err := NewManager(ctx, Config{
		RefreshInterval: 20 * time.Millisecond,
		Authorization:   []SourceConfig{{Type: "file", Enabled: true, Path: acl}},
	}, testLogger())
	require.NoError(t, err)
	defer func() { _ = m.Close() }()
	m.Start(ctx)

	client := hook.ClientInfo{Username: "u"}
	assert.False(t, m.Allow(ctx, client, hook.OpPublish, "t/x"))

	require.NoError(t, os.WriteFile(acl, []byte(
		"rules:\n  - { username: \"*\", action: allow, permission: pubsub, topic: \"t/#\" }\n"), 0o600))
	require.Eventually(t, func() bool {
		return m.Allow(ctx, client, hook.OpPublish, "t/x")
	}, 3*time.Second, 10*time.Millisecond)
}
