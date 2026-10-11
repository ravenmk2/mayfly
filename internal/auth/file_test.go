package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// bcrypt hash of "secret" at default cost, generated once per test run.
func hashSecret(t *testing.T) string {
	t.Helper()
	h, err := HashPassword("secret")
	require.NoError(t, err)
	return h
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestFileUsersFormats(t *testing.T) {
	hash := hashSecret(t)
	files := map[string]string{
		"users.json": `{"users": {"sensor1": {"password": "` + hash + `", "superuser": true}}}`,
		"users.yaml": "users:\n  sensor1: { password: \"" + hash + "\", superuser: true }\n",
		"users.toml": "[users.sensor1]\npassword = \"" + hash + "\"\nsuperuser = true\n",
	}
	for name, content := range files {
		t.Run(name, func(t *testing.T) {
			f, err := NewFileUsers(writeFile(t, name, content))
			require.NoError(t, err)

			res, err := f.Authenticate(context.Background(), hook.ConnectInfo{
				ClientID: "dev1", Username: "sensor1", Password: []byte("secret"),
			})
			require.NoError(t, err)
			assert.True(t, res.OK)
			assert.True(t, res.Superuser)

			// Wrong password denies and stops the chain.
			res, err = f.Authenticate(context.Background(), hook.ConnectInfo{
				Username: "sensor1", Password: []byte("wrong"),
			})
			require.NoError(t, err)
			assert.False(t, res.OK)

			// Unknown user falls through.
			_, err = f.Authenticate(context.Background(), hook.ConnectInfo{
				Username: "nobody", Password: []byte("secret"),
			})
			assert.ErrorIs(t, err, hook.ErrNotRegistered)

			// No username at all falls through as well.
			_, err = f.Authenticate(context.Background(), hook.ConnectInfo{})
			assert.ErrorIs(t, err, hook.ErrNotRegistered)
		})
	}
}

func TestFileUsersMissingAndCorrupt(t *testing.T) {
	// Missing file: constructor reports the error and the source denies.
	f, err := NewFileUsers(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
	_, err = f.Authenticate(context.Background(), hook.ConnectInfo{Username: "u", Password: []byte("p")})
	assert.ErrorIs(t, err, errSourceUnavailable)

	// Corrupt content: same fail-closed behaviour.
	f, err = NewFileUsers(writeFile(t, "bad.yaml", "users: [unclosed"))
	require.Error(t, err)
	_, err = f.Authenticate(context.Background(), hook.ConnectInfo{Username: "u", Password: []byte("p")})
	assert.ErrorIs(t, err, errSourceUnavailable)

	// Unknown extension is rejected.
	_, err = NewFileUsers(writeFile(t, "users.ini", "x"))
	assert.Error(t, err)

	// A user without a password hash is rejected.
	_, err = NewFileUsers(writeFile(t, "empty.yaml", "users:\n  u1: { superuser: false }\n"))
	assert.Error(t, err)
}

// TestFileUsersReload: a failed reload keeps the last good table, a good
// reload swaps it atomically.
func TestFileUsersReload(t *testing.T) {
	hash := hashSecret(t)
	path := writeFile(t, "users.yaml", "users:\n  old: { password: \""+hash+"\", superuser: false }\n")
	f, err := NewFileUsers(path)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("users: [corrupt"), 0o600))
	assert.Error(t, f.Reload())
	res, err := f.Authenticate(context.Background(), hook.ConnectInfo{Username: "old", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK, "stale table must keep serving after a failed reload")

	require.NoError(t, os.WriteFile(path, []byte("users:\n  new: { password: \""+hash+"\" }\n"), 0o600))
	require.NoError(t, f.Reload())
	_, err = f.Authenticate(context.Background(), hook.ConnectInfo{Username: "old", Password: []byte("secret")})
	assert.ErrorIs(t, err, hook.ErrNotRegistered)
	res, err = f.Authenticate(context.Background(), hook.ConnectInfo{Username: "new", Password: []byte("secret")})
	require.NoError(t, err)
	assert.True(t, res.OK)
}

func TestFileACLFormats(t *testing.T) {
	rules := `- username: "*"
  action: deny
  permission: pubsub
  topic: "$SYS/#"
- username: "*"
  action: allow
  permission: publish
  topic: "up/{user}"
`
	files := map[string]string{
		"acl.yaml": "rules:\n" + rules,
		"acl.json": `{"rules": [` +
			`{"username": "*", "action": "deny", "permission": "pubsub", "topic": "$SYS/#"},` +
			`{"username": "*", "action": "allow", "permission": "publish", "topic": "up/{user}"}` +
			`]}`,
		"acl.toml": "[[rules]]\nusername = \"*\"\naction = \"deny\"\npermission = \"pubsub\"\ntopic = \"$SYS/#\"\n" +
			"[[rules]]\nusername = \"*\"\naction = \"allow\"\npermission = \"publish\"\ntopic = \"up/{user}\"\n",
	}
	for name, content := range files {
		t.Run(name, func(t *testing.T) {
			f, err := NewFileACL(writeFile(t, name, content))
			require.NoError(t, err)

			ctx := context.Background()
			alice := hook.ClientInfo{ClientID: "c1", Username: "alice"}
			assert.Equal(t, hook.VerdictDeny, f.Authorize(ctx, alice, hook.OpPublish, "$SYS/x"))
			assert.Equal(t, hook.VerdictAllow, f.Authorize(ctx, alice, hook.OpPublish, "up/alice"))
			assert.Equal(t, hook.VerdictIgnore, f.Authorize(ctx, alice, hook.OpPublish, "up/bob"))
			assert.Equal(t, hook.VerdictIgnore, f.Authorize(ctx, alice, hook.OpSubscribe, "up/alice"))
		})
	}
}

func TestFileACLMissingAndCorrupt(t *testing.T) {
	f, err := NewFileACL(filepath.Join(t.TempDir(), "nope.yaml"))
	require.Error(t, err)
	// Fail-closed: never-loaded acl source denies.
	assert.Equal(t, hook.VerdictDeny,
		f.Authorize(context.Background(), hook.ClientInfo{Username: "u"}, hook.OpPublish, "a/b"))

	f, err = NewFileACL(writeFile(t, "bad.yaml", "rules:\n  - { action: nah, permission: pubsub, topic: \"#\" }\n"))
	require.Error(t, err)
	assert.Equal(t, hook.VerdictDeny,
		f.Authorize(context.Background(), hook.ClientInfo{Username: "u"}, hook.OpPublish, "a/b"))
}

// TestFileACLReload: reloads pick up rule changes and the decision cache of
// the previous generation does not leak into the new one.
func TestFileACLReload(t *testing.T) {
	path := writeFile(t, "acl.yaml", "rules:\n  - { username: \"*\", action: allow, permission: pubsub, topic: \"t/#\" }\n")
	f, err := NewFileACL(path)
	require.NoError(t, err)

	ctx := context.Background()
	client := hook.ClientInfo{ClientID: "c", Username: "u"}
	assert.Equal(t, hook.VerdictAllow, f.Authorize(ctx, client, hook.OpPublish, "t/x"))

	// Flip the rule to deny; the cached allow must not survive the swap.
	require.NoError(t, os.WriteFile(path, []byte(
		"rules:\n  - { username: \"*\", action: deny, permission: pubsub, topic: \"t/#\" }\n"), 0o600))
	require.NoError(t, f.Reload())
	assert.Equal(t, hook.VerdictDeny, f.Authorize(ctx, client, hook.OpPublish, "t/x"))

	// Failed reload keeps the previous generation serving.
	require.NoError(t, os.WriteFile(path, []byte("rules: [corrupt"), 0o600))
	assert.Error(t, f.Reload())
	assert.Equal(t, hook.VerdictDeny, f.Authorize(ctx, client, hook.OpPublish, "t/x"))
}
