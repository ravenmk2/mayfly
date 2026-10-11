package admin

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestLoadMasterKeyMissing(t *testing.T) {
	dir := t.TempDir()
	key, persisted, err := LoadMasterKey(dir, testLogger())
	require.NoError(t, err)
	assert.False(t, persisted)
	assert.Len(t, key, MasterKeySize)

	// The ephemeral key is not written to disk.
	_, err = os.Stat(filepath.Join(dir, MasterKeyFile))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteAndLoadMasterKey(t *testing.T) {
	dir := t.TempDir()
	key, err := WriteMasterKey(dir)
	require.NoError(t, err)
	assert.Len(t, key, MasterKeySize)

	loaded, persisted, err := LoadMasterKey(dir, testLogger())
	require.NoError(t, err)
	assert.True(t, persisted)
	assert.Equal(t, key, loaded)
}

func TestLoadMasterKeyBadSize(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, MasterKeyFile), []byte("short"), 0o600))
	_, _, err := LoadMasterKey(dir, testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "master.key")
}
