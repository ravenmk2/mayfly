package admin

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// Master key semantics (docs/design/setup.md section 4): a 32-byte random
// secret at <data-dir>/master.key that signs console session tokens. When
// the file is absent (pre-setup or memory-only runs) an ephemeral in-memory
// key is used and console sessions invalidate on restart.
const (
	// MasterKeyFile is the file name of the master key inside the data dir.
	MasterKeyFile = "master.key"
	// MasterKeySize is the master key length in bytes.
	MasterKeySize = 32
)

// LoadMasterKey reads the master key from <dataDir>/master.key. A missing
// file is not an error: an ephemeral random key is returned with
// persisted=false and a log record explaining the consequence.
func LoadMasterKey(dataDir string, logger *slog.Logger) (key []byte, persisted bool, err error) {
	path := filepath.Join(dataDir, MasterKeyFile)
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(raw) != MasterKeySize {
			return nil, false, fmt.Errorf("admin: %s holds %d bytes, want %d", path, len(raw), MasterKeySize)
		}
		return raw, true, nil
	case errors.Is(err, fs.ErrNotExist):
		key, err = GenerateMasterKey()
		if err != nil {
			return nil, false, err
		}
		logger.Warn("no master key found; using an ephemeral in-memory key, console sessions invalidate on restart",
			"path", path)
		return key, false, nil
	default:
		return nil, false, fmt.Errorf("admin: read %s: %w", path, err)
	}
}

// GenerateMasterKey returns a fresh random master key.
func GenerateMasterKey() ([]byte, error) {
	key := make([]byte, MasterKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("admin: generate master key: %w", err)
	}
	return key, nil
}

// WriteMasterKey generates a master key and persists it to
// <dataDir>/master.key with owner-only permissions, creating the data
// directory when needed.
func WriteMasterKey(dataDir string) ([]byte, error) {
	key, err := GenerateMasterKey()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("admin: create data dir: %w", err)
	}
	path := filepath.Join(dataDir, MasterKeyFile)
	if err := os.WriteFile(path, key, 0o600); err != nil {
		return nil, fmt.Errorf("admin: write %s: %w", path, err)
	}
	return key, nil
}
