package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// API keys are presented in the X-API-Key header (docs/design/admin-api.md
// section 2). Only the SHA-256 hash is stored; the plaintext is shown once
// at creation time.
const (
	// apiKeyPrefix marks mayfly API keys.
	apiKeyPrefix = "mfk_"
	// apiKeyEntropy is the random part length in bytes (48 hex chars).
	apiKeyEntropy = 24
	// apiKeyPrefixLen is how many leading characters of the key are kept as
	// the display prefix.
	apiKeyPrefixLen = 12
)

// GenerateAPIKey returns a new plaintext API key together with its display
// prefix and storage hash.
func GenerateAPIKey() (plaintext, prefix, hash string, err error) {
	raw := make([]byte, apiKeyEntropy)
	if _, err := rand.Read(raw); err != nil {
		return "", "", "", fmt.Errorf("admin: generate api key: %w", err)
	}
	plaintext = apiKeyPrefix + hex.EncodeToString(raw)
	return plaintext, plaintext[:apiKeyPrefixLen], HashAPIKey(plaintext), nil
}

// HashAPIKey returns the hex SHA-256 of a plaintext key.
func HashAPIKey(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
