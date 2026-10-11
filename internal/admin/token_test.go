package admin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenRoundTrip(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Now()

	token, expires, err := signToken(secret, "admin", now)
	require.NoError(t, err)
	assert.Equal(t, now.Add(TokenTTL).Unix(), expires.Unix())

	sub, err := verifyToken(secret, token, now)
	require.NoError(t, err)
	assert.Equal(t, "admin", sub)
}

func TestVerifyTokenRejects(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Now()
	token, _, err := signToken(secret, "admin", now)
	require.NoError(t, err)

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"no segments", "abc"},
		{"two segments", "abc.def"},
		{"bad header", "AAA.BBB.CCC"},
		{"bad signature", token[:len(token)-2] + "xx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := verifyToken(secret, tt.token, now)
			require.Error(t, err)
		})
	}

	t.Run("wrong secret", func(t *testing.T) {
		other := []byte("fedcba9876543210fedcba9876543210")
		_, err := verifyToken(other, token, now)
		require.ErrorIs(t, err, errTokenSignature)
	})

	t.Run("expired", func(t *testing.T) {
		_, err := verifyToken(secret, token, now.Add(TokenTTL+time.Second))
		require.ErrorIs(t, err, errTokenExpired)
	})
}

func TestGenerateAPIKey(t *testing.T) {
	plaintext, prefix, hash, err := GenerateAPIKey()
	require.NoError(t, err)
	assert.Len(t, plaintext, 4+48)
	assert.Equal(t, plaintext[:12], prefix)
	assert.Len(t, hash, 64)
	assert.Equal(t, hash, HashAPIKey(plaintext))
	assert.NotEqual(t, HashAPIKey(plaintext), HashAPIKey(plaintext+"x"))
}
