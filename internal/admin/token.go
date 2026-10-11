package admin

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// TokenTTL is the console session lifetime (docs/design/admin-api.md
// section 2).
const TokenTTL = 12 * time.Hour

var (
	errTokenMalformed = errors.New("admin: malformed token")
	errTokenSignature = errors.New("admin: invalid token signature")
	errTokenExpired   = errors.New("admin: token expired")
)

// jwtHeader is the fixed HS256 JOSE header, base64url-encoded.
var jwtHeader = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

type jwtClaims struct {
	Sub string `json:"sub"`
	Iat int64  `json:"iat"`
	Exp int64  `json:"exp"`
}

// signToken issues an HS256 JWT for subject, signed with the master key.
func signToken(secret []byte, subject string, now time.Time) (token string, expires time.Time, err error) {
	expires = now.Add(TokenTTL)
	claims := jwtClaims{Sub: subject, Iat: now.Unix(), Exp: expires.Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("admin: encode token claims: %w", err)
	}
	body := jwtHeader + "." + base64.RawURLEncoding.EncodeToString(payload)
	sig := hmacSHA256(secret, body)
	return body + "." + base64.RawURLEncoding.EncodeToString(sig), expires, nil
}

// verifyToken validates the signature and expiry of a token and returns its
// subject.
func verifyToken(secret []byte, token string, now time.Time) (string, error) {
	head, rest, ok := strings.Cut(token, ".")
	if !ok || head != jwtHeader {
		return "", errTokenMalformed
	}
	payloadB64, sigB64, ok := strings.Cut(rest, ".")
	if !ok {
		return "", errTokenMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return "", errTokenMalformed
	}
	want := hmacSHA256(secret, head+"."+payloadB64)
	if subtle.ConstantTimeCompare(sig, want) != 1 {
		return "", errTokenSignature
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", errTokenMalformed
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Sub == "" {
		return "", errTokenMalformed
	}
	if now.Unix() >= claims.Exp {
		return "", errTokenExpired
	}
	return claims.Sub, nil
}

func hmacSHA256(secret []byte, body string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(body))
	return mac.Sum(nil)
}
