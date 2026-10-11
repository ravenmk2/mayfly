package auth

import (
	"errors"
	"fmt"

	"github.com/ravenmk2/mayfly/internal/hook"
	"golang.org/x/crypto/bcrypt"
)

// userRecord is one principal's stored credentials.
type userRecord struct {
	hash      string // bcrypt
	superuser bool
}

// authenticateUsers applies the chain semantics against a user table
// (docs/design/auth.md section 2): unknown users fall through, known users
// with a bad password deny and stop the chain. The password never appears in
// results, errors or logs.
func authenticateUsers(users map[string]userRecord, info hook.ConnectInfo) (hook.Result, error) {
	if info.Username == "" {
		return hook.Result{}, hook.ErrNotRegistered
	}
	u, ok := users[info.Username]
	if !ok {
		return hook.Result{}, hook.ErrNotRegistered
	}
	if bcrypt.CompareHashAndPassword([]byte(u.hash), info.Password) != nil {
		return hook.Result{OK: false, Reason: "invalid credentials"}, nil
	}
	return hook.Result{OK: true, Superuser: u.superuser}, nil
}

// authorizeRules applies one source's compiled rule set. A nil rule set
// means the source never loaded: deny (fail-closed, docs/design/auth.md
// section 6).
func authorizeRules(rs *RuleSet, client hook.ClientInfo, op hook.Op, topic string) hook.Verdict {
	if rs == nil {
		return hook.VerdictDeny
	}
	return rs.Decide(client.Username, client.ClientID, op, topic)
}

// HashPassword returns the bcrypt hash of a plaintext password at the
// default cost (docs/design/auth.md section 7).
func HashPassword(password string) (string, error) {
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("auth: hash password: %w", err)
	}
	return string(h), nil
}

// errSourceUnavailable marks a source whose content never loaded; chains
// treat it as a denial (fail-closed).
var errSourceUnavailable = errors.New("auth: source content unavailable")
