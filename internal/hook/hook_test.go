package hook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAuth is a table-driven Authenticator for chain tests.
type stubAuth struct {
	res Result
	err error
}

func (s stubAuth) Authenticate(context.Context, ConnectInfo) (Result, error) {
	return s.res, s.err
}

// stubAuthz is a table-driven Authorizer for chain tests.
type stubAuthz struct {
	verdict Verdict
}

func (s stubAuthz) Authorize(context.Context, ClientInfo, Op, string) Verdict {
	return s.verdict
}

func TestAuthChainStopsOnAllow(t *testing.T) {
	chain := NewAuthChain(
		stubAuth{err: ErrNotRegistered},
		stubAuth{res: Result{OK: true, Superuser: true}},
		stubAuth{res: Result{OK: false}}, // never reached
	)
	res, err := chain.Authenticate(context.Background(), ConnectInfo{Username: "u"})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.True(t, res.Superuser)
}

func TestAuthChainStopsOnDeny(t *testing.T) {
	// A wrong password denies immediately; a weaker source later in the
	// chain must not get a chance to allow (docs/design/auth.md section 2).
	chain := NewAuthChain(
		stubAuth{res: Result{OK: false, Reason: "bad password"}},
		stubAuth{res: Result{OK: true}},
	)
	res, err := chain.Authenticate(context.Background(), ConnectInfo{Username: "u"})
	require.NoError(t, err)
	assert.False(t, res.OK)
}

func TestAuthChainAllMissDenies(t *testing.T) {
	chain := NewAuthChain(
		stubAuth{err: ErrNotRegistered},
		stubAuth{err: ErrNotRegistered},
	)
	res, err := chain.Authenticate(context.Background(), ConnectInfo{Username: "u"})
	require.NoError(t, err)
	assert.False(t, res.OK)
}

func TestAuthChainSourceErrorFailsClosed(t *testing.T) {
	chain := NewAuthChain(
		stubAuth{err: errors.New("backend unreachable")},
	)
	res, err := chain.Authenticate(context.Background(), ConnectInfo{Username: "u"})
	require.Error(t, err)
	assert.False(t, res.OK)
}

func TestAuthzChainStopsOnFirstHit(t *testing.T) {
	chain := NewAuthzChain(
		stubAuthz{verdict: VerdictIgnore},
		stubAuthz{verdict: VerdictDeny},
		stubAuthz{verdict: VerdictAllow}, // never reached
	)
	assert.False(t, chain.Allow(context.Background(), ClientInfo{}, OpPublish, "a/b"))

	chain = NewAuthzChain(
		stubAuthz{verdict: VerdictIgnore},
		stubAuthz{verdict: VerdictAllow},
	)
	assert.True(t, chain.Allow(context.Background(), ClientInfo{}, OpSubscribe, "a/#"))
}

func TestAuthzChainAllMissDenies(t *testing.T) {
	chain := NewAuthzChain(stubAuthz{verdict: VerdictIgnore})
	assert.False(t, chain.Allow(context.Background(), ClientInfo{}, OpPublish, "a/b"))
}

func TestAuthzChainSuperuserSkipsSources(t *testing.T) {
	chain := NewAuthzChain(stubAuthz{verdict: VerdictDeny})
	assert.True(t, chain.Allow(context.Background(), ClientInfo{Superuser: true}, OpPublish, "$SYS/broker"))
}

func TestAllowAllStub(t *testing.T) {
	res, err := AllowAll{}.Authenticate(context.Background(), ConnectInfo{})
	require.NoError(t, err)
	assert.True(t, res.OK)
	assert.False(t, res.Superuser)
	assert.Equal(t, VerdictAllow, AllowAll{}.Authorize(context.Background(), ClientInfo{}, OpPublish, "x"))
}
