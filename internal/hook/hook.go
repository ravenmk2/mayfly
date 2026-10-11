// Package hook defines the pluggable authentication and authorization
// interfaces (architecture decision A7/A8). Interfaces are grouped small and
// narrow: the broker type-asserts against them instead of consuming one large
// hook interface. This milestone ships the interface skeleton, the chain
// semantics and an allow-all stub; concrete sources (file/db) arrive with
// internal/auth.
package hook

import (
	"context"
	"errors"
)

// Verdict is the tri-state outcome of one chain source.
type Verdict int

const (
	// VerdictIgnore: the source has no record for this request; the chain
	// falls through to the next source.
	VerdictIgnore Verdict = iota
	// VerdictAllow: the source grants the request and the chain stops.
	VerdictAllow
	// VerdictDeny: the source rejects the request and the chain stops. A
	// source that found the user but rejected the credentials MUST return
	// VerdictDeny so a weak source further down the chain cannot be used to
	// bypass a strong one (docs/design/auth.md section 2).
	VerdictDeny
)

// ConnectInfo describes one CONNECT attempt presented to the authentication
// chain.
type ConnectInfo struct {
	// ClientID is the Client Identifier from CONNECT.
	ClientID string
	// Username is the User Name from CONNECT, empty when absent.
	Username string
	// Password is the Password from CONNECT, nil when absent.
	Password []byte
	// RemoteAddr is the peer network address ("ip:port").
	RemoteAddr string
	// ListenerID identifies the listener the connection arrived on.
	ListenerID string
	// ProtocolVersion is packet.Version311 or packet.Version5.
	ProtocolVersion byte
	// CleanStart is the v5 Clean Start / v3 Clean Session flag.
	CleanStart bool
	// Keepalive is the requested Keep Alive in seconds.
	Keepalive uint16
}

// Result is the outcome of a successful authentication.
type Result struct {
	// OK reports whether the client may proceed to session establishment.
	OK bool
	// Superuser marks the client as a superuser, which skips the entire
	// authorization chain (docs/design/auth.md section 2).
	Superuser bool
	// Reason carries a human readable rejection detail (never the password).
	Reason string
}

// Authenticator is one authentication source.
type Authenticator interface {
	// Authenticate inspects the CONNECT attempt. A nil error with
	// Result.OK=false stops the chain with a denial; an error indicates the
	// source itself failed and is treated as a denial (fail-closed).
	// Returning ErrNotRegistered continues the chain.
	Authenticate(ctx context.Context, info ConnectInfo) (Result, error)
}

// ErrNotRegistered is returned by an Authenticator that has no record of the
// user; the chain falls through to the next source.
var ErrNotRegistered = errors.New("hook: user not registered")

// Op is the operation an authorization decision applies to.
type Op int

const (
	// OpPublish authorizes publishing to a topic.
	OpPublish Op = iota
	// OpSubscribe authorizes subscribing to a topic filter.
	OpSubscribe
)

// String returns the operation name for logs and metrics labels.
func (o Op) String() string {
	switch o {
	case OpPublish:
		return "publish"
	case OpSubscribe:
		return "subscribe"
	default:
		return "unknown"
	}
}

// ClientInfo describes the authenticated client an authorization decision
// applies to.
type ClientInfo struct {
	ClientID string
	Username string
	// Superuser clients skip the authorization chain entirely.
	Superuser bool
	RemoteAddr string
	ListenerID string
}

// Authorizer is one authorization source.
type Authorizer interface {
	// Authorize decides whether the client may perform op on the topic
	// (a topic name for OpPublish, a topic filter for OpSubscribe). The
	// Verdict follows the chain semantics: allow/deny stop the chain, ignore
	// falls through.
	Authorize(ctx context.Context, client ClientInfo, op Op, topic string) Verdict
}

// Gate is the broker-facing authorization decision point: a boolean
// allow/deny for one operation. AuthzChain implements it.
type Gate interface {
	// Allow reports whether the client may perform op on topic.
	Allow(ctx context.Context, client ClientInfo, op Op, topic string) bool
}

// AuthChain runs the authentication chain in configuration order
// (ordered fall-through, docs/design/auth.md section 2):
//
//	found and accepted  -> allow, chain stops
//	found but rejected  -> deny, chain stops
//	not registered      -> ignore, fall through
//	every source missed -> deny
type AuthChain struct {
	sources []Authenticator
}

// NewAuthChain builds a chain over the given sources in order.
func NewAuthChain(sources ...Authenticator) *AuthChain {
	return &AuthChain{sources: sources}
}

// Authenticate implements Authenticator over the whole chain.
func (c *AuthChain) Authenticate(ctx context.Context, info ConnectInfo) (Result, error) {
	for _, src := range c.sources {
		res, err := src.Authenticate(ctx, info)
		switch {
		case errors.Is(err, ErrNotRegistered):
			continue
		case err != nil:
			return Result{OK: false, Reason: "authentication source failure"}, err
		default:
			return res, nil
		}
	}
	return Result{OK: false, Reason: "no authentication source matched"}, nil
}

// AuthzChain runs the authorization chain in configuration order: the first
// allow or deny stops the chain; when every source ignores the request the
// default is deny (docs/design/auth.md section 2). Superuser clients are
// always allowed without consulting any source.
type AuthzChain struct {
	sources []Authorizer
}

// NewAuthzChain builds a chain over the given sources in order.
func NewAuthzChain(sources ...Authorizer) *AuthzChain {
	return &AuthzChain{sources: sources}
}

// Allow implements Gate over the whole chain.
func (c *AuthzChain) Allow(ctx context.Context, client ClientInfo, op Op, topic string) bool {
	if client.Superuser {
		return true
	}
	for _, src := range c.sources {
		switch src.Authorize(ctx, client, op, topic) {
		case VerdictAllow:
			return true
		case VerdictDeny:
			return false
		}
	}
	return false
}

// AllowAll is the built-in stub used until concrete auth sources land: it
// authenticates every client as a non-superuser and authorizes every
// publish and subscribe.
type AllowAll struct{}

// Authenticate implements Authenticator, always accepting.
func (AllowAll) Authenticate(context.Context, ConnectInfo) (Result, error) {
	return Result{OK: true}, nil
}

// Authorize implements Authorizer, always allowing.
func (AllowAll) Authorize(context.Context, ClientInfo, Op, string) Verdict {
	return VerdictAllow
}

// DefaultChains returns the chains wired for this milestone: one allow-all
// stub per chain.
func DefaultChains() (*AuthChain, *AuthzChain) {
	return NewAuthChain(AllowAll{}), NewAuthzChain(AllowAll{})
}
