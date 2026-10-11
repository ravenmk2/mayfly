// Package auth implements the multi-source authentication and authorization
// chains (docs/design/auth.md): file and db sources, the compiled ACL rule
// sets with subject bucketing and decision caching, chain assembly with
// periodic refresh and fail-closed degradation.
package auth

import (
	"errors"
	"strings"
	"sync"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// ACL compilation errors.
var (
	errEmptyFilter     = errors.New("auth: rule topic filter is empty")
	errHashNotFinal    = errors.New("auth: '#' must be the final filter level")
	errWildcardInLevel = errors.New("auth: wildcard must occupy a whole filter level")
)

// Action is an ACL rule's effect.
type Action int

const (
	// ActionAllow grants the operation.
	ActionAllow Action = iota
	// ActionDeny rejects the operation.
	ActionDeny
)

// Permission is the operation set an ACL rule applies to.
type Permission int

const (
	// PermissionPublish applies to publishes.
	PermissionPublish Permission = 1 << iota
	// PermissionSubscribe applies to subscribes.
	PermissionSubscribe
	// PermissionPubsub applies to both.
	PermissionPubsub = PermissionPublish | PermissionSubscribe
)

// covers reports whether the rule permission applies to op.
func (p Permission) covers(op hook.Op) bool {
	switch op {
	case hook.OpPublish:
		return p&PermissionPublish != 0
	case hook.OpSubscribe:
		return p&PermissionSubscribe != 0
	default:
		return false
	}
}

// Rule is one ACL rule in source-agnostic input form. Seq is the rule's
// position inside its source (first hit wins); Username/ClientID are empty
// when the rule does not constrain that subject.
type Rule struct {
	Seq        int
	Username   string
	ClientID   string
	Action     Action
	Permission Permission
	Topic      string
}

// segKind classifies one compiled topic-filter level.
type segKind int

const (
	segLiteral segKind = iota
	segPlus
	segHash
	// segVarUser and segVarClient are placeholder levels ({user} /
	// {clientid}): a placeholder always occupies a whole level and matches by
	// direct comparison with the client's username/clientid, without string
	// concatenation (docs/design/auth.md section 5.1).
	segVarUser
	segVarClient
)

type segment struct {
	kind    segKind
	literal string
}

// compiledRule is the runtime form of a Rule (docs/design/auth.md
// section 5.1).
type compiledRule struct {
	seq        int
	action     Action
	permission Permission
	segments   []segment
}

// compileRule validates and compiles one rule.
func compileRule(r Rule) (compiledRule, error) {
	segs, err := compileFilter(r.Topic)
	if err != nil {
		return compiledRule{}, err
	}
	return compiledRule{seq: r.Seq, action: r.Action, permission: r.Permission, segments: segs}, nil
}

// compileFilter splits a rule topic filter into typed segments. Wildcards
// follow the MQTT rules: '#' must be the final level on its own, '+' must
// occupy a whole level; placeholders must occupy a whole level as well.
func compileFilter(filter string) ([]segment, error) {
	if filter == "" {
		return nil, errEmptyFilter
	}
	levels := strings.Split(filter, "/")
	segs := make([]segment, 0, len(levels))
	for i, level := range levels {
		switch {
		case level == "#":
			if i != len(levels)-1 {
				return nil, errHashNotFinal
			}
			segs = append(segs, segment{kind: segHash})
		case level == "+":
			segs = append(segs, segment{kind: segPlus})
		case level == "{user}":
			segs = append(segs, segment{kind: segVarUser})
		case level == "{clientid}":
			segs = append(segs, segment{kind: segVarClient})
		case strings.ContainsAny(level, "+#"):
			return nil, errWildcardInLevel
		default:
			segs = append(segs, segment{kind: segLiteral, literal: level})
		}
	}
	return segs, nil
}

// match reports whether the compiled rule matches the requested topic (a
// topic name for publish, a topic filter for subscribe). Wildcard rules do
// not match '$'-prefixed topics unless the rule itself starts with '$',
// mirroring the subscription matching rules [MQTT-4.7.2-1].
func (r *compiledRule) match(username, clientID, topic string) bool {
	if strings.HasPrefix(topic, "$") && len(r.segments) > 0 &&
		(r.segments[0].kind == segPlus || r.segments[0].kind == segHash) {
		return false
	}
	return matchSegments(r.segments, strings.Split(topic, "/"), username, clientID)
}

// matchSegments walks rule segments against topic levels; '#' matches the
// remaining levels including zero [MQTT-4.7.1-2].
func matchSegments(segs []segment, levels []string, username, clientID string) bool {
	for _, seg := range segs {
		if seg.kind == segHash {
			return true
		}
		if len(levels) == 0 {
			return false
		}
		level := levels[0]
		levels = levels[1:]
		switch seg.kind {
		case segLiteral:
			if seg.literal != level {
				return false
			}
		case segPlus:
			// matches any single level
		case segVarUser:
			if username == "" || level != username {
				return false
			}
		case segVarClient:
			if clientID == "" || level != clientID {
				return false
			}
		}
	}
	return len(levels) == 0
}

// cacheKey identifies one authorization decision. ClientID participates
// because clientid-bucketed rules depend on it.
type cacheKey struct {
	username string
	clientID string
	op       hook.Op
	topic    string
}

// RuleSet is one authorization source's compiled, immutable rule view
// (docs/design/auth.md section 5.1): rules are bucketed by subject and a
// per-generation decision cache keeps the steady-state hot path at one map
// lookup. A refresh builds a fresh RuleSet and swaps it atomically (RCU);
// the old cache dies with the old set.
type RuleSet struct {
	global     []compiledRule
	byUser     map[string][]compiledRule
	byClient   map[string][]compiledRule
	generation uint64

	mu    sync.RWMutex
	cache map[cacheKey]hook.Verdict
}

// NewRuleSet compiles rules into a bucketed RuleSet. generation identifies
// the rule revision for observability; callers increment it on every
// refresh.
func NewRuleSet(rules []Rule, generation uint64) (*RuleSet, error) {
	rs := &RuleSet{
		byUser:     map[string][]compiledRule{},
		byClient:   map[string][]compiledRule{},
		generation: generation,
		cache:      map[cacheKey]hook.Verdict{},
	}
	for _, r := range rules {
		cr, err := compileRule(r)
		if err != nil {
			return nil, err
		}
		switch {
		case r.Username != "":
			rs.byUser[r.Username] = append(rs.byUser[r.Username], cr)
		case r.ClientID != "":
			rs.byClient[r.ClientID] = append(rs.byClient[r.ClientID], cr)
		default:
			rs.global = append(rs.global, cr)
		}
	}
	return rs, nil
}

// Generation returns the rule revision this set was built from.
func (rs *RuleSet) Generation() uint64 {
	if rs == nil {
		return 0
	}
	return rs.generation
}

// Decide resolves one authorization request against the compiled rules:
// candidates are the client's user bucket, clientid bucket and the global
// bucket merged in seq order; the first matching rule wins. No hit yields
// hook.VerdictIgnore so the chain falls through.
func (rs *RuleSet) Decide(username, clientID string, op hook.Op, topic string) hook.Verdict {
	key := cacheKey{username: username, clientID: clientID, op: op, topic: topic}
	rs.mu.RLock()
	v, ok := rs.cache[key]
	rs.mu.RUnlock()
	if ok {
		return v
	}
	v = rs.decideSlow(username, clientID, op, topic)
	rs.mu.Lock()
	rs.cache[key] = v
	rs.mu.Unlock()
	return v
}

// decideSlow merges the three subject buckets in seq order and returns the
// first matching rule's verdict.
func (rs *RuleSet) decideSlow(username, clientID string, op hook.Op, topic string) hook.Verdict {
	merged := mergeBySeq(rs.global, rs.byUser[username], rs.byClient[clientID])
	for i := range merged {
		r := &merged[i]
		if !r.permission.covers(op) {
			continue
		}
		if !r.match(username, clientID, topic) {
			continue
		}
		if r.action == ActionAllow {
			return hook.VerdictAllow
		}
		return hook.VerdictDeny
	}
	return hook.VerdictIgnore
}

// mergeBySeq returns the union of the buckets ordered by rule seq.
func mergeBySeq(buckets ...[]compiledRule) []compiledRule {
	n := 0
	for _, b := range buckets {
		n += len(b)
	}
	out := make([]compiledRule, 0, n)
	for _, b := range buckets {
		out = append(out, b...)
	}
	sortBySeq(out)
	return out
}

// sortBySeq insertion-sorts the (almost always tiny) candidate slice by seq.
func sortBySeq(rules []compiledRule) {
	for i := 1; i < len(rules); i++ {
		for j := i; j > 0 && rules[j].seq < rules[j-1].seq; j-- {
			rules[j], rules[j-1] = rules[j-1], rules[j]
		}
	}
}
