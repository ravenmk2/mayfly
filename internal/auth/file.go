package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/pelletier/go-toml/v2"
	"go.yaml.in/yaml/v3"

	"github.com/ravenmk2/mayfly/internal/hook"
)

// File layout (docs/design/auth.md section 3), parsed by extension
// (.json / .yaml / .yml / .toml):
//
//	# users.yaml
//	users:
//	  sensor1: { password: "$2y$10$...(bcrypt)", superuser: false }
//
//	# acl.yaml ("*" = any subject; array order is the rule seq)
//	rules:
//	  - { username: "*", action: allow, permission: publish, topic: "up/{user}" }

// fileUser is one entry of the users document.
type fileUser struct {
	Password  string `json:"password" yaml:"password" toml:"password"`
	Superuser bool   `json:"superuser" yaml:"superuser" toml:"superuser"`
}

type usersDocument struct {
	Users map[string]fileUser `json:"users" yaml:"users" toml:"users"`
}

// fileRule is one entry of the acl document.
type fileRule struct {
	Username   string `json:"username" yaml:"username" toml:"username"`
	ClientID   string `json:"clientid" yaml:"clientid" toml:"clientid"`
	Action     string `json:"action" yaml:"action" toml:"action"`
	Permission string `json:"permission" yaml:"permission" toml:"permission"`
	Topic      string `json:"topic" yaml:"topic" toml:"topic"`
}

type aclDocument struct {
	Rules []fileRule `json:"rules" yaml:"rules" toml:"rules"`
}

// decodeFile unmarshals data according to the file extension of path.
func decodeFile(path string, data []byte, out any) error {
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".json":
		return json.Unmarshal(data, out)
	case ".yaml", ".yml":
		return yaml.Unmarshal(data, out)
	case ".toml":
		return toml.Unmarshal(data, out)
	default:
		return fmt.Errorf("unsupported extension %q (want .json/.yaml/.yml/.toml)", ext)
	}
}

// FileUsers is a users-file authentication source. Reload re-reads the file
// and atomically swaps the parsed table (RCU); a failed reload keeps the
// last good table, and a source that never loaded fails closed.
type FileUsers struct {
	path  string
	table atomic.Pointer[map[string]userRecord]
}

// NewFileUsers loads the users file. A load failure is returned but the
// source stays usable: it denies every lookup until a Reload succeeds.
func NewFileUsers(path string) (*FileUsers, error) {
	f := &FileUsers{path: path}
	return f, f.Reload()
}

// Path returns the backing file path.
func (f *FileUsers) Path() string { return f.path }

// Reload re-reads and re-parses the users file, swapping the table
// atomically on success.
func (f *FileUsers) Reload() error {
	table, err := loadUsersFile(f.path)
	if err != nil {
		return err
	}
	f.table.Store(&table)
	return nil
}

// Authenticate implements hook.Authenticator.
func (f *FileUsers) Authenticate(_ context.Context, info hook.ConnectInfo) (hook.Result, error) {
	table := f.table.Load()
	if table == nil {
		return hook.Result{OK: false, Reason: "authentication source failure"}, errSourceUnavailable
	}
	return authenticateUsers(*table, info)
}

// loadUsersFile reads and validates a users file.
func loadUsersFile(path string) (map[string]userRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read users file: %w", err)
	}
	var doc usersDocument
	if err := decodeFile(path, data, &doc); err != nil {
		return nil, fmt.Errorf("parse users file %s: %w", path, err)
	}
	table := make(map[string]userRecord, len(doc.Users))
	for name, u := range doc.Users {
		if name == "" {
			return nil, errors.New("parse users file: empty username")
		}
		if u.Password == "" {
			return nil, fmt.Errorf("parse users file: user %q has no password hash", name)
		}
		table[name] = userRecord{hash: u.Password, superuser: u.Superuser}
	}
	return table, nil
}

// FileACL is an acl-file authorization source with the same RCU reload
// semantics as FileUsers.
type FileACL struct {
	path  string
	rules atomic.Pointer[RuleSet]
	gen   atomic.Uint64
}

// NewFileACL loads the acl file. A load failure is returned but the source
// stays usable: it denies every decision until a Reload succeeds.
func NewFileACL(path string) (*FileACL, error) {
	f := &FileACL{path: path}
	return f, f.Reload()
}

// Path returns the backing file path.
func (f *FileACL) Path() string { return f.path }

// Reload re-reads the acl file, compiles a fresh RuleSet and swaps it
// atomically, invalidating the previous decision cache with it.
func (f *FileACL) Reload() error {
	rs, err := loadACLFile(f.path, f.gen.Add(1))
	if err != nil {
		return err
	}
	f.rules.Store(rs)
	return nil
}

// Authorize implements hook.Authorizer.
func (f *FileACL) Authorize(_ context.Context, client hook.ClientInfo, op hook.Op, topic string) hook.Verdict {
	return authorizeRules(f.rules.Load(), client, op, topic)
}

// loadACLFile reads, validates and compiles an acl file.
func loadACLFile(path string, generation uint64) (*RuleSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read acl file: %w", err)
	}
	var doc aclDocument
	if err := decodeFile(path, data, &doc); err != nil {
		return nil, fmt.Errorf("parse acl file %s: %w", path, err)
	}
	rules := make([]Rule, 0, len(doc.Rules))
	for i, fr := range doc.Rules {
		r, err := convertFileRule(fr, i)
		if err != nil {
			return nil, fmt.Errorf("parse acl file %s rule %d: %w", path, i, err)
		}
		rules = append(rules, r)
	}
	return NewRuleSet(rules, generation)
}

// convertFileRule maps the file form ("*" = any subject) to a Rule.
func convertFileRule(fr fileRule, seq int) (Rule, error) {
	action, err := parseAction(fr.Action)
	if err != nil {
		return Rule{}, err
	}
	perm, err := parsePermission(fr.Permission)
	if err != nil {
		return Rule{}, err
	}
	r := Rule{Seq: seq, Action: action, Permission: perm, Topic: fr.Topic}
	if fr.Username != "*" {
		r.Username = fr.Username
	}
	if fr.ClientID != "*" {
		r.ClientID = fr.ClientID
	}
	return r, nil
}

func parseAction(s string) (Action, error) {
	switch s {
	case "allow":
		return ActionAllow, nil
	case "deny":
		return ActionDeny, nil
	default:
		return ActionAllow, fmt.Errorf("invalid action %q (want allow|deny)", s)
	}
}

func parsePermission(s string) (Permission, error) {
	switch s {
	case "publish":
		return PermissionPublish, nil
	case "subscribe":
		return PermissionSubscribe, nil
	case "pubsub":
		return PermissionPubsub, nil
	default:
		return PermissionPubsub, fmt.Errorf("invalid permission %q (want publish|subscribe|pubsub)", s)
	}
}
