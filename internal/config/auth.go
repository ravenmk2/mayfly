package config

import (
	"time"

	"github.com/ravenmk2/mayfly/internal/auth"
)

// ToAuthConfig maps the YAML model onto the chain assembly input of
// internal/auth. Sources default to enabled unless explicitly disabled.
func (a *AuthConfig) ToAuthConfig() auth.Config {
	cfg := auth.Config{
		AnonymousAllow:  a.AnonymousAllow,
		RefreshInterval: time.Duration(a.RefreshInterval),
	}
	cfg.Authentication = toAuthSources(a.Authentication)
	cfg.Authorization = toAuthSources(a.Authorization)
	return cfg
}

// HasSources reports whether any chain source is configured (enabled or
// not); without sources the broker stays on the allow-all stub.
func (a *AuthConfig) HasSources() bool {
	return len(a.Authentication) > 0 || len(a.Authorization) > 0
}

// toAuthSources converts one chain.
func toAuthSources(in []AuthSourceConfig) []auth.SourceConfig {
	out := make([]auth.SourceConfig, 0, len(in))
	for _, sc := range in {
		out = append(out, auth.SourceConfig{
			Type:    sc.Type,
			Enabled: sc.Enabled == nil || *sc.Enabled,
			Path:    sc.Path,
			DSN:     sc.DSN,
		})
	}
	return out
}
