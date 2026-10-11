package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// envPrefix marks the configuration environment variables.
const envPrefix = "MAYFLY_"

// Load reads the YAML file at path, applies MAYFLY_* environment variable
// overrides and validates the result. When path is empty DefaultPath is
// used; a missing default file is not an error — Load returns the built-in
// Default and fromFile=false so the caller can warn. A missing explicitly
// requested file is an error.
//
// Environment overrides use the YAML key path with MAYFLY_ prefixed and
// underscores separating segments, matching keys greedily so multi-word
// keys keep working: MAYFLY_LOG_LEVEL sets log.level,
// MAYFLY_DATA_DIR sets data_dir, MAYFLY_BROKER__MAX_PACKET_SIZE and
// MAYFLY_BROKER_MAX_PACKET_SIZE both set broker.max_packet_size. Listeners
// and auth chains (arrays) are not settable via the environment.
func Load(path string) (cfg *Config, fromFile bool, err error) {
	explicit := path != ""
	if !explicit {
		path = DefaultPath
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		cfg = Default()
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
			return nil, false, fmt.Errorf("config: parse %s: %w", path, err)
		}
		fromFile = true
	case errors.Is(err, fs.ErrNotExist) && !explicit:
		cfg = Default()
	case errors.Is(err, fs.ErrNotExist):
		return nil, false, fmt.Errorf("config: %s: %w", path, err)
	default:
		return nil, false, fmt.Errorf("config: read %s: %w", path, err)
	}

	if err := applyEnv(cfg, os.Environ()); err != nil {
		return nil, false, err
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, false, err
	}
	return cfg, fromFile, nil
}

// applyEnv overlays MAYFLY_* variables onto cfg. Variables that do not map
// to a known key are reported so typos do not silently no-op.
func applyEnv(cfg *Config, environ []string) error {
	for _, kv := range environ {
		key, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(key, envPrefix) {
			continue
		}
		rest := strings.TrimPrefix(key, envPrefix)
		// Reserved namespaces not backed by the YAML model: the setup
		// wizard (docs/design/setup.md section 5) and test hooks.
		if strings.HasPrefix(rest, "SETUP_") || strings.HasPrefix(rest, "TEST_") {
			continue
		}
		tokens := strings.Split(strings.ToLower(rest), "_")
		tokens = compact(tokens)
		if len(tokens) == 0 {
			continue
		}
		if err := setEnvValue(reflect.ValueOf(cfg).Elem(), tokens, value, key); err != nil {
			return err
		}
	}
	return nil
}

// compact drops empty tokens produced by doubled underscores, so both
// MAYFLY_BROKER_MAX_CONNECTIONS and MAYFLY_BROKER__MAX_CONNECTIONS work.
func compact(tokens []string) []string {
	out := tokens[:0]
	for _, t := range tokens {
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

// setEnvValue walks the struct fields of v (a struct) matching the yaml key
// path greedily — multi-word keys like max_packet_size are matched by
// joining tokens — and parses value into the leaf field.
func setEnvValue(v reflect.Value, tokens []string, value, envKey string) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		name := yamlFieldName(t.Field(i))
		if name == "" {
			continue
		}
		fieldTokens := strings.Split(name, "_")
		if len(fieldTokens) > len(tokens) || !tokensMatch(fieldTokens, tokens[:len(fieldTokens)]) {
			continue
		}
		fv := v.Field(i)
		rest := tokens[len(fieldTokens):]
		if len(rest) == 0 {
			return parseEnvScalar(fv, value, envKey)
		}
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				fv.Set(reflect.New(fv.Type().Elem()))
			}
			fv = fv.Elem()
		}
		if fv.Kind() != reflect.Struct {
			return fmt.Errorf("config: %s does not map to a configuration key", envKey)
		}
		return setEnvValue(fv, rest, value, envKey)
	}
	return fmt.Errorf("config: %s does not map to a configuration key", envKey)
}

// tokensMatch reports whether the field name tokens equal the leading
// environment tokens.
func tokensMatch(field, env []string) bool {
	for i := range field {
		if field[i] != env[i] {
			return false
		}
	}
	return true
}

// yamlFieldName returns the yaml key of f, empty for fields excluded from
// the YAML model.
func yamlFieldName(f reflect.StructField) string {
	tag := f.Tag.Get("yaml")
	if tag == "-" {
		return ""
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		name = strings.ToLower(f.Name)
	}
	return name
}

// parseEnvScalar assigns value to the leaf field fv.
func parseEnvScalar(fv reflect.Value, value, envKey string) error {
	if fv.Kind() == reflect.Pointer && fv.Type().Elem().Kind() == reflect.Bool {
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("config: %s: invalid boolean %q", envKey, value)
		}
		fv.Set(reflect.ValueOf(&b))
		return nil
	}
	if fv.Type() == reflect.TypeOf(Duration(0)) {
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("config: %s: invalid duration %q", envKey, value)
		}
		fv.SetInt(int64(d))
		return nil
	}
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(value)
	case reflect.Bool:
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("config: %s: invalid boolean %q", envKey, value)
		}
		fv.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(value, 10, fv.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: %s: invalid integer %q", envKey, value)
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(value, 10, fv.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: %s: invalid unsigned integer %q", envKey, value)
		}
		fv.SetUint(n)
	default:
		return fmt.Errorf("config: %s does not map to a settable configuration key", envKey)
	}
	return nil
}
