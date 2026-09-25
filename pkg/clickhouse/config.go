package clickhouse

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.k6.io/k6/v2/output"
)

// validIdentifierRegex matches valid ClickHouse identifiers
// Alphanumeric + underscore, 1-63 characters
var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_]{1,63}$`)

// isValidIdentifier validates ClickHouse identifier names
func isValidIdentifier(name string) bool {
	return validIdentifierRegex.MatchString(name)
}

// maxRetryAttempts caps config.RetryAttempts. A sane upper bound prevents two
// footguns: a typo'd huge value stalling every flush (and hanging Stop()), and
// an integer overflow where flush() passes retry.Attempts(RetryAttempts+1) —
// MaxUint+1 wraps to 0, which retry-go interprets as INFINITE retry. See validate().
const maxRetryAttempts = 100

// Valid values for config.BufferDropPolicy.
const (
	dropOldest = "oldest"
	dropNewest = "newest"
)

// tlsOptions holds TLS/SSL configuration options
type tlsOptions struct {
	// Enabled controls whether TLS is enabled
	Enabled bool

	// InsecureSkipVerify disables certificate verification (INSECURE - use only for testing)
	InsecureSkipVerify bool

	// CAFile is the path to a CA certificate file to append to the system pool
	CAFile string

	// CertFile is the path to a client certificate file for mTLS
	CertFile string

	// KeyFile is the path to a client private key file for mTLS
	KeyFile string

	// ServerName is the server name for SNI (Server Name Indication)
	ServerName string
}

// config holds the ClickHouse output configuration. defaultConfig returns the
// defaults; options lists every key and its environment variable.
//
// Configuration sources (in priority order):
//  1. Environment variables (K6_CLICKHOUSE_*)
//  2. URL parameters (e.g. --out xk6-clickhouse=...?param=value)
//  3. JSON config file (collectors.xk6-clickhouse, via --config)
//  4. Default values
type config struct {
	// Addr is the ClickHouse server address (host:port).
	Addr string

	// User is the ClickHouse username.
	User string

	// Password is the ClickHouse password.
	Password string

	// Database is the database name to store metrics.
	Database string

	// Table is the table name to store metrics.
	Table string

	// PushInterval is how often to flush metrics to ClickHouse.
	PushInterval time.Duration

	// SchemaMode determines the table schema ("simple" or "compatible").
	SchemaMode string

	// SkipSchemaCreation disables automatic database and table creation.
	SkipSchemaCreation bool

	// TLS holds TLS/SSL configuration
	TLS tlsOptions

	// RetryAttempts is the maximum number of retries per flush; 0 fails immediately.
	RetryAttempts uint

	// RetryDelay is the initial delay between retries, doubled on each attempt.
	RetryDelay time.Duration

	// RetryMaxDelay caps the exponential backoff delay.
	RetryMaxDelay time.Duration

	// BufferEnabled keeps samples from failed flushes in memory and retries
	// them on the next flush.
	BufferEnabled bool

	// BufferMaxSamples is the maximum number of samples to buffer.
	BufferMaxSamples int

	// BufferDropPolicy picks which samples to drop when the buffer is full:
	// "oldest" (keep recent samples) or "newest" (drop incoming samples).
	BufferDropPolicy string
}

// validate checks the configuration for validity
//
//nolint:gocyclo // complexity is acceptable for validation with many fields
func (c config) validate() error {
	if c.Addr == "" {
		return fmt.Errorf("clickhouse address is required")
	}

	if c.User == "" {
		return fmt.Errorf("clickhouse user is required")
	}

	if c.Database == "" {
		return fmt.Errorf("clickhouse database name is required")
	}

	if !isValidIdentifier(c.Database) {
		return fmt.Errorf("invalid database name: %s (must be alphanumeric + underscore, max 63 chars)", c.Database)
	}

	if c.Table == "" {
		return fmt.Errorf("clickhouse table name is required")
	}

	if !isValidIdentifier(c.Table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric + underscore, max 63 chars)", c.Table)
	}

	if c.PushInterval <= 0 {
		return fmt.Errorf("push interval must be positive, got %v", c.PushInterval)
	}

	if c.RetryAttempts > maxRetryAttempts {
		return fmt.Errorf("retry attempts must not exceed %d, got %d", maxRetryAttempts, c.RetryAttempts)
	}
	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be non-negative, got %v", c.RetryDelay)
	}
	if c.RetryMaxDelay < 0 {
		return fmt.Errorf("retry max delay must be non-negative, got %v", c.RetryMaxDelay)
	}
	// A zero max delay disables the exponential-backoff cap, letting per-retry
	// delays grow without bound. Require a positive cap whenever retries with a
	// non-zero base delay are enabled, so a misconfigured "0" can't stall flushes.
	if c.RetryAttempts > 0 && c.RetryDelay > 0 && c.RetryMaxDelay == 0 {
		return fmt.Errorf("retry max delay must be positive when retries are enabled (got 0); set retryMaxDelay to cap exponential backoff")
	}
	if c.RetryMaxDelay > 0 && c.RetryDelay > c.RetryMaxDelay {
		return fmt.Errorf("retry delay (%v) cannot exceed max delay (%v)", c.RetryDelay, c.RetryMaxDelay)
	}

	if c.BufferEnabled && c.BufferMaxSamples <= 0 {
		return fmt.Errorf("buffer max samples must be positive when buffering is enabled, got %d", c.BufferMaxSamples)
	}
	if c.BufferDropPolicy != "" && c.BufferDropPolicy != dropOldest && c.BufferDropPolicy != dropNewest {
		return fmt.Errorf("invalid buffer drop policy: %s (valid: %s, %s)", c.BufferDropPolicy, dropOldest, dropNewest)
	}

	return nil
}

// defaultConfig returns a config with default values
func defaultConfig() config {
	return config{
		Addr:             "localhost:9000",
		User:             "default",
		Database:         "k6",
		Table:            "samples",
		PushInterval:     1 * time.Second,
		SchemaMode:       "simple",
		RetryAttempts:    3,
		RetryDelay:       100 * time.Millisecond,
		RetryMaxDelay:    5 * time.Second,
		BufferEnabled:    true,
		BufferMaxSamples: 100000,
		BufferDropPolicy: dropOldest,
	}
}

// envPrefix is the prefix shared by every option's environment variable.
const envPrefix = "K6_CLICKHOUSE_"

// option is a config key and its environment variable.
type option struct{ key, env string }

// options lists every config key with its environment variable, in the
// order shown in error messages.
var options = []option{
	{"addr", "K6_CLICKHOUSE_ADDR"},
	{"user", "K6_CLICKHOUSE_USER"},
	{"password", "K6_CLICKHOUSE_PASSWORD"},
	{"database", "K6_CLICKHOUSE_DB"},
	{"table", "K6_CLICKHOUSE_TABLE"},
	{"pushInterval", "K6_CLICKHOUSE_PUSH_INTERVAL"},
	{"schemaMode", "K6_CLICKHOUSE_SCHEMA_MODE"},
	{"skipSchemaCreation", "K6_CLICKHOUSE_SKIP_SCHEMA_CREATION"},
	{"tlsEnabled", "K6_CLICKHOUSE_TLS_ENABLED"},
	{"tlsInsecureSkipVerify", "K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY"},
	{"tlsCAFile", "K6_CLICKHOUSE_TLS_CA_FILE"},
	{"tlsCertFile", "K6_CLICKHOUSE_TLS_CERT_FILE"},
	{"tlsKeyFile", "K6_CLICKHOUSE_TLS_KEY_FILE"},
	{"tlsServerName", "K6_CLICKHOUSE_TLS_SERVER_NAME"},
	{"retryAttempts", "K6_CLICKHOUSE_RETRY_ATTEMPTS"},
	{"retryDelay", "K6_CLICKHOUSE_RETRY_DELAY"},
	{"retryMaxDelay", "K6_CLICKHOUSE_RETRY_MAX_DELAY"},
	{"bufferEnabled", "K6_CLICKHOUSE_BUFFER_ENABLED"},
	{"bufferMaxSamples", "K6_CLICKHOUSE_BUFFER_MAX_SAMPLES"},
	{"bufferDropPolicy", "K6_CLICKHOUSE_BUFFER_DROP_POLICY"},
}

// jsonTLSKeys maps the keys of the JSON config's "tls" object to option keys.
var jsonTLSKeys = map[string]string{
	"enabled":            "tlsEnabled",
	"insecureSkipVerify": "tlsInsecureSkipVerify",
	"caFile":             "tlsCAFile",
	"certFile":           "tlsCertFile",
	"keyFile":            "tlsKeyFile",
	"serverName":         "tlsServerName",
}

// isOption reports whether key is listed in options.
func isOption(key string) bool {
	return slices.ContainsFunc(options, func(o option) bool { return o.key == key })
}

// unknownOptionError reports a key that is not listed in options.
func unknownOptionError(key string) error {
	keys := make([]string, len(options))
	for i, o := range options {
		keys[i] = o.key
	}
	return fmt.Errorf("unknown option %q (valid options: %s)", key, strings.Join(keys, ", "))
}

// set parses value and assigns it to the option key. An empty value leaves
// the option unchanged.
//
//nolint:gocyclo // one case per option
func (c *config) set(key, value string) error {
	if !isOption(key) {
		return unknownOptionError(key)
	}
	if value == "" {
		return nil
	}

	var err error
	switch key {
	case "addr":
		c.Addr = value
	case "user":
		c.User = value
	case "password":
		c.Password = value
	case "database":
		c.Database = value
	case "table":
		c.Table = value
	case "pushInterval":
		c.PushInterval, err = time.ParseDuration(value)
	case "schemaMode":
		c.SchemaMode = value
	case "skipSchemaCreation":
		c.SkipSchemaCreation, err = strconv.ParseBool(value)
	case "tlsEnabled":
		c.TLS.Enabled, err = strconv.ParseBool(value)
	case "tlsInsecureSkipVerify":
		c.TLS.InsecureSkipVerify, err = strconv.ParseBool(value)
	case "tlsCAFile":
		c.TLS.CAFile = value
	case "tlsCertFile":
		c.TLS.CertFile = value
	case "tlsKeyFile":
		c.TLS.KeyFile = value
	case "tlsServerName":
		c.TLS.ServerName = value
	case "retryAttempts":
		var n uint64
		n, err = strconv.ParseUint(value, 10, 32)
		c.RetryAttempts = uint(n)
	case "retryDelay":
		c.RetryDelay, err = time.ParseDuration(value)
	case "retryMaxDelay":
		c.RetryMaxDelay, err = time.ParseDuration(value)
	case "bufferEnabled":
		c.BufferEnabled, err = strconv.ParseBool(value)
	case "bufferMaxSamples":
		c.BufferMaxSamples, err = strconv.Atoi(value)
	case "bufferDropPolicy":
		c.BufferDropPolicy = value
	default:
		return unknownOptionError(key)
	}
	if err != nil {
		return fmt.Errorf("invalid %s value %q: %w", key, value, err)
	}
	return nil
}

// parseConfig builds a config from defaults, the JSON config, the config
// argument and the environment, each overriding the previous, and validates it.
func parseConfig(params output.Params) (config, error) {
	cfg := defaultConfig()

	if params.JSONConfig != nil {
		if err := cfg.applyJSON(params.JSONConfig); err != nil {
			return cfg, fmt.Errorf("json config: %w", err)
		}
	}

	if params.ConfigArgument != "" {
		if err := cfg.applyArgument(params.ConfigArgument); err != nil {
			return cfg, fmt.Errorf("invalid --out argument: %w", err)
		}
	}

	for _, o := range options {
		if err := cfg.set(o.key, params.Environment[o.env]); err != nil {
			return cfg, fmt.Errorf("%s: %w", o.env, err)
		}
	}

	if err := cfg.validate(); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyJSON applies the collectors.xk6-clickhouse object. Keys match
// case-insensitively, as encoding/json matches struct fields. TLS options may
// be nested in a "tls" object.
func (c *config) applyJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	names := make([]string, 0, len(options)+1)
	for _, o := range options {
		names = append(names, o.key)
	}
	fields, err := foldKeys(raw, append(names, "tls"), "", unknownOptionError)
	if err != nil {
		return err
	}

	if tlsValue, ok := fields["tls"]; ok {
		delete(fields, "tls")
		var tlsRaw map[string]json.RawMessage
		if err := json.Unmarshal(tlsValue, &tlsRaw); err != nil {
			return fmt.Errorf("invalid tls value: %w", err)
		}
		tlsNames := slices.Sorted(maps.Keys(jsonTLSKeys))
		tlsFields, tlsErr := foldKeys(tlsRaw, tlsNames, "tls.", func(name string) error {
			return fmt.Errorf("unknown tls option %q (valid tls options: %s)", name, strings.Join(tlsNames, ", "))
		})
		if tlsErr != nil {
			return tlsErr
		}
		for _, name := range slices.Sorted(maps.Keys(tlsFields)) {
			key := jsonTLSKeys[name]
			if _, dup := fields[key]; dup {
				return fmt.Errorf("both tls.%s and %s are set", name, key)
			}
			fields[key] = tlsFields[name]
		}
	}

	for _, key := range slices.Sorted(maps.Keys(fields)) {
		value, err := jsonScalar(fields[key])
		if err != nil {
			return fmt.Errorf("invalid %s value: %w", key, err)
		}
		if err := c.set(key, value); err != nil {
			return err
		}
	}
	return nil
}

// foldKeys re-keys fields by the name in names that each key matches
// case-insensitively. It rejects a key matching no name, and two keys matching
// the same name; prefix qualifies the name in that error.
func foldKeys(fields map[string]json.RawMessage, names []string, prefix string,
	unknown func(key string) error,
) (map[string]json.RawMessage, error) {
	folded := make(map[string]json.RawMessage, len(fields))
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		i := slices.IndexFunc(names, func(name string) bool { return strings.EqualFold(name, key) })
		if i < 0 {
			return nil, unknown(key)
		}
		if _, dup := folded[names[i]]; dup {
			return nil, fmt.Errorf("%s%s is set more than once", prefix, names[i])
		}
		folded[names[i]] = fields[key]
	}
	return folded, nil
}

// jsonScalar returns the option text of a JSON value: a string's contents, a
// number's or boolean's literal, or "" (unset) for null.
func jsonScalar(raw json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	switch v := v.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case json.Number:
		return v.String(), nil
	default:
		return "", errors.New("must be a string, number, boolean or null")
	}
}

// schemeRegex matches a URL scheme and "://" at the start of the --out argument.
var schemeRegex = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)

// errMalformedDSN reports an --out argument that cannot be parsed without
// risking a password in the error, so it quotes none of the argument.
var errMalformedDSN = errors.New("malformed DSN: percent-encode reserved characters (@ : / ? # %) in the user name and password")

// parseArgumentURL parses the --out argument as a ClickHouse DSN, prefixing
// a "clickhouse://" scheme when arg has none (a bare "host:port" is not a
// URL — url.Parse would misread the host as a scheme). It rejects any other
// scheme and a fragment (which would otherwise silently truncate a password
// containing an unencoded '#').
func parseArgumentURL(arg string) (*url.URL, error) {
	withScheme := arg
	if !schemeRegex.MatchString(arg) {
		withScheme = "clickhouse://" + arg
	}

	u, err := url.Parse(withScheme)
	if err != nil {
		// The inner error may quote part of a password that an unencoded
		// reserved character split off the userinfo.
		if strings.Contains(arg, "@") {
			return nil, errMalformedDSN
		}
		// *url.Error embeds the whole input in its message; unwrap to the
		// inner error.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return nil, err
	}

	// An unencoded '/' or '?' in the userinfo ends it early, leaving the rest
	// of the password and the '@' in the host, path or a query key, which
	// later errors would echo. None of them can legitimately contain '@'.
	if strings.Contains(u.Host, "@") || strings.Contains(u.Path, "@") || queryKeyContainsAt(u.RawQuery) {
		return nil, errMalformedDSN
	}

	if u.Scheme != "clickhouse" {
		return nil, fmt.Errorf("unsupported scheme %q (use clickhouse://)", u.Scheme)
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("fragment not allowed in address; percent-encode '#' as %%23 if it belongs to the password")
	}

	return u, nil
}

// queryKeyContainsAt reports whether a key of rawQuery contains '@'.
func queryKeyContainsAt(rawQuery string) bool {
	for pair := range strings.SplitSeq(rawQuery, "&") {
		if key, _, _ := strings.Cut(pair, "="); strings.Contains(key, "@") {
			return true
		}
	}
	return false
}

// applyArgument applies the --out argument, a ClickHouse DSN of the form
// [clickhouse://][user[:password]@]host:port[/database][?option=value&...].
func (c *config) applyArgument(arg string) error {
	u, err := parseArgumentURL(arg)
	if err != nil {
		return err
	}

	if err := c.set("addr", u.Host); err != nil {
		return err
	}
	if u.User != nil {
		if err := c.set("user", u.User.Username()); err != nil {
			return err
		}
		if password, ok := u.User.Password(); ok {
			if err := c.set("password", password); err != nil {
				return err
			}
		}
	}

	database := strings.TrimPrefix(u.Path, "/")
	if strings.Contains(database, "/") {
		return fmt.Errorf("address path must be a single database segment, got %q", database)
	}
	if err := c.set("database", database); err != nil {
		return err
	}

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(query)) {
		if err := c.set(key, query.Get(key)); err != nil {
			return err
		}
	}
	return nil
}

// unknownEnvVars returns the sorted K6_CLICKHOUSE_* variables in env that
// match no option, such as misspelled names.
func unknownEnvVars(env map[string]string) []string {
	var unknown []string
	for name := range env {
		if strings.HasPrefix(name, envPrefix) &&
			!slices.ContainsFunc(options, func(o option) bool { return o.env == name }) {
			unknown = append(unknown, name)
		}
	}
	slices.Sort(unknown)
	return unknown
}

// build builds a *tls.Config from the tlsOptions settings.
// Returns nil, nil if TLS is not enabled (valid nil value, not an error)
func (tc tlsOptions) build() (*tls.Config, error) {
	if !tc.Enabled {
		return nil, nil //nolint:nilnil // nil TLS config is valid when TLS is disabled
	}

	// Client certificate and key must be specified together, or neither.
	if (tc.CertFile != "") != (tc.KeyFile != "") {
		return nil, fmt.Errorf("TLS client certificate and key must be specified together")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: tc.InsecureSkipVerify, //nolint:gosec // G402: User-configurable option for testing purposes
		ServerName:         tc.ServerName,
	}

	var certPool *x509.CertPool
	var err error

	certPool, err = x509.SystemCertPool()
	if err != nil {
		// On some systems (like Windows), SystemCertPool might not be available.
		certPool = x509.NewCertPool()
	}

	if tc.CAFile != "" {
		caCert, err := os.ReadFile(tc.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", tc.CAFile, err)
		}

		if ok := certPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("failed to parse CA certificate from %s: no valid certificates found", tc.CAFile)
		}
	}

	tlsConfig.RootCAs = certPool

	if tc.CertFile != "" && tc.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
