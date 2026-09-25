package clickhouse

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

var (
	testTLSDir     string
	testCACertFile string
	testClientCert string
	testClientKey  string
)

func TestMain(m *testing.M) {
	var err error
	testTLSDir, err = os.MkdirTemp("", "tls-test-*")
	if err != nil {
		panic(err)
	}
	testCACertFile = generateCACert(testTLSDir)
	testClientCert, testClientKey = generateClientCert(testTLSDir)

	baseGoroutines := runtime.NumGoroutine()
	code := m.Run()
	_ = os.RemoveAll(testTLSDir)

	// Guard against goroutine leaks (e.g. a periodic flusher outliving Stop()).
	// Only enforced in -short runs: the testcontainers-based integration tests
	// leave background goroutines that would otherwise cause false positives.
	if code == 0 && testing.Short() {
		if n := settleGoroutines(baseGoroutines); n > baseGoroutines {
			fmt.Fprintf(os.Stderr, "goroutine leak detected: %d at start, %d after tests\n", baseGoroutines, n)
			code = 1
		}
	}
	os.Exit(code)
}

// settleGoroutines polls the goroutine count for up to ~1s, giving runtime and
// test goroutines a window to wind down, and returns the lowest count observed.
func settleGoroutines(base int) int {
	n := runtime.NumGoroutine()
	for i := 0; n > base && i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		if c := runtime.NumGoroutine(); c < n {
			n = c
		}
	}
	return n
}

// generateCACert creates a CA certificate file in dir and returns its path.
func generateCACert(dir string) string {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		panic(err)
	}

	certFile := filepath.Join(dir, "ca.pem")
	f, err := os.Create(certFile)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		panic(err)
	}

	return certFile
}

// generateClientCert creates client certificate and key files in dir and returns their paths.
func generateClientCert(dir string) (certFile, keyFile string) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		panic(err)
	}

	certFile = filepath.Join(dir, "client-cert.pem")
	cf, err := os.Create(certFile)
	if err != nil {
		panic(err)
	}
	defer func() { _ = cf.Close() }()

	if err := pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		panic(err)
	}

	keyFile = filepath.Join(dir, "client-key.pem")
	kf, err := os.Create(keyFile)
	if err != nil {
		panic(err)
	}
	defer func() { _ = kf.Close() }()

	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	if err := pem.Encode(kf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes}); err != nil {
		panic(err)
	}

	return certFile, keyFile
}

func TestTLSConfig_BuildTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: false,
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	assert.Nil(t, result, "BuildTLSConfig should return nil when TLS is disabled")
}

func TestTLSConfig_BuildTLSConfig_EnabledWithSystemCA(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: true,
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.False(t, result.InsecureSkipVerify)
	assert.NotNil(t, result.RootCAs, "RootCAs should be set (system pool)")
	assert.Empty(t, result.Certificates, "No client certificates should be loaded")
}

func TestTLSConfig_BuildTLSConfig_WithCustomCA(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: true,
		CAFile:  testCACertFile,
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.NotNil(t, result.RootCAs, "RootCAs should include custom CA")
	assert.False(t, result.InsecureSkipVerify)
}

func TestTLSConfig_BuildTLSConfig_WithClientCertificate(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:  true,
		CertFile: testClientCert,
		KeyFile:  testClientKey,
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Len(t, result.Certificates, 1, "Client certificate should be loaded")
	assert.NotNil(t, result.RootCAs)
}

func TestTLSConfig_BuildTLSConfig_InsecureSkipVerify(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:            true,
		InsecureSkipVerify: true,
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.InsecureSkipVerify, "InsecureSkipVerify should be true")
}

func TestTLSConfig_BuildTLSConfig_WithServerName(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:    true,
		ServerName: "clickhouse.example.com",
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "clickhouse.example.com", result.ServerName, "ServerName should be set for SNI")
}

func TestTLSConfig_BuildTLSConfig_InvalidCAFile(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: true,
		CAFile:  "/nonexistent/ca.pem",
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to read CA certificate file")
}

func TestTLSConfig_BuildTLSConfig_InvalidCAContent(t *testing.T) {
	t.Parallel()

	// Create a file with invalid PEM content
	invalidCAFile := filepath.Join(t.TempDir(), "invalid-ca.pem")
	err := os.WriteFile(invalidCAFile, []byte("not a valid PEM certificate"), 0o600)
	require.NoError(t, err)

	tlsConfig := tlsOptions{
		Enabled: true,
		CAFile:  invalidCAFile,
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
}

func TestTLSConfig_BuildTLSConfig_InvalidClientCert(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  testClientKey,
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to load client certificate/key pair")
}

func TestTLSConfig_BuildTLSConfig_InvalidClientKey(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:  true,
		CertFile: testClientCert,
		KeyFile:  "/nonexistent/key.pem",
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to load client certificate/key pair")
}

func TestTLSConfig_BuildTLSConfig_CompleteConfiguration(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:            true,
		InsecureSkipVerify: false,
		CAFile:             testCACertFile,
		CertFile:           testClientCert,
		KeyFile:            testClientKey,
		ServerName:         "clickhouse.example.com",
	}

	result, err := tlsConfig.build()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.False(t, result.InsecureSkipVerify)
	assert.NotNil(t, result.RootCAs)
	assert.Len(t, result.Certificates, 1)
	assert.Equal(t, "clickhouse.example.com", result.ServerName)
}

// TLS Config Parsing Tests (from config sources)

func TestParseConfig_TLS_JSON(t *testing.T) {
	t.Parallel()

	t.Run("TLS enabled in JSON", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"tls": map[string]any{
					"enabled": true,
				},
			}),
		}

		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
	})

	t.Run("TLS with all options in JSON", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"tls": map[string]any{
					"enabled":            true,
					"insecureSkipVerify": true,
					"caFile":             testCACertFile,
					"certFile":           testClientCert,
					"keyFile":            testClientKey,
					"serverName":         "clickhouse.example.com",
				},
			}),
		}

		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
		assert.True(t, cfg.TLS.InsecureSkipVerify)
		assert.Equal(t, testCACertFile, cfg.TLS.CAFile)
		assert.Equal(t, testClientCert, cfg.TLS.CertFile)
		assert.Equal(t, testClientKey, cfg.TLS.KeyFile)
		assert.Equal(t, "clickhouse.example.com", cfg.TLS.ServerName)
	})
}

func TestParseConfig_TLS_URL(t *testing.T) {
	t.Parallel()

	t.Run("TLS enabled via URL", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			ConfigArgument: "localhost:9440?tlsEnabled=true",
		}

		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
	})

	t.Run("TLS with insecure skip verify via URL", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			ConfigArgument: "localhost:9440?tlsEnabled=true&tlsInsecureSkipVerify=true",
		}

		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
		assert.True(t, cfg.TLS.InsecureSkipVerify)
	})
}

func TestParseConfig_TLS_Environment(t *testing.T) {
	t.Parallel()

	t.Run("TLS enabled via ENV", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			Environment: map[string]string{"K6_CLICKHOUSE_TLS_ENABLED": "true"},
		}
		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
	})

	t.Run("TLS with all options via ENV", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			Environment: map[string]string{
				"K6_CLICKHOUSE_TLS_ENABLED":              "true",
				"K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY": "false",
				"K6_CLICKHOUSE_TLS_CA_FILE":              testCACertFile,
				"K6_CLICKHOUSE_TLS_CERT_FILE":            testClientCert,
				"K6_CLICKHOUSE_TLS_KEY_FILE":             testClientKey,
				"K6_CLICKHOUSE_TLS_SERVER_NAME":          "clickhouse.local",
			},
		}
		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
		assert.False(t, cfg.TLS.InsecureSkipVerify)
		assert.Equal(t, testCACertFile, cfg.TLS.CAFile)
		assert.Equal(t, testClientCert, cfg.TLS.CertFile)
		assert.Equal(t, testClientKey, cfg.TLS.KeyFile)
		assert.Equal(t, "clickhouse.local", cfg.TLS.ServerName)
	})
}

func TestParseConfig_TLS_Priority(t *testing.T) {
	t.Parallel()

	t.Run("ENV overrides JSON", func(t *testing.T) {
		t.Parallel()

		params := output.Params{
			JSONConfig: mustMarshalJSON(map[string]any{
				"tls": map[string]any{
					"enabled":    false,
					"serverName": "json.example.com",
				},
			}),
			Environment: map[string]string{
				"K6_CLICKHOUSE_TLS_ENABLED":     "true",
				"K6_CLICKHOUSE_TLS_SERVER_NAME": "env.example.com",
			},
		}

		cfg, err := parseConfig(params)
		require.NoError(t, err)
		assert.True(t, cfg.TLS.Enabled)
		assert.Equal(t, "env.example.com", cfg.TLS.ServerName)
	})
}

func TestTLSConfig_BuildTLSConfig_CertWithoutKey(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled:  true,
		CertFile: testClientCert,
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "TLS client certificate and key must be specified together")
}

func TestTLSConfig_BuildTLSConfig_KeyWithoutCert(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: true,
		KeyFile: testClientKey,
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "TLS client certificate and key must be specified together")
}

func TestTLSConfig_BuildTLSConfig_CADirectory(t *testing.T) {
	t.Parallel()

	tlsConfig := tlsOptions{
		Enabled: true,
		CAFile:  t.TempDir(),
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to read CA certificate file")
}

func TestTLSConfig_BuildTLSConfig_UnreadableCAFile(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("running as root: file permissions do not restrict reads")
	}

	file := filepath.Join(t.TempDir(), "unreadable-ca.pem")
	require.NoError(t, os.WriteFile(file, []byte("test content"), 0o000))

	tlsConfig := tlsOptions{
		Enabled: true,
		CAFile:  file,
	}

	result, err := tlsConfig.build()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to read CA certificate file")
}
