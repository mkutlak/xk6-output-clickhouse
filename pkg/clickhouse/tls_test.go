package clickhouse

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

// testTLSDir and the certificate/key files below are generated once in
// TestMain (main_test.go) and shared read-only across this file's tests.
var (
	testTLSDir     string
	testCACertFile string
	testClientCert string
	testClientKey  string
)

// newRSAKey generates an RSA key for test certificates.
func newRSAKey() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
}

// writePEMFile PEM-encodes bytes under blockType to <dir>/<filename> and
// returns the resulting path.
func writePEMFile(dir, filename, blockType string, bytes []byte) string {
	path := filepath.Join(dir, filename)
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: bytes}); err != nil {
		panic(err)
	}
	return path
}

// writeSelfSignedCert generates an RSA key, self-signs a certificate from
// tmpl, and PEM-encodes it to <dir>/<filename>.
func writeSelfSignedCert(dir, filename string, tmpl *x509.Certificate) (certFile string, key *rsa.PrivateKey) {
	key = newRSAKey()
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return writePEMFile(dir, filename, "CERTIFICATE", certDER), key
}

// generateCACert creates a self-signed CA certificate file in dir and returns its path.
func generateCACert(dir string) string {
	certFile, _ := writeSelfSignedCert(dir, "ca.pem", &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	})
	return certFile
}

// generateClientCert creates a self-signed client certificate and key file in dir.
func generateClientCert(dir string) (certFile, keyFile string) {
	certFile, key := writeSelfSignedCert(dir, "client-cert.pem", &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"Test Client"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	keyFile = writePEMFile(dir, "client-key.pem", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	return certFile, keyFile
}

func TestTLSOptionsBuild(t *testing.T) {
	t.Parallel()

	unreadableCAFile := filepath.Join(t.TempDir(), "unreadable-ca.pem")
	require.NoError(t, os.WriteFile(unreadableCAFile, []byte("test content"), 0o000))

	invalidCAFile := filepath.Join(t.TempDir(), "invalid-ca.pem")
	require.NoError(t, os.WriteFile(invalidCAFile, []byte("not a valid PEM certificate"), 0o600))

	tests := []struct {
		name     string
		skipRoot bool
		opts     tlsOptions
		wantNil  bool
		wantErr  string
		check    func(t *testing.T, cfg *tls.Config)
	}{
		{
			name:    "disabled",
			opts:    tlsOptions{Enabled: false},
			wantNil: true,
		},
		{
			name: "enabled with system CA pool",
			opts: tlsOptions{Enabled: true},
			check: func(t *testing.T, cfg *tls.Config) {
				assert.False(t, cfg.InsecureSkipVerify)
				assert.NotNil(t, cfg.RootCAs)
				assert.Empty(t, cfg.Certificates)
			},
		},
		{
			name: "custom CA",
			opts: tlsOptions{Enabled: true, CAFile: testCACertFile},
			check: func(t *testing.T, cfg *tls.Config) {
				assert.NotNil(t, cfg.RootCAs)
				assert.False(t, cfg.InsecureSkipVerify)
			},
		},
		{
			name:    "missing CA file",
			opts:    tlsOptions{Enabled: true, CAFile: "/nonexistent/ca.pem"},
			wantErr: "failed to read CA certificate file",
		},
		{
			name:    "invalid CA content",
			opts:    tlsOptions{Enabled: true, CAFile: invalidCAFile},
			wantErr: "failed to parse CA certificate",
		},
		{
			name:    "CA path is a directory",
			opts:    tlsOptions{Enabled: true, CAFile: t.TempDir()},
			wantErr: "failed to read CA certificate file",
		},
		{
			name:     "unreadable CA file",
			skipRoot: true,
			opts:     tlsOptions{Enabled: true, CAFile: unreadableCAFile},
			wantErr:  "failed to read CA certificate file",
		},
		{
			name: "client cert and key",
			opts: tlsOptions{Enabled: true, CertFile: testClientCert, KeyFile: testClientKey},
			check: func(t *testing.T, cfg *tls.Config) {
				assert.Len(t, cfg.Certificates, 1)
				assert.NotNil(t, cfg.RootCAs)
			},
		},
		{
			name:    "cert without key",
			opts:    tlsOptions{Enabled: true, CertFile: testClientCert},
			wantErr: "TLS client certificate and key must be specified together",
		},
		{
			name:    "key without cert",
			opts:    tlsOptions{Enabled: true, KeyFile: testClientKey},
			wantErr: "TLS client certificate and key must be specified together",
		},
		{
			name:    "invalid client cert file",
			opts:    tlsOptions{Enabled: true, CertFile: "/nonexistent/cert.pem", KeyFile: testClientKey},
			wantErr: "failed to load client certificate/key pair",
		},
		{
			name:    "invalid client key file",
			opts:    tlsOptions{Enabled: true, CertFile: testClientCert, KeyFile: "/nonexistent/key.pem"},
			wantErr: "failed to load client certificate/key pair",
		},
		{
			name: "server name",
			opts: tlsOptions{Enabled: true, ServerName: "clickhouse.example.com"},
			check: func(t *testing.T, cfg *tls.Config) {
				assert.Equal(t, "clickhouse.example.com", cfg.ServerName)
			},
		},
		{
			name: "insecure skip verify",
			opts: tlsOptions{Enabled: true, InsecureSkipVerify: true},
			check: func(t *testing.T, cfg *tls.Config) {
				assert.True(t, cfg.InsecureSkipVerify)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.skipRoot && os.Geteuid() == 0 {
				t.Skip("running as root: file permissions do not restrict reads")
			}

			cfg, err := tt.opts.build()

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Nil(t, cfg)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, cfg)
				return
			}
			require.NotNil(t, cfg)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestLogTLSStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		configArg    string
		wantWarnings []string
	}{
		{
			name:         "TLS files set but TLS disabled",
			configArg:    "?" + url.Values{"tlsCAFile": {testCACertFile}}.Encode(),
			wantWarnings: []string{"TLS certificate/CA files are configured but TLS is disabled"},
		},
		{
			name:         "TLS enabled on the plaintext port",
			configArg:    "?tlsEnabled=true",
			wantWarnings: []string{"TLS is enabled but using port 9000"},
		},
		{
			name: "InsecureSkipVerify with CA and serverName configured",
			configArg: "clickhouse.example.com:9440?" + url.Values{
				"tlsEnabled":            {"true"},
				"tlsInsecureSkipVerify": {"true"},
				"tlsCAFile":             {testCACertFile},
				"tlsServerName":         {"clickhouse.example.com"},
			}.Encode(),
			wantWarnings: []string{
				"Certificate verification is DISABLED",
				"the configured CA file and serverName are ignored",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logger, hook := test.NewNullLogger()
			_, err := New(output.Params{Logger: logger, ConfigArgument: tt.configArg})
			require.NoError(t, err)

			var warnings []string
			for _, e := range hook.AllEntries() {
				if e.Level == logrus.WarnLevel {
					warnings = append(warnings, e.Message)
				}
			}

			for _, want := range tt.wantWarnings {
				found := false
				for _, w := range warnings {
					if strings.Contains(w, want) {
						found = true
						break
					}
				}
				assert.True(t, found, "expected a warning containing %q, got: %v", want, warnings)
			}
		})
	}
}
