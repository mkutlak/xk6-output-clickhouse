package clickhouse

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTempCACert creates a temporary CA certificate file for testing
func createTempCACert(t *testing.T) string {
	t.Helper()

	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
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

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	// Write certificate to temp file
	certFile := filepath.Join(t.TempDir(), "ca.pem")
	f, err := os.Create(certFile)
	require.NoError(t, err)
	defer f.Close()

	err = pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, err)

	return certFile
}

// createTempClientCert creates temporary client certificate and key files for testing
func createTempClientCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
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

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	// Write certificate to temp file
	certFile = filepath.Join(t.TempDir(), "client-cert.pem")
	cf, err := os.Create(certFile)
	require.NoError(t, err)
	defer cf.Close()

	err = pem.Encode(cf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, err)

	// Write private key to temp file
	keyFile = filepath.Join(t.TempDir(), "client-key.pem")
	kf, err := os.Create(keyFile)
	require.NoError(t, err)
	defer kf.Close()

	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	err = pem.Encode(kf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privateKeyBytes})
	require.NoError(t, err)

	return certFile, keyFile
}

func TestTLSConfig_BuildTLSConfig_Disabled(t *testing.T) {
	t.Parallel()

	tlsConfig := TLSConfig{
		Enabled: false,
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	assert.Nil(t, result, "BuildTLSConfig should return nil when TLS is disabled")
}

func TestTLSConfig_BuildTLSConfig_EnabledWithSystemCA(t *testing.T) {
	t.Parallel()

	tlsConfig := TLSConfig{
		Enabled: true,
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.False(t, result.InsecureSkipVerify)
	assert.NotNil(t, result.RootCAs, "RootCAs should be set (system pool)")
	assert.Empty(t, result.Certificates, "No client certificates should be loaded")
}

func TestTLSConfig_BuildTLSConfig_WithCustomCA(t *testing.T) {
	t.Parallel()

	caFile := createTempCACert(t)

	tlsConfig := TLSConfig{
		Enabled: true,
		CAFile:  caFile,
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.NotNil(t, result.RootCAs, "RootCAs should include custom CA")
	assert.False(t, result.InsecureSkipVerify)
}

func TestTLSConfig_BuildTLSConfig_WithClientCertificate(t *testing.T) {
	t.Parallel()

	certFile, keyFile := createTempClientCert(t)

	tlsConfig := TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Len(t, result.Certificates, 1, "Client certificate should be loaded")
	assert.NotNil(t, result.RootCAs)
}

func TestTLSConfig_BuildTLSConfig_InsecureSkipVerify(t *testing.T) {
	t.Parallel()

	tlsConfig := TLSConfig{
		Enabled:            true,
		InsecureSkipVerify: true,
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.InsecureSkipVerify, "InsecureSkipVerify should be true")
}

func TestTLSConfig_BuildTLSConfig_WithServerName(t *testing.T) {
	t.Parallel()

	tlsConfig := TLSConfig{
		Enabled:    true,
		ServerName: "clickhouse.example.com",
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "clickhouse.example.com", result.ServerName, "ServerName should be set for SNI")
}

func TestTLSConfig_BuildTLSConfig_InvalidCAFile(t *testing.T) {
	t.Parallel()

	tlsConfig := TLSConfig{
		Enabled: true,
		CAFile:  "/nonexistent/ca.pem",
	}

	result, err := tlsConfig.BuildTLSConfig()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to read CA certificate file")
}

func TestTLSConfig_BuildTLSConfig_InvalidCAContent(t *testing.T) {
	t.Parallel()

	// Create a file with invalid PEM content
	invalidCAFile := filepath.Join(t.TempDir(), "invalid-ca.pem")
	err := os.WriteFile(invalidCAFile, []byte("not a valid PEM certificate"), 0600)
	require.NoError(t, err)

	tlsConfig := TLSConfig{
		Enabled: true,
		CAFile:  invalidCAFile,
	}

	result, err := tlsConfig.BuildTLSConfig()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
}

func TestTLSConfig_BuildTLSConfig_InvalidClientCert(t *testing.T) {
	t.Parallel()

	_, keyFile := createTempClientCert(t)

	tlsConfig := TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  keyFile,
	}

	result, err := tlsConfig.BuildTLSConfig()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to load client certificate/key pair")
}

func TestTLSConfig_BuildTLSConfig_InvalidClientKey(t *testing.T) {
	t.Parallel()

	certFile, _ := createTempClientCert(t)

	tlsConfig := TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  "/nonexistent/key.pem",
	}

	result, err := tlsConfig.BuildTLSConfig()
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to load client certificate/key pair")
}

func TestTLSConfig_BuildTLSConfig_CompleteConfiguration(t *testing.T) {
	t.Parallel()

	caFile := createTempCACert(t)
	certFile, keyFile := createTempClientCert(t)

	tlsConfig := TLSConfig{
		Enabled:            true,
		InsecureSkipVerify: false,
		CAFile:             caFile,
		CertFile:           certFile,
		KeyFile:            keyFile,
		ServerName:         "clickhouse.example.com",
	}

	result, err := tlsConfig.BuildTLSConfig()
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.False(t, result.InsecureSkipVerify)
	assert.NotNil(t, result.RootCAs)
	assert.Len(t, result.Certificates, 1)
	assert.Equal(t, "clickhouse.example.com", result.ServerName)
}

func TestValidateFileReadable(t *testing.T) {
	t.Parallel()

	t.Run("empty path", func(t *testing.T) {
		t.Parallel()

		err := validateFileReadable("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file path is empty")
	})

	t.Run("nonexistent file", func(t *testing.T) {
		t.Parallel()

		err := validateFileReadable("/nonexistent/file.txt")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "file does not exist")
	})

	t.Run("directory instead of file", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		err := validateFileReadable(dir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "path is a directory")
	})

	t.Run("valid readable file", func(t *testing.T) {
		t.Parallel()

		file := filepath.Join(t.TempDir(), "test.txt")
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)

		err = validateFileReadable(file)
		assert.NoError(t, err)
	})

	t.Run("unreadable file", func(t *testing.T) {
		t.Parallel()

		file := filepath.Join(t.TempDir(), "unreadable.txt")
		err := os.WriteFile(file, []byte("test content"), 0000)
		require.NoError(t, err)

		err = validateFileReadable(file)
		// Note: This test may behave differently depending on OS and permissions
		// On some systems (like when running as root), the file may still be readable
		if err != nil {
			assert.Contains(t, err.Error(), "file is not readable")
		}
	})
}

func TestConfig_Validate_TLSConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("TLS disabled passes validation", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.TLS.Enabled = false

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("TLS enabled without files passes validation", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.TLS.Enabled = true

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("TLS with valid CA file passes validation", func(t *testing.T) {
		t.Parallel()

		caFile := createTempCACert(t)

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.CAFile = caFile

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("TLS with invalid CA file fails validation", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.CAFile = "/nonexistent/ca.pem"

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TLS CA file validation failed")
	})

	t.Run("TLS with valid client certificate passes validation", func(t *testing.T) {
		t.Parallel()

		certFile, keyFile := createTempClientCert(t)

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.CertFile = certFile
		cfg.TLS.KeyFile = keyFile

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("TLS with cert but no key fails validation", func(t *testing.T) {
		t.Parallel()

		certFile, _ := createTempClientCert(t)

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.CertFile = certFile

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TLS client certificate and key must be specified together")
	})

	t.Run("TLS with key but no cert fails validation", func(t *testing.T) {
		t.Parallel()

		_, keyFile := createTempClientCert(t)

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.KeyFile = keyFile

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TLS client certificate and key must be specified together")
	})

	t.Run("TLS with invalid cert file fails validation", func(t *testing.T) {
		t.Parallel()

		cfg := NewConfig()
		cfg.TLS.Enabled = true
		cfg.TLS.CertFile = "/nonexistent/cert.pem"
		cfg.TLS.KeyFile = "/nonexistent/key.pem"

		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "TLS client certificate file validation failed")
	})
}
