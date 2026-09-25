package clickhouse

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestMain applies to the whole package: it prepares shared TLS fixtures
// (see tls_test.go) and, in -short runs, checks for goroutine leaks across
// all tests in this package.
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
