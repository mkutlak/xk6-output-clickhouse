import http from 'k6/http';
import { check, sleep } from 'k6';

// =============================================================================
// TLS / mTLS configuration for xk6-output-clickhouse
// =============================================================================
//
// IMPORTANT: TLS for this output is NOT configured from inside the test script.
// k6 ignores `export const options.ext` for output extensions (it is tagged
// `ignored` and never reaches the extension), so a `tls` block placed there has
// NO effect — the connection silently falls back to plaintext. Configure TLS
// using one of the three supported mechanisms below instead. See
// docs/configuration.md for the full option reference.
//
// This file is only the k6 *workload*. Pick a mechanism below to enable TLS,
// then run it, e.g.:
//
//   # 1) --config JSON file (see examples/tls-config.json in this directory)
//   ./bin/k6 run --config examples/tls-config.json \
//       --out xk6-clickhouse examples/tls-config.js
//
//   # 2) --out URL parameters (note the quotes — & is shell-significant)
//   ./bin/k6 run \
//       --out "xk6-clickhouse=clickhouse.example.com:9440?tlsEnabled=true&tlsCAFile=/path/to/ca.pem" \
//       examples/tls-config.js
//
//   # 3) environment variables
//   export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
//   export K6_CLICKHOUSE_TLS_ENABLED=true
//   export K6_CLICKHOUSE_TLS_CA_FILE=/path/to/ca.pem
//   ./bin/k6 run --out xk6-clickhouse examples/tls-config.js
//
// -----------------------------------------------------------------------------
// Recipes (env-var form shown; the same keys exist as --out params and JSON)
// -----------------------------------------------------------------------------
//
// Basic TLS with the system CA pool:
//   K6_CLICKHOUSE_TLS_ENABLED=true
//   --out param:  ?tlsEnabled=true
//
// TLS with a custom / private CA certificate:
//   K6_CLICKHOUSE_TLS_ENABLED=true
//   K6_CLICKHOUSE_TLS_CA_FILE=/path/to/ca.pem
//   --out params: ?tlsEnabled=true&tlsCAFile=/path/to/ca.pem
//
// Mutual TLS (mTLS) with client certificates (certFile + keyFile must be paired):
//   K6_CLICKHOUSE_TLS_ENABLED=true
//   K6_CLICKHOUSE_TLS_CA_FILE=/path/to/ca.pem
//   K6_CLICKHOUSE_TLS_CERT_FILE=/path/to/client-cert.pem
//   K6_CLICKHOUSE_TLS_KEY_FILE=/path/to/client-key.pem
//
// Server Name Indication (SNI) — when the cert CN/SAN differs from the dial host:
//   K6_CLICKHOUSE_TLS_ENABLED=true
//   K6_CLICKHOUSE_TLS_SERVER_NAME=clickhouse.example.com
//
// InsecureSkipVerify (TESTING ONLY — disables certificate verification):
//   K6_CLICKHOUSE_TLS_ENABLED=true
//   K6_CLICKHOUSE_TLS_INSECURE_SKIP_VERIFY=true
//
// Note: TLS uses ClickHouse's secure native port 9440 (not 9000).
// =============================================================================

export const options = {
    vus: 10,
    duration: '30s',
};

// Test scenario — the actual workload whose metrics are streamed to ClickHouse.
export default function () {
    const res = http.get('https://httpbin.test.k6.io/get');

    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    });

    sleep(1);
}
