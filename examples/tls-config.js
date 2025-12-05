import http from 'k6/http';
import { check, sleep } from 'k6';

// TLS Configuration Examples for xk6-output-clickhouse
//
// This example demonstrates various TLS/SSL configuration options
// for connecting to ClickHouse securely.

// Example 1: Basic TLS with System CA Pool
// The simplest configuration - uses your system's trusted certificates
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            table: "samples",
            tls: {
                enabled: true
            }
        }
    },
    vus: 10,
    duration: '30s'
};

// Example 2: TLS with Custom CA Certificate
// Uncomment this configuration if using a self-signed certificate or private CA
/*
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            table: "samples",
            tls: {
                enabled: true,
                caFile: "/path/to/ca.pem"
            }
        }
    },
    vus: 10,
    duration: '30s'
};
*/

// Example 3: Mutual TLS (mTLS) with Client Certificates
// For environments requiring client certificate authentication
/*
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            table: "samples",
            tls: {
                enabled: true,
                caFile: "/path/to/ca.pem",
                certFile: "/path/to/client-cert.pem",
                keyFile: "/path/to/client-key.pem"
            }
        }
    },
    vus: 10,
    duration: '30s'
};
*/

// Example 4: TLS with Server Name Indication (SNI)
// For servers with multiple domains on the same IP
/*
export const options = {
    ext: {
        clickhouse: {
            addr: "192.168.1.100:9440",
            database: "k6",
            table: "samples",
            tls: {
                enabled: true,
                serverName: "clickhouse.example.com"
            }
        }
    },
    vus: 10,
    duration: '30s'
};
*/

// Example 5: TLS with InsecureSkipVerify (TESTING ONLY)
// WARNING: This disables certificate verification and should NEVER be used in production
/*
export const options = {
    ext: {
        clickhouse: {
            addr: "clickhouse.example.com:9440",
            database: "k6",
            table: "samples",
            tls: {
                enabled: true,
                insecureSkipVerify: true  // INSECURE - testing only!
            }
        }
    },
    vus: 10,
    duration: '30s'
};
*/

// Test scenario
export default function () {
    const res = http.get('https://httpbin.test.k6.io/get');

    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    });

    sleep(1);
}

// Alternative: Using Environment Variables
// You can also configure TLS using environment variables:
//
// export K6_CLICKHOUSE_ADDR=clickhouse.example.com:9440
// export K6_CLICKHOUSE_TLS_ENABLED=true
// export K6_CLICKHOUSE_TLS_CA_FILE=/path/to/ca.pem
// export K6_CLICKHOUSE_TLS_CERT_FILE=/path/to/client-cert.pem
// export K6_CLICKHOUSE_TLS_KEY_FILE=/path/to/client-key.pem
// export K6_CLICKHOUSE_TLS_SERVER_NAME=clickhouse.example.com
// ./k6 run --out clickhouse script.js

// Alternative: Using Command Line Parameters
// ./k6 run --out clickhouse=clickhouse.example.com:9440?tlsEnabled=true&tlsCAFile=/path/to/ca.pem script.js
