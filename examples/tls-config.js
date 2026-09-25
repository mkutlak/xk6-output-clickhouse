import http from 'k6/http';
import { check, sleep } from 'k6';

// TLS/mTLS configuration example for xk6-output-clickhouse.
// TLS is configured via --out URL params, JSON config, or environment variables,
// not inside the test script. Example:
//   ./bin/k6 run --out "xk6-clickhouse=clickhouse.example.com:9440?tlsEnabled=true&tlsCAFile=/path/ca.pem" examples/tls-config.js
// See docs/configuration.md#tls-options for all TLS options.

export const options = {
    vus: 10,
    duration: '30s',
};

export default function () {
    const res = http.get('https://quickpizza.grafana.com/api/quotes');

    check(res, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    });

    sleep(1);
}
