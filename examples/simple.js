import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    scenarios: {
         clickhouse: {
             executor: 'constant-vus',
             vus: 5,
             duration: '10s',
         },
    }
};

export default function () {
    http.get('https://test.k6.io');
    sleep(1);
}
