// Obsidio grading script. This is BOTH the tool we grade with and the tool you
// use to test yourself. There is no hidden version.
//
//   Run against your container:  k6 run -e TARGET=http://127.0.0.1:8080 grading.js
//
// It ramps a crowd of virtual users hitting a fixed 60/30/10 mix of the three
// endpoints, checks each tier against its own latency bar, and totals a
// weighted work_score (the leaderboard number).
//
// NOTE: the numbers below (ramp peak, thresholds) are PLACEHOLDERS to be
// finalised on the grading hardware. The version published before the event
// carries the locked values.

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.TARGET || 'http://localhost:8080';

// The fixed set of valid symbols (must match the starter data).
const SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA', 'META', 'TSLA', 'JPM'];
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

// Leaderboard metric: weighted count of correctly completed requests.
const workScore = new Counter('work_score');

export const options = {
  scenarios: {
    siege: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '1m',  target: 50 },    // warm up
        { duration: '2m',  target: 100 },   // climb 
        { duration: '1m',  target: 200 },   // hold at peak 
        { duration: '30s', target: 0 },     // cool down
      ],
    },
  },

  // Per-tier latency bars plus an overall error ceiling. PLACEHOLDERS.
  thresholds: {
    'http_req_duration{tier:price}': ['p(95)<200'],
    'http_req_duration{tier:stats}': ['p(95)<500'],
    'http_req_duration{tier:risk}':  ['p(95)<1500'],
    'http_req_failed':               ['rate<0.01'],
  },
};

export default function () {
  const r = Math.random();
  let res, weight, tier;

  if (r < 0.6) {
    // CHEAP: price lookup. The `name` tag groups all symbols under one metric
    // bucket so k6 does not create a fresh time series per symbol.
    tier = 'price'; weight = 1;
    res = http.get(`${BASE}/price?symbol=${pick(SYMBOLS)}`, { tags: { tier, name: 'price' } });
  } else if (r < 0.9) {
    // MEDIUM: stats aggregation.
    tier = 'stats'; weight = 3;
    res = http.get(`${BASE}/stats?symbol=${pick(SYMBOLS)}`, { tags: { tier, name: 'stats' } });
  } else {
    // HEAVY: risk computation.
    // the `name` tag keeps it as ONE metric bucket instead of one per seed.
    tier = 'risk'; weight = 10;
    res = http.get(`${BASE}/risk?seed=${Math.random()}`, { tags: { tier, name: 'risk' } });
  }

  // Count only requests that actually came back 200. (For stricter anti-cheat,
  // the grader can additionally verify the /risk digest against the seed.)
  const ok = check(res, { 'status is 200': (resp) => resp.status === 200 });
  if (ok) workScore.add(weight);
}
