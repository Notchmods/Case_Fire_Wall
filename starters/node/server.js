// Obsidio starter: Node.js + Express. NAIVE ON PURPOSE.
//
// This implements the contract correctly, so it builds, runs, and passes the
// health check. But it is a single process on one event loop, so it WILL fail
// the load test: the /risk computation blocks the loop and clogs /price behind
// it. Making it resilient (use both CPUs, keep the fast path fast) is YOUR job.
//
// There is deliberately NO resilience machinery here: no clustering, no worker
// threads, no caching, no queueing. That is the part you build.
//
// Core-count note: your container is capped at 2 CPUs but can SEE all host
// cores. If you reach for the cluster module or worker_threads, size the pool
// explicitly (e.g. 2), do not trust the visible core count.

const express = require('express');
const crypto = require('crypto');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 8080;

// Fixed data (same symbol set the grader uses).
const PRICES = {
  AAPL: 187.42, GOOG: 141.80, MSFT: 412.30, AMZN: 178.10,
  NVDA: 120.15, META: 502.60, TSLA: 244.70, JPM: 198.35,
};
const SERIES = {};
for (const [sym, base] of Object.entries(PRICES)) {
  SERIES[sym] = Array.from({ length: 500 }, (_, i) => base * (1 + Math.sin(i) / 50));
}

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// CHEAP (weight 1): simple lookup.
app.get('/price', (req, res) => {
  const s = req.query.symbol;
  if (!(s in PRICES)) return res.status(404).json({ error: 'unknown symbol' });
  res.json({ symbol: s, price: PRICES[s] });
});

// MEDIUM (weight 3): aggregate the series on every request.
app.get('/stats', (req, res) => {
  const s = req.query.symbol;
  const series = SERIES[s];
  if (!series) return res.status(404).json({ error: 'unknown symbol' });
  const n = series.length;
  const mean = series.reduce((a, b) => a + b, 0) / n;
  const variance = series.reduce((a, b) => a + (b - mean) ** 2, 0) / n;
  res.json({
    symbol: s, mean,
    min: Math.min(...series), max: Math.max(...series),
    stddev: Math.sqrt(variance),
  });
});

// HEAVY (weight 10): 50000 iterations of SHA-256 over the seed. Uncacheable.
app.get('/risk', (req, res) => {
  const seed = req.query.seed || 'none';
  let h = String(seed);
  for (let i = 0; i < 50000; i++) {
    h = crypto.createHash('sha256').update(h).digest('hex');
  }
  res.json({ seed: String(seed), risk_hash: h });
});

// OPTIONAL, only for the persistence bonus: record a price update.
// The naive version keeps it in memory, so it does NOT survive a restart and
// earns no bonus. Add real persistence (and pay its cost) to claim it.
app.post('/price', (req, res) => {
  const { symbol, price } = req.body || {};
  if (!symbol || typeof price !== 'number') {
    return res.status(400).json({ error: 'symbol and numeric price required' });
  }
  PRICES[symbol] = price;
  res.json({ symbol, price });
});

app.listen(PORT, () => console.log(`naive starter listening on :${PORT}`));
