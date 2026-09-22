import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DuckDBInstance } from '@duckdb/node-api';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.PORT || 5544);

// Point this at your built transparency.duckdb (the one build_benchmarks.py ran against).
const databasePath = path.resolve(process.env.DUCKDB_PATH || path.join(__dirname, 'transparency.duckdb'));

app.use(express.json({ limit: '1mb' }));
app.use(express.static(__dirname));

let instance;
let connection;

async function execute(sql) {
  const reader = await connection.runAndReadAll(sql);
  return reader.getRowObjectsJson();
}

async function initialize() {
  instance = await DuckDBInstance.create(databasePath, {
    threads: String(Math.max(1, Number(process.env.DUCKDB_THREADS || 4)))
  });
  connection = await instance.connect();
  console.log(`DuckDB connected: ${databasePath}`);
}

// ---------------------------------------------------------------------------
// Input sanitizing helpers. Billing codes / types are short tokens
// (letters, digits, dashes) - never free-form SQL. We whitelist the
// character set and length rather than trying to escape quotes.
// ---------------------------------------------------------------------------
function cleanToken(v, maxlen = 24) {
  const s = String(v ?? '').trim().slice(0, maxlen);
  if (/^[A-Za-z0-9\-]+$/.test(s)) return s;
  return null;
}

function sqlLit(s) {
  return `'${s.replace(/'/g, "''")}'`;
}

app.get('/api/health', async (_req, res) => {
  try {
    const rows = await execute("SELECT version() AS version, (SELECT count(*) FROM benchmarks) AS benchmark_rows");
    res.json({ ok: true, database: path.basename(databasePath), version: rows[0]?.version, benchmarkRows: rows[0]?.benchmark_rows });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

// Distinct billing code types actually present, for the dropdown.
app.get('/api/code-types', async (_req, res) => {
  try {
    const rows = await execute(`
      SELECT billing_code_type, COUNT(*) AS n
      FROM benchmarks_code_stats
      GROUP BY 1 ORDER BY n DESC
    `);
    res.json(rows.map(r => r.billing_code_type));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Overall summary for one code, e.g. ?code=99213&type=CPT
app.get('/api/benchmark/summary', async (req, res) => {
  const code = cleanToken(req.query.code);
  const type = cleanToken(req.query.type);
  if (!code || !type) return res.status(400).json({ error: 'code and type are required.' });
  try {
    const rows = await execute(`
      SELECT *
      FROM benchmarks_code_stats
      WHERE billing_code = ${sqlLit(code)} AND billing_code_type = ${sqlLit(type)}
    `);
    res.json(rows[0] || null);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Per-state rollup for one code (aggregated across counties), sorted by median rate desc.
app.get('/api/benchmark/states', async (req, res) => {
  const code = cleanToken(req.query.code);
  const type = cleanToken(req.query.type);
  if (!code || !type) return res.status(400).json({ error: 'code and type are required.' });
  try {
    const rows = await execute(`
      SELECT
        provider_state AS state,
        SUM(n_rates) AS n_rates,
        SUM(n_providers) AS n_providers,
        COUNT(DISTINCT county_fips) AS n_counties,
        ROUND(MIN(rate_min), 2) AS rate_min,
        ROUND(SUM(rate_median * n_rates) / SUM(n_rates), 2) AS rate_median,
        ROUND(MAX(rate_max), 2) AS rate_max,
        ROUND(SUM(rate_avg * n_rates) / SUM(n_rates), 2) AS rate_avg
      FROM benchmarks_geo_stats
      WHERE billing_code = ${sqlLit(code)} AND billing_code_type = ${sqlLit(type)}
      GROUP BY provider_state
      ORDER BY rate_median DESC
    `);
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Per-county rollup for one code within one state, sorted by median rate desc.
app.get('/api/benchmark/counties', async (req, res) => {
  const code = cleanToken(req.query.code);
  const type = cleanToken(req.query.type);
  const state = cleanToken(req.query.state, 4);
  if (!code || !type || !state) return res.status(400).json({ error: 'code, type and state are required.' });
  try {
    const rows = await execute(`
      SELECT
        county_name, county_fips,
        n_rates, n_providers, n_payers,
        rate_min, rate_p25, rate_median, rate_p75, rate_max, rate_avg
      FROM benchmarks_geo_stats
      WHERE billing_code = ${sqlLit(code)} AND billing_code_type = ${sqlLit(type)}
        AND provider_state = ${sqlLit(state)}
      ORDER BY rate_median DESC
    `);
    res.json(rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'benchmark_dashboard.html')));

initialize()
  .then(() => app.listen(port, '127.0.0.1', () => {
    console.log(`Dashboard: http://localhost:${port}`);
    console.log(`API health: http://localhost:${port}/api/health`);
  }))
  .catch(error => {
    console.error('Failed to start:', error);
    process.exit(1);
  });

async function shutdown() {
  try { connection?.closeSync(); } catch {}
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);