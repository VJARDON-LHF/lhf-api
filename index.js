const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const cron = require('node-cron');
 
const app = express();
app.use(cors());
 
const config = {
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: { encrypt: true },
  connectionTimeout: 30000,
  requestTimeout: 300000 // 5 minutes
};
 
// --- Tuning knobs ---
const RECENT_DAYS = 3;   // rolling window refreshed frequently (cheap query)
const FULL_WEEKS = 15;   // full history window, rebuilt once a day
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 5000; // 5s, 10s, 15s backoff
 
let cachedData = [];
let lastUpdated = null;
let lastFullRefresh = null;
let lastError = null;
let refreshing = false;
let consecutiveFailures = 0;
 
// Build the connection pool once and reuse it.
let pool;
async function getPool() {
  if (!pool) {
    const p = new sql.ConnectionPool(config);
    p.on('error', (err) => {
      console.error('Pool error:', err.message);
      pool = null; // force a rebuild on next call
    });
    try {
      pool = await p.connect();
    } catch (err) {
      pool = null;
      throw err;
    }
  }
  return pool;
}
 
// Shared query body. WITH (NOLOCK) + READ UNCOMMITTED so a long-running
// report never blocks on (or gets blocked by) concurrent order writes.
// Trade-off: can read uncommitted/in-flight rows, which is fine for a
// dashboard cache that isn't used for financial reconciliation.
async function runSalesQuery(sinceSql) {
  const p = await getPool();
  const result = await p.request().query(`
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT
      CAST(oh.DispatchDate AS DATE) AS SaleDate,
      oh.StoreName,
      od.Brand,
      od.Upc AS SKU,
      od.Name AS ProductName,
      SUM(od.Quantity) AS TotalUnits,
      SUM(od.Price * od.Quantity) AS TotalRevenue
    FROM OrderHeaders oh WITH (NOLOCK)
    INNER JOIN OrderDetails od WITH (NOLOCK) ON oh.Id = od.OrderId
    WHERE oh.DispatchDate >= ${sinceSql}
    GROUP BY
      CAST(oh.DispatchDate AS DATE),
      oh.StoreName,
      od.Brand,
      od.Upc,
      od.Name
    ORDER BY SaleDate DESC, oh.StoreName, od.Upc
  `);
  return result.recordset;
}
 
// Retries a query a few times with backoff before giving up. Forces a
// pool rebuild between attempts in case the connection itself is the problem.
async function withRetry(fn, label) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      console.error(`${label} — attempt ${attempt}/${MAX_RETRIES} failed:`, err.code || '', err.message);
      if (attempt >= MAX_RETRIES) throw err;
      pool = null; // reconnect fresh next try
      await new Promise((r) => setTimeout(r, attempt * RETRY_BASE_MS));
    }
  }
}
 
function dateOnly(d) {
  return new Date(d).toISOString().slice(0, 10);
}
 
// Full rebuild of the whole 15-week window. Runs once a day — this is the
// expensive query, so it doesn't need to run often.
async function fullRefresh() {
  if (refreshing) {
    console.log('Refresh already in progress — skipping full refresh.');
    return;
  }
  refreshing = true;
  const startedAt = Date.now();
  console.log('Starting full refresh...');
  try {
    const rows = await withRetry(
      () => runSalesQuery(`DATEADD(WEEK, -${FULL_WEEKS}, GETDATE())`),
      'Full refresh'
    );
    cachedData = rows;
    lastUpdated = new Date();
    lastFullRefresh = lastUpdated;
    lastError = null;
    consecutiveFailures = 0;
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    const heapMb = Math.round(process.memoryUsage().heapUsed / 1048576);
    console.log(`Full refresh OK — ${cachedData.length} rows in ${secs}s (heap ${heapMb} MB)`);
  } catch (err) {
    consecutiveFailures++;
    lastError = { message: err.message, code: err.code, at: new Date().toISOString(), scope: 'full' };
    console.error(`Full refresh failed after ${MAX_RETRIES} attempts:`, err.code || '', err.message);
    pool = null;
  } finally {
    refreshing = false;
  }
}
 
// Cheap incremental refresh: re-pulls only the last RECENT_DAYS days and
// merges them into cachedData, replacing whatever was there for that
// window. This is what keeps "today"/"this month" numbers current without
// re-scanning 15 weeks of history every few minutes.
async function recentRefresh() {
  if (refreshing) {
    console.log('Refresh already in progress — skipping recent refresh.');
    return;
  }
  if (!lastFullRefresh) {
    // Nothing to merge into yet — let the full refresh populate first.
    return;
  }
  refreshing = true;
  const startedAt = Date.now();
  try {
    const rows = await withRetry(
      () => runSalesQuery(`DATEADD(DAY, -${RECENT_DAYS}, CAST(GETDATE() AS DATE))`),
      'Recent refresh'
    );
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - RECENT_DAYS);
    const cutoffStr = dateOnly(cutoff);
 
    // Keep everything older than the rolling window untouched, drop the
    // stale copies of the recent window, splice in the fresh rows.
    const kept = cachedData.filter((r) => dateOnly(r.SaleDate) < cutoffStr);
    cachedData = [...rows, ...kept];
 
    lastUpdated = new Date();
    lastError = null;
    consecutiveFailures = 0;
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`Recent refresh OK — merged ${rows.length} rows (last ${RECENT_DAYS}d) in ${secs}s`);
  } catch (err) {
    consecutiveFailures++;
    lastError = { message: err.message, code: err.code, at: new Date().toISOString(), scope: 'recent' };
    console.error(`Recent refresh failed after ${MAX_RETRIES} attempts:`, err.code || '', err.message);
  } finally {
    refreshing = false;
  }
}
 
// Full rebuild once a day at 1am PR time; cheap recent-window refresh
// every 15 minutes so today's numbers never drift far.
cron.schedule('0 1 * * *', fullRefresh, { timezone: 'America/Puerto_Rico' });
cron.schedule('*/15 * * * *', recentRefresh, { timezone: 'America/Puerto_Rico' });
 
// Simple bearer-token gate. If API_TOKEN isn't set, the route stays open
// (handy for local dev) but logs a warning so it's not a silent hole.
let tokenWarned = false;
function requireToken(req, res, next) {
  const expected = process.env.API_TOKEN;
  if (!expected) {
    if (!tokenWarned) {
      console.warn('API_TOKEN not set — /sales is unauthenticated.');
      tokenWarned = true;
    }
    return next();
  }
  const provided = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (provided !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}
 
// Endpoint — instant response from cache
app.get('/sales', requireToken, (req, res) => {
  if (!lastUpdated) {
    return res.status(503).json({
      error: 'Cache not populated yet',
      refreshing,
      lastError: lastError && lastError.message
    });
  }
  res.json({ lastUpdated, data: cachedData });
});
 
// Status check (left open — exposes only row count + timestamps)
app.get('/status', (req, res) => {
  res.json({
    status: cachedData.length > 0 ? 'ready' : refreshing ? 'loading' : 'empty',
    lastUpdated,
    lastFullRefresh,
    totalRows: cachedData.length,
    refreshing,
    consecutiveFailures,
    heapMb: Math.round(process.memoryUsage().heapUsed / 1048576),
    lastError
  });
});
 
// Health check — always 200, never touches the DB.
app.get('/health', (req, res) => res.status(200).send('ok'));
 
// Manual refresh trigger. ?full=true forces the full 15-week rebuild;
// otherwise runs the cheap recent-window refresh.
app.post('/refresh', requireToken, (req, res) => {
  if (refreshing) return res.status(409).json({ error: 'Refresh already in progress' });
  if (req.query.full === 'true') {
    fullRefresh();
  } else {
    recentRefresh();
  }
  res.status(202).json({ started: true, full: req.query.full === 'true' });
});
 
// Bind the port first, then load data in the background.
const port = process.env.PORT || 3000;
app.listen(port, '0.0.0.0', () => {
  console.log(`API running on port ${port}`);
  fullRefresh();
});
 
process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err && err.message);
});
