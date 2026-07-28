const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const cron = require('node-cron');
 
const app = express();
app.use(cors());
 
// How far back to pull, and how big each chunk is. 56 weeks in 28-day slices
// = 14 queries instead of one giant one.
const WEEKS_BACK = Number(process.env.WEEKS_BACK || 56);
const CHUNK_DAYS = Number(process.env.CHUNK_DAYS || 28);
 
const config = {
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: { encrypt: true },
  connectionTimeout: 30000,
  requestTimeout: 120000, // per chunk, not for the whole pull
  pool: { max: 4, min: 0, idleTimeoutMillis: 30000 }
};
 
let cachedData = [];
let lastUpdated = null;
let lastError = null;
let refreshing = false;
let progress = null; // { done, total } while a refresh is running
let tokenWarned = false;
 
// ---------------------------------------------------------------- pool
 
let pool;
async function getPool() {
  if (!pool) {
    const p = new sql.ConnectionPool(config);
    p.on('error', (err) => {
      console.error('Pool error:', err.message);
      pool = null;
    });
    try {
      pool = await p.connect();
      console.log('DB pool connected.');
    } catch (err) {
      pool = null;
      throw err;
    }
  }
  return pool;
}
 
// ---------------------------------------------------------------- query
 
// No ORDER BY — sorting a large aggregate server-side is expensive and we can
// do it in memory once, for free. Date range is parameterized and compared
// against the raw column so an index on DispatchDate can still be used.
const CHUNK_SQL = `
  SELECT
    CAST(oh.DispatchDate AS DATE) AS SaleDate,
    oh.StoreName,
    od.Brand,
    od.Upc AS SKU,
    od.Name AS ProductName,
    SUM(od.Quantity) AS TotalUnits,
    SUM(od.Price * od.Quantity) AS TotalRevenue
  FROM OrderHeaders oh
  INNER JOIN OrderDetails od ON oh.Id = od.OrderId
  WHERE oh.DispatchDate >= @from
    AND oh.DispatchDate <  @to
  GROUP BY
    CAST(oh.DispatchDate AS DATE),
    oh.StoreName,
    od.Brand,
    od.Upc,
    od.Name
`;
 
function buildWindows() {
  const now = new Date();
  const start = new Date(now.getTime() - WEEKS_BACK * 7 * 86400000);
  const windows = [];
  let cursor = start;
  while (cursor < now) {
    const next = new Date(Math.min(cursor.getTime() + CHUNK_DAYS * 86400000, now.getTime()));
    windows.push({ from: cursor, to: next });
    cursor = next;
  }
  return windows;
}
 
async function fetchChunk(p, win, attempt = 1) {
  try {
    const result = await p
      .request()
      .input('from', sql.DateTime2, win.from)
      .input('to', sql.DateTime2, win.to)
      .query(CHUNK_SQL);
    return result.recordset;
  } catch (err) {
    if (attempt < 2) {
      console.warn(`Chunk ${win.from.toISOString().slice(0, 10)} failed (${err.message}) — retrying once.`);
      return fetchChunk(p, win, attempt + 1);
    }
    throw err;
  }
}
 
async function refreshData() {
  if (refreshing) {
    console.log('Refresh already in progress — skipping.');
    return;
  }
  refreshing = true;
 
  const startedAt = Date.now();
  const windows = buildWindows();
  progress = { done: 0, total: windows.length };
  console.log(`Refreshing data — ${windows.length} chunks of ${CHUNK_DAYS} days...`);
 
  const rows = [];
  try {
    const p = await getPool();
 
    for (const win of windows) {
      const t = Date.now();
      const chunk = await fetchChunk(p, win);
      rows.push(...chunk);
      progress.done += 1;
      console.log(
        `  [${progress.done}/${windows.length}] ${win.from.toISOString().slice(0, 10)} → ` +
        `${win.to.toISOString().slice(0, 10)}: ${chunk.length} rows in ` +
        `${((Date.now() - t) / 1000).toFixed(1)}s`
      );
    }
 
    rows.sort((a, b) =>
      (b.SaleDate > a.SaleDate) - (b.SaleDate < a.SaleDate) ||
      String(a.StoreName).localeCompare(String(b.StoreName)) ||
      String(a.SKU).localeCompare(String(b.SKU))
    );
 
    // Swap in atomically only on full success — a partial pull never
    // replaces good data.
    cachedData = rows;
    lastUpdated = new Date();
    lastError = null;
    console.log(
      `Data refreshed — ${rows.length} rows in ${((Date.now() - startedAt) / 1000).toFixed(1)}s`
    );
  } catch (err) {
    lastError = {
      message: err.message,
      code: err.code,
      chunksCompleted: progress.done,
      chunksTotal: progress.total,
      at: new Date().toISOString()
    };
    console.error(
      `Refresh failed after ${progress.done}/${progress.total} chunks:`,
      err.code || '',
      err.message
    );
    // Rebuild the pool next time in case the connection is the problem.
    pool = null;
  } finally {
    refreshing = false;
    progress = null;
  }
}
 
// Retry on startup failure with backoff, so a cold DB doesn't leave the
// cache empty until 1am.
async function refreshWithRetry(attempt = 1) {
  await refreshData();
  if (!lastUpdated && attempt < 4) {
    const wait = attempt * 60000;
    console.log(`No data yet — retrying in ${wait / 1000}s (attempt ${attempt + 1}/4).`);
    setTimeout(() => refreshWithRetry(attempt + 1), wait);
  }
}
 
cron.schedule('0 1 * * *', refreshData, { timezone: 'America/Puerto_Rico' });
 
// ---------------------------------------------------------------- routes
 
function requireToken(req, res, next) {
  const expected = process.env.API_TOKEN;
  if (!expected) {
    if (!tokenWarned) {
      console.warn('API_TOKEN not set — /sales is unauthenticated. Set it in Railway.');
      tokenWarned = true;
    }
    return next();
  }
  const provided = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (provided !== expected) return res.status(401).json({ error: 'Unauthorized' });
  next();
}
 
// Always 200, never touches the DB. Point the platform health check here.
app.get('/health', (req, res) => res.status(200).send('ok'));
 
app.get('/status', (req, res) => {
  res.json({
    status: cachedData.length > 0 ? 'ready' : refreshing ? 'loading' : 'empty',
    lastUpdated,
    totalRows: cachedData.length,
    refreshing,
    progress,
    lastError
  });
});
 
// Filters applied in memory against the single cached pull, so they cost
// nothing at the database.
app.get('/sales', requireToken, (req, res) => {
  if (!lastUpdated) {
    return res.status(503).json({
      error: 'Cache not populated yet',
      refreshing,
      progress,
      lastError: lastError && lastError.message
    });
  }
 
  const { brand, store, since, limit } = req.query;
  let data = cachedData;
 
  if (brand) {
    const b = String(brand).toLowerCase();
    data = data.filter((r) => String(r.Brand || '').toLowerCase() === b);
  }
  if (store) {
    const s = String(store).toLowerCase();
    data = data.filter((r) => String(r.StoreName || '').toLowerCase() === s);
  }
  if (since) {
    const d = new Date(since);
    if (!isNaN(d)) data = data.filter((r) => new Date(r.SaleDate) >= d);
  }
  if (limit) {
    const n = parseInt(limit, 10);
    if (n > 0) data = data.slice(0, n);
  }
 
  res.json({ lastUpdated, rows: data.length, data });
});
 
// Distinct brands/stores in the cache — useful for driving a filter UI.
app.get('/meta', (req, res) => {
  res.json({
    lastUpdated,
    brands: [...new Set(cachedData.map((r) => r.Brand).filter(Boolean))].sort(),
    stores: [...new Set(cachedData.map((r) => r.StoreName).filter(Boolean))].sort()
  });
});
 
app.post('/refresh', requireToken, (req, res) => {
  if (refreshing) return res.status(409).json({ error: 'Refresh already in progress' });
  refreshData();
  res.status(202).json({ started: true });
});
 
// ---------------------------------------------------------------- boot
 
const port = process.env.PORT || 3000;
app.listen(port, '0.0.0.0', () => {
  console.log(`API running on port ${port}`);
  refreshWithRetry();
});
 
process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err && err.message);
});
