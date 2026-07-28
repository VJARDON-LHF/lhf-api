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
  connectionTimeout: 30000,  // fail fast if the host is unreachable
  requestTimeout: 300000     // 5 minutes — the aggregate query is slow
};

let cachedData = [];
let lastUpdated = null;
let lastError = null;
let refreshing = false;

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
      pool = null; // never cache a half-open pool
      throw err;
    }
  }
  return pool;
}

async function refreshData() {
  // Startup and cron can both fire; don't stack two 5-minute queries.
  if (refreshing) {
    console.log('Refresh already in progress — skipping.');
    return;
  }
  refreshing = true;

  const startedAt = Date.now();
  console.log('Refreshing data...');
  try {
    const p = await getPool();
    const result = await p.request().query(`
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
      WHERE oh.DispatchDate >= DATEADD(WEEK, -56, GETDATE())
      GROUP BY
        CAST(oh.DispatchDate AS DATE),
        oh.StoreName,
        od.Brand,
        od.Upc,
        od.Name
      ORDER BY SaleDate DESC, oh.StoreName, od.Upc
    `);

    cachedData = result.recordset;
    lastUpdated = new Date();
    lastError = null;
    const secs = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `Data refreshed at ${lastUpdated.toISOString()} — ${cachedData.length} rows in ${secs}s`
    );
  } catch (err) {
    // Keep the previous cache rather than blanking it out on a failed refresh.
    lastError = { message: err.message, code: err.code, at: new Date().toISOString() };
    console.error('Refresh failed:', err.code || '', err.message);
  } finally {
    refreshing = false;
  }
}

// Refresh every day at 1am, pinned to local timezone.
cron.schedule('0 1 * * *', refreshData, { timezone: 'America/Puerto_Rico' });

// Simple bearer-token gate. If API_TOKEN isn't set, the route stays open
// (handy for local dev) but logs a warning so it's not a silent hole.
function requireToken(req, res, next) {
  const expected = process.env.API_TOKEN;
  if (!expected) {
    console.warn('API_TOKEN not set — /sales is unauthenticated');
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
    status: cachedData.length > 0 ? 'ready' : (refreshing ? 'loading' : 'empty'),
    lastUpdated,
    totalRows: cachedData.length,
    refreshing,
    lastError
  });
});

// Health check — always 200, never touches the DB. Point Railway here.
app.get('/health', (req, res) => res.status(200).send('ok'));

// Manual refresh trigger (token-gated), so you don't have to redeploy to retry.
app.post('/refresh', requireToken, (req, res) => {
  if (refreshing) return res.status(409).json({ error: 'Refresh already in progress' });
  refreshData();
  res.status(202).json({ started: true });
});

// Bind the port FIRST, then load data in the background. Otherwise the
// platform health check hits a dead port for up to 5 minutes and restarts us.
const port = process.env.PORT || 3000;
app.listen(port, '0.0.0.0', () => {
  console.log(`API running on port ${port}`);
  refreshData();
});

// Don't let an unhandled rejection take the process down silently.
process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err && err.message);
});
