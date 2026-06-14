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
  requestTimeout: 300000 // 5 minutes
};

let cachedData = [];
let lastUpdated = null;

// Build the connection pool once and reuse it.
let pool;
async function getPool() {
  if (!pool) {
    pool = await new sql.ConnectionPool(config).connect();
    pool.on('error', (err) => {
      console.error('Pool error:', err.message);
      pool = null; // force a rebuild on next call
    });
  }
  return pool;
}

async function refreshData() {
  console.log('Refreshing data...');
  try {
    const p = await getPool();
    const result = await p.request().query(`
      SELECT
        CAST(oh.DispatchDate AS DATE) AS SaleDate,
        oh.StoreName,
        od.Upc AS SKU,
        od.Name AS ProductName,
        SUM(od.Quantity) AS TotalUnits,
        SUM(od.Price * od.Quantity) AS TotalRevenue
      FROM OrderHeaders oh
      INNER JOIN OrderDetails od ON oh.Id = od.OrderId
      WHERE od.Brand = 'La Hacienda'
        AND oh.DispatchDate >= DATEADD(WEEK, -52, GETDATE())
      GROUP BY
        CAST(oh.DispatchDate AS DATE),
        oh.StoreName,
        od.Upc,
        od.Name
      ORDER BY SaleDate DESC, oh.StoreName, od.Upc
    `);
    cachedData = result.recordset;
    lastUpdated = new Date();
    console.log(`Data refreshed at ${lastUpdated} — ${cachedData.length} rows`);
  } catch (err) {
    console.error('Refresh failed:', err.message);
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
  res.json({
    lastUpdated,
    data: cachedData
  });
});

// Status check (left open — exposes only row count + timestamp)
app.get('/status', (req, res) => {
  res.json({
    lastUpdated,
    totalRows: cachedData.length,
    status: cachedData.length > 0 ? 'ready' : 'loading'
  });
});

// Load data on startup then start server
refreshData().then(() => {
  app.listen(process.env.PORT || 3000, () => console.log('API running'));
});
