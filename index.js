const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const cron = require('node-cron');

const app = express();
app.use(cors());

const config = {
  server: 'lhf-mov.database.windows.net',
  database: 'lhf-movement',
  user: 'lhfreportsuser',
  password: 'LHFr3@d0nly',
  options: { encrypt: true }
};

let cachedData = [];
let lastUpdated = null;

async function refreshData() {
  console.log('Refreshing data...');
  try {
    const pool = await sql.connect(config);
    const result = await pool.request().query(`
      SELECT 
        DATEADD(DAY, -(DATEPART(WEEKDAY, oh.DispatchDate)-1), CAST(oh.DispatchDate AS DATE)) AS WeekStart,
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
        DATEADD(DAY, -(DATEPART(WEEKDAY, oh.DispatchDate)-1), CAST(oh.DispatchDate AS DATE)),
        oh.StoreName,
        od.Upc,
        od.Name
      ORDER BY WeekStart DESC, oh.StoreName, od.Upc
    `);
    cachedData = result.recordset;
    lastUpdated = new Date();
    console.log(`Data refreshed at ${lastUpdated} — ${cachedData.length} rows`);
  } catch (err) {
    console.error('Refresh failed:', err.message);
  }
}

// Refresh every Sunday at 1am
cron.schedule('0 1 * * 0', refreshData);

// Endpoint — instant response from cache
app.get('/sales', (req, res) => {
  res.json({
    lastUpdated,
    data: cachedData
  });
});

// Status check
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
