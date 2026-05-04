const express = require('express');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());

const config = {
  server: 'lhf-mov.database.windows.net',
  database: 'lhf-movement',
  user: 'lhfreportsuser',
  password: 'LHFr3@d0nly',
  options: { encrypt: true }
};

app.get('/sales', async (req, res) => {
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
    res.json(result.recordset);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(process.env.PORT || 3000, () => console.log('API running'));
