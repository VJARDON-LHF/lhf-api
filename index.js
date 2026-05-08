const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const axios = require('axios');
const qs = require('querystring');

const app = express();
app.use(cors());
app.use(express.json());

// ─── Azure SQL Config ───────────────────────────────────────────────
const dbConfig = {
  server: 'lhf-mov.database.windows.net',
  database: 'lhf-movement',
  user: 'lhfreportsuser',
  password: 'LHFr3@d0nly',
  options: { encrypt: true }
};

// ─── QBO Config ─────────────────────────────────────────────────────
const QBO_CLIENT_ID     = process.env.QBO_CLIENT_ID;
const QBO_CLIENT_SECRET = process.env.QBO_CLIENT_SECRET;
const QBO_REDIRECT_URI  = process.env.QBO_REDIRECT_URI;
const QBO_REALM_ID      = process.env.QBO_REALM_ID;
const QBO_BASE_URL      = 'https://quickbooks.api.intuit.com/v3/company';

// In-memory token store (persists as long as Railway is running)
let qboTokens = {
  access_token: null,
  refresh_token: null,
  expires_at: null
};

// ─── QBO Auth Routes ────────────────────────────────────────────────

// Step 1: Start OAuth flow — visit this URL in your browser once
app.get('/qbo/auth', (req, res) => {
  const scopes = 'com.intuit.quickbooks.accounting';
  const authUrl = `https://appcenter.intuit.com/connect/oauth2?` +
    `client_id=${QBO_CLIENT_ID}` +
    `&redirect_uri=${encodeURIComponent(QBO_REDIRECT_URI)}` +
    `&response_type=code` +
    `&scope=${encodeURIComponent(scopes)}` +
    `&state=randomstate123`;
  res.redirect(authUrl);
});

// Step 2: Intuit redirects here with the auth code
app.get('/qbo/callback', async (req, res) => {
  const { code } = req.query;
  try {
    const response = await axios.post(
      'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
      qs.stringify({
        grant_type: 'authorization_code',
        code,
        redirect_uri: QBO_REDIRECT_URI
      }),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': 'Basic ' + Buffer.from(`${QBO_CLIENT_ID}:${QBO_CLIENT_SECRET}`).toString('base64')
        }
      }
    );
    qboTokens.access_token  = response.data.access_token;
    qboTokens.refresh_token = response.data.refresh_token;
    qboTokens.expires_at    = Date.now() + (response.data.expires_in * 1000);
    res.send('✅ QuickBooks connected! You can close this tab.');
  } catch (err) {
    res.status(500).send('Auth failed: ' + err.message);
  }
});

// Auto-refresh access token when expired
async function getValidToken() {
  if (!qboTokens.refresh_token) throw new Error('Not authenticated. Visit /qbo/auth first.');
  if (Date.now() < qboTokens.expires_at - 60000) return qboTokens.access_token;

  const response = await axios.post(
    'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
    qs.stringify({ grant_type: 'refresh_token', refresh_token: qboTokens.refresh_token }),
    {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Basic ' + Buffer.from(`${QBO_CLIENT_ID}:${QBO_CLIENT_SECRET}`).toString('base64')
      }
    }
  );
  qboTokens.access_token  = response.data.access_token;
  qboTokens.refresh_token = response.data.refresh_token;
  qboTokens.expires_at    = Date.now() + (response.data.expires_in * 1000);
  return qboTokens.access_token;
}

// ─── QBO Data Routes ────────────────────────────────────────────────

// Profit & Loss report
app.get('/qbo/profit-loss', async (req, res) => {
  try {
    const token = await getValidToken();
    const { start_date = '2024-01-01', end_date = '2024-12-31' } = req.query;
    const response = await axios.get(
      `${QBO_BASE_URL}/${QBO_REALM_ID}/reports/ProfitAndLoss?start_date=${start_date}&end_date=${end_date}&summarize_column_by=Month`,
      { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } }
    );
    res.json(response.data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Invoices / Sales
app.get('/qbo/invoices', async (req, res) => {
  try {
    const token = await getValidToken();
    const response = await axios.get(
      `${QBO_BASE_URL}/${QBO_REALM_ID}/query?query=SELECT * FROM Invoice ORDER BY TxnDate DESC MAXRESULTS 100`,
      { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } }
    );
    res.json(response.data.QueryResponse.Invoice || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Expenses
app.get('/qbo/expenses', async (req, res) => {
  try {
    const token = await getValidToken();
    const response = await axios.get(
      `${QBO_BASE_URL}/${QBO_REALM_ID}/query?query=SELECT * FROM Purchase ORDER BY TxnDate DESC MAXRESULTS 100`,
      { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } }
    );
    res.json(response.data.QueryResponse.Purchase || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Auth status check
app.get('/qbo/status', (req, res) => {
  res.json({
    connected: !!qboTokens.access_token,
    expires_at: qboTokens.expires_at ? new Date(qboTokens.expires_at).toISOString() : null
  });
});

// ─── Existing POS/Azure Route ────────────────────────────────────────
app.get('/sales', async (req, res) => {
  try {
    const pool = await sql.connect(dbConfig);
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
        oh.StoreName, od.Upc, od.Name
      ORDER BY WeekStart DESC, oh.StoreName, od.Upc
    `);
    res.json(result.recordset);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/status', (req, res) => res.json({ status: 'ok' }));

app.listen(process.env.PORT || 3000, () => console.log('API running'));
