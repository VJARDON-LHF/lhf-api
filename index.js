'use strict';
// lhf-api — búsqueda de producto por tienda y día (schema dw de Azure SQL).
//
// Diseño: NO se cachea el detalle. Se cachea un catálogo chico (un UPC por
// fila, ~7k) para buscar/autocompletar, y el detalle día × tienda se pide a
// dw por UPC en vivo: 56 semanas de un producto son ~400 filas y dw las
// devuelve en milisegundos filtrando por ItemCode + fecha. Así este servicio
// usa poca RAM y nunca compite con el Pulso.
//
// Rutas (Bearer API_TOKEN, igual que antes):
//   GET /catalog?q=tostones          → productos que matchean (nombre/marca/UPC), máx 50
//   GET /sales?upc=A,B&weeks=56      → día × tienda × UPC para esos UPC (también ?from=&to=&store_id=)
//   GET /sales?q=tostones&weeks=12   → atajo: resuelve el q contra el catálogo (máx 20 UPC) y trae su detalle
//   GET /status, GET /health, POST /refresh (recarga el catálogo)

const express = require('express');
const sql = require('mssql');
const cors = require('cors');
const cron = require('node-cron');
const compression = require('compression');

const app = express();
app.use(compression());
app.use(cors({ origin: (process.env.CORS_ORIGINS || '*').split(',').map((s) => s.trim()) }));

const config = {
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  options: { encrypt: true },
  pool: { max: 4, min: 0, idleTimeoutMillis: 30000 },
  connectionTimeout: 30000,
  requestTimeout: 120000
};

const TZ = 'America/Puerto_Rico';
const MAX_WEEKS = Number(process.env.MAX_WEEKS || 56);
const DEFAULT_WEEKS = Number(process.env.DEFAULT_WEEKS || 12);
const MAX_UPCS = 50;
const CATALOG_CRON = process.env.CATALOG_CRON || '15 5 * * *'; // después de la carga de dw

// ---------------------------------------------------------------- DB ---
let pool;
async function getPool() {
  if (!pool) {
    const p = new sql.ConnectionPool(config);
    p.on('error', (err) => { console.error('Pool error:', err.message); pool = null; });
    try { pool = await p.connect(); } catch (err) { pool = null; throw err; }
  }
  return pool;
}

const isoDate = (v) => (v instanceof Date ? v.toISOString().slice(0, 10) : String(v ?? '').slice(0, 10));
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const todayPR = () => new Date().toLocaleDateString('en-CA', { timeZone: TZ });
function addDays(iso, n) { const d = new Date(`${iso}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + n); return isoDate(d); }
function mondayOf(iso) { const d = new Date(`${iso}T00:00:00Z`); return addDays(iso, -((d.getUTCDay() + 6) % 7)); }

// ------------------------------------------------------------- SQL ----
const DIVISION = (raw) => `CASE UPPER(LTRIM(RTRIM(${raw})))
    WHEN 'GOODIELABS' THEN N'GoodieLabs' WHEN 'GONDOLA SECO' THEN N'Gondola Seco'
    WHEN 'GONDOLA FRIO' THEN N'Gondola Frio' WHEN 'CARNICERIA' THEN N'Carniceria'
    WHEN 'CARNICERIA ADOBADOS' THEN N'Carniceria Adobados' WHEN 'FLEXYKITCHEN' THEN N'FlexyKitchen'
    WHEN 'PRODUCE' THEN N'Produce' WHEN 'COFFESHOP' THEN N'Coffeshop'
    WHEN 'ALCOHOL' THEN N'Alcohol' WHEN 'DELI' THEN N'Deli'
    ELSE N'Otras' END`;

// Catálogo: un UPC por fila, la entrada no borrada editada más recientemente.
const CATALOG_SQL = `
WITH Cat AS (
    SELECT p.PrimaryUpc, p.EffectiveProductName, p.BrandCanonicalName, p.BrandRawValue,
           p.DivisionCanonicalName, p.DepartmentNameEs, p.CategoryNameEs, p.BasePrice, p.CatalogCost,
           ROW_NUMBER() OVER (PARTITION BY p.PrimaryUpc
                              ORDER BY p.IsDeleted ASC, p.BusinessItemEditedAtUtc DESC,
                                       p.ItemEditedAtUtc DESC, p.ProductKey DESC) AS rn
    FROM dw.vDimProductCurrent p
    WHERE p.PrimaryUpc IS NOT NULL AND p.PrimaryUpc <> ''
)
SELECT PrimaryUpc                                   AS upc,
       EffectiveProductName                         AS product_name,
       COALESCE(BrandCanonicalName, BrandRawValue)  AS brand,
       ${DIVISION('DivisionCanonicalName')}         AS division,
       DepartmentNameEs                             AS department,
       CategoryNameEs                               AS category,
       CAST(BasePrice AS decimal(18,2))             AS price,
       CAST(CatalogCost AS decimal(18,4))           AS cost
FROM Cat WHERE rn = 1
ORDER BY upc;`;

// Detalle día × tienda para una lista de UPC (@upcs = lista separada por coma).
const SALES_SQL = `
WITH Cat AS (
    SELECT p.PrimaryUpc, p.DivisionCanonicalName,
           ROW_NUMBER() OVER (PARTITION BY p.PrimaryUpc
                              ORDER BY p.IsDeleted ASC, p.BusinessItemEditedAtUtc DESC,
                                       p.ItemEditedAtUtc DESC, p.ProductKey DESC) AS rn
    FROM dw.vDimProductCurrent p
    WHERE p.PrimaryUpc IN (SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@upcs, ','))
)
SELECT d.FullDate                                          AS sale_date,
       d.WeekStartDate                                     AS week_start,
       d.DayOfWeekIso                                      AS dow,
       b.BusinessId                                        AS store_id,
       b.BusinessName                                      AS store_name,
       CASE WHEN b.BusinessId IN (9, 6, 12, 13) THEN N'Area 1'
            WHEN b.BusinessId IN (5, 3, 10, 11) THEN N'Area 2' ELSE N'Otras' END AS area,
       x.ItemCode                                          AS upc,
       MAX(x.ProductName)                                  AS product_name,
       MAX(x.BrandName)                                    AS brand,
       ${DIVISION('COALESCE(MAX(c.DivisionCanonicalName), MAX(x.DivisionCanonicalName))')} AS division,
       CAST(SUM(x.SalesQuantity) AS decimal(18,2))         AS qty,
       CAST(SUM(x.GrossSalesAmount) AS decimal(18,2))      AS sales,
       CAST(SUM(x.NetSalesAmount) AS decimal(18,2))        AS net_sales,
       CAST(SUM(x.SalesCost) AS decimal(18,2))             AS cost,
       SUM(x.SalesLineCount)                               AS lines
FROM dw.AggSalesProductDaily x
JOIN dw.DimDate d ON d.DateKey = x.BusinessDateKey
JOIN dw.DimBusiness b ON b.BusinessKey = x.BusinessKey AND b.RowIsCurrent = 1 AND b.IsExcluded = 0
LEFT JOIN Cat c ON c.PrimaryUpc = x.ItemCode AND c.rn = 1
WHERE x.ItemCode IN (SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@upcs, ','))
  AND x.BusinessDateKey BETWEEN CONVERT(int, CONVERT(char(8), @from, 112))
                            AND CONVERT(int, CONVERT(char(8), @to, 112))
GROUP BY d.FullDate, d.WeekStartDate, d.DayOfWeekIso, b.BusinessId, b.BusinessName, x.ItemCode
ORDER BY sale_date DESC, store_id, upc;`;

// --------------------------------------------------------- Catálogo ---
let catalog = [];          // filas
let catalogIndex = [];     // [{ upc, hay }] para buscar
let catalogAt = null;
let lastError = null;
let loading = false;

async function loadCatalog() {
  if (loading) return;
  loading = true;
  const t0 = Date.now();
  try {
    const p = await getPool();
    const r = await p.request().query(CATALOG_SQL);
    catalog = r.recordset;
    catalogIndex = catalog.map((row) => ({
      upc: String(row.upc),
      hay: `${row.product_name ?? ''} ${row.brand ?? ''} ${row.upc}`.toLowerCase()
    }));
    catalogAt = new Date();
    lastError = null;
    console.log(`Catálogo: ${catalog.length} productos en ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  } catch (err) {
    lastError = { message: err.message, code: err.code, at: new Date().toISOString() };
    console.error('Catálogo falló:', err.code || '', err.message);
    pool = null;
  } finally {
    loading = false;
  }
}

function searchCatalog(q, max = 50) {
  const needle = String(q || '').trim().toLowerCase();
  if (!needle) return [];
  const terms = needle.split(/\s+/);
  const out = [];
  for (let i = 0; i < catalogIndex.length && out.length < max; i++) {
    const h = catalogIndex[i].hay;
    if (terms.every((t) => h.includes(t))) out.push(catalog[i]);
  }
  return out;
}

cron.schedule(CATALOG_CRON, loadCatalog, { timezone: TZ });

// -------------------------------------------------------------- Auth ---
let tokenWarned = false;
function requireToken(req, res, next) {
  const expected = process.env.API_TOKEN;
  if (!expected) {
    if (!tokenWarned) { console.warn('API_TOKEN no definido — rutas abiertas.'); tokenWarned = true; }
    return next();
  }
  const provided = (req.headers.authorization || '').replace(/^Bearer\s+/i, '') || req.get('x-api-key') || req.query.key;
  if (provided !== expected) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ------------------------------------------------------------ Rutas ---
app.get('/health', (req, res) => res.status(200).send('ok'));

app.get('/status', (req, res) => res.json({
  status: catalog.length ? 'ready' : loading ? 'loading' : 'empty',
  source: 'dw', catalogProducts: catalog.length, catalogAt, lastError,
  maxWeeks: MAX_WEEKS, heapMb: Math.round(process.memoryUsage().heapUsed / 1048576)
}));

app.post('/refresh', requireToken, (req, res) => { loadCatalog(); res.status(202).json({ started: true }); });

app.get('/catalog', requireToken, (req, res) => {
  if (!catalog.length) return res.status(503).json({ error: 'Catálogo no cargado aún', lastError });
  const limit = Math.min(Number(req.query.limit) || 50, 500);
  const rows = req.query.q ? searchCatalog(req.query.q, limit) : catalog.slice(0, limit);
  res.set('Cache-Control', 'public, max-age=300');
  res.json({ count: rows.length, catalogAt, rows });
});

app.get('/sales', requireToken, async (req, res) => {
  // 1) qué UPC
  let upcs = String(req.query.upc || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (!upcs.length && req.query.q) upcs = searchCatalog(req.query.q, 20).map((r) => String(r.upc));
  if (!upcs.length) return res.status(400).json({ error: 'Indica ?upc=A,B o ?q=texto' });
  if (upcs.length > MAX_UPCS) return res.status(400).json({ error: `Máximo ${MAX_UPCS} UPC por llamada` });

  // 2) qué rango: ?from/?to explícitos, o ?weeks (semanas completas lunes–domingo hacia atrás, más la actual)
  const to = DATE_RE.test(req.query.to || '') ? req.query.to : todayPR();
  let from;
  if (DATE_RE.test(req.query.from || '')) from = req.query.from;
  else {
    const weeks = Math.min(Math.max(Number(req.query.weeks) || DEFAULT_WEEKS, 1), MAX_WEEKS);
    from = addDays(mondayOf(to), -7 * weeks);
  }
  if (addDays(from, 7 * MAX_WEEKS + 7) < to) return res.status(400).json({ error: `Rango máximo ${MAX_WEEKS} semanas` });

  // 3) a dw
  try {
    const p = await getPool();
    const r = await p.request()
      .input('upcs', sql.NVarChar(sql.MAX), upcs.join(','))
      .input('from', sql.Date, from)
      .input('to', sql.Date, to)
      .query(`SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;\n${SALES_SQL}`);
    let rows = r.recordset;
    for (const row of rows) { row.sale_date = isoDate(row.sale_date); row.week_start = isoDate(row.week_start); }
    if (req.query.store_id) {
      const ids = new Set(String(req.query.store_id).split(','));
      rows = rows.filter((x) => ids.has(String(x.store_id)));
    }
    res.set('Cache-Control', 'public, max-age=300');
    res.json({ from, to, upcs, count: rows.length, rows });
  } catch (err) {
    pool = null;
    res.status(503).json({ error: 'dw no disponible', detail: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, '0.0.0.0', () => {
  console.log(`lhf-api (dw) en puerto ${port} · máx ${MAX_WEEKS} semanas`);
  loadCatalog();
});

process.on('unhandledRejection', (err) => console.error('Unhandled rejection:', err && err.message));
