# lhf-api (v2 — schema dw)

Búsqueda de producto por tienda y día para el app de heatmap. Lee `dw` en Azure SQL.
No cachea el detalle: cachea el catálogo (un UPC por fila) y pide a dw solo los UPC
consultados, así soporta hasta `MAX_WEEKS` (56) semanas sin cargar memoria.

Rutas (header `Authorization: Bearer <API_TOKEN>`, o `x-api-key`):
- `GET /catalog?q=tostones` → hasta 50 productos (upc, product_name, brand, division, department, category, price, cost)
- `GET /sales?upc=A,B&weeks=56` → día × tienda × UPC; también `?from=&to=&store_id=`
- `GET /sales?q=tostones&weeks=12` → resuelve q en el catálogo (máx 20 UPC) y trae el detalle
- `GET /status`, `GET /health`, `POST /refresh` (recarga el catálogo)

Filas de `/sales`: `sale_date, week_start, dow (1=lun…7=dom), store_id, store_name, area, upc,
product_name, brand, division, qty, sales, net_sales, cost, lines`.

Variables: `DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD, API_TOKEN` (igual que antes);
opcionales `CORS_ORIGINS`, `MAX_WEEKS` (56), `DEFAULT_WEEKS` (12), `CATALOG_CRON` (`15 5 * * *`).
