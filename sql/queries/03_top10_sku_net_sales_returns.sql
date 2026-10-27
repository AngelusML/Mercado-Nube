-- Pregunta 3: los 10 SKU con mayor venta neta y los 10 con mas devoluciones,
-- con volumen (unidades) y tasa de devolucion.
-- Los atributos mostrados son los vigentes (is_current = true).
-- Se excluyen codigos de servicio (POST, MANUAL, AMAZONFEE...): no son productos.
-- return_rate_pct = devoluciones USD / ventas brutas USD.
-- unit_return_pct = unidades devueltas / unidades vendidas.

-- 3a) Top 10 por venta neta.
WITH sku_sales AS (
    SELECT
        cur.sku,
        cur.product_name,
        cur.category,
        SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END) AS units_returned,
        SUM(f.gross_amount_usd) AS gross_usd,
        SUM(f.return_amount_usd) AS returns_usd,
        SUM(f.net_amount_usd) AS net_usd
    FROM analytics.fact_order_line f
    JOIN analytics.dim_product p
      ON p.product_key = f.product_key
    JOIN analytics.dim_product cur
      ON cur.sku = p.sku
     AND cur.is_current = true
    WHERE cur.is_service = false
    GROUP BY cur.sku, cur.product_name, cur.category
)
SELECT
    sku,
    product_name,
    category,
    units_sold,
    units_returned,
    ROUND(gross_usd, 2) AS gross_usd,
    ROUND(returns_usd, 2) AS returns_usd,
    ROUND(net_usd, 2) AS net_usd,
    ROUND(100.0 * returns_usd / NULLIF(gross_usd, 0), 2) AS return_rate_pct,
    ROUND(100.0 * units_returned / NULLIF(units_sold, 0), 2) AS unit_return_pct
FROM sku_sales
ORDER BY net_usd DESC
LIMIT 10;

-- 3b) Top 10 por monto devuelto.
WITH sku_sales AS (
    SELECT
        cur.sku,
        cur.product_name,
        cur.category,
        SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END) AS units_returned,
        SUM(CASE WHEN f.line_type = 'return' THEN 1 ELSE 0 END) AS return_lines,
        SUM(f.gross_amount_usd) AS gross_usd,
        SUM(f.return_amount_usd) AS returns_usd
    FROM analytics.fact_order_line f
    JOIN analytics.dim_product p
      ON p.product_key = f.product_key
    JOIN analytics.dim_product cur
      ON cur.sku = p.sku
     AND cur.is_current = true
    WHERE cur.is_service = false
    GROUP BY cur.sku, cur.product_name, cur.category
)
SELECT
    sku,
    product_name,
    category,
    return_lines,
    units_returned,
    units_sold,
    ROUND(returns_usd, 2) AS returns_usd,
    ROUND(gross_usd, 2) AS gross_usd,
    ROUND(100.0 * returns_usd / NULLIF(gross_usd, 0), 2) AS return_rate_pct,
    ROUND(100.0 * units_returned / NULLIF(units_sold, 0), 2) AS unit_return_pct
FROM sku_sales
WHERE returns_usd > 0
ORDER BY returns_usd DESC
LIMIT 10;
