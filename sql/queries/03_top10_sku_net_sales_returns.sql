-- Pregunta 3: top 10 SKU por venta neta, con ventas brutas y devoluciones.
-- Los atributos mostrados son los vigentes (is_current = true).
SELECT
    cur.sku,
    cur.product_name,
    cur.category,
    SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END) AS units_returned,
    ROUND(SUM(f.gross_amount_usd), 2) AS gross_usd,
    ROUND(SUM(f.return_amount_usd), 2) AS returns_usd,
    ROUND(SUM(f.net_amount_usd), 2) AS net_usd
FROM analytics.fact_order_line f
JOIN analytics.dim_product p
  ON p.product_key = f.product_key
JOIN analytics.dim_product cur
  ON cur.sku = p.sku
 AND cur.is_current = true
WHERE cur.is_service = false
GROUP BY cur.sku, cur.product_name, cur.category
ORDER BY net_usd DESC
LIMIT 10;
