-- Pregunta 2: que categorias generan mas ingresos y cuales tienen mayor
-- proporcion de devoluciones.
-- Se usa la version del producto vigente en la fecha de la linea (SCD2).
-- return_rate_pct    = devoluciones USD / ventas brutas USD.
-- unit_return_pct    = unidades devueltas / unidades vendidas.

-- 2a) Categorias ordenadas por ingresos netos.
SELECT
    p.category,
    COUNT(*) AS line_count,
    COUNT(DISTINCT f.invoice_no) AS invoice_count,
    SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END) AS units_returned,
    ROUND(SUM(f.gross_amount_usd), 2) AS gross_usd,
    ROUND(SUM(f.return_amount_usd), 2) AS returns_usd,
    ROUND(SUM(f.net_amount_usd), 2) AS net_usd,
    ROUND(100.0 * SUM(f.return_amount_usd) / NULLIF(SUM(f.gross_amount_usd), 0), 2) AS return_rate_pct
FROM analytics.fact_order_line f
JOIN analytics.dim_product p
  ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY net_usd DESC;

-- 2b) Categorias ordenadas por proporcion de devoluciones.
-- 'Services' agrupa cargos no fisicos (POST, MANUAL, AMAZONFEE...); se marca
-- para no confundirlo con devoluciones de producto.
SELECT
    p.category,
    BOOL_OR(p.is_service) AS includes_service_codes,
    ROUND(SUM(f.gross_amount_usd), 2) AS gross_usd,
    ROUND(SUM(f.return_amount_usd), 2) AS returns_usd,
    ROUND(100.0 * SUM(f.return_amount_usd) / NULLIF(SUM(f.gross_amount_usd), 0), 2) AS return_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END), 0),
        2
    ) AS unit_return_pct
FROM analytics.fact_order_line f
JOIN analytics.dim_product p
  ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY return_rate_pct DESC NULLS LAST;
