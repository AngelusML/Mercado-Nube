-- Pregunta 2: ingresos brutos, devoluciones y neto por categoria.
-- Se usa la version del producto vigente en la fecha de la linea (SCD2).
SELECT
    p.category,
    COUNT(*) AS line_count,
    COUNT(DISTINCT f.invoice_no) AS invoice_count,
    SUM(CASE WHEN f.line_type = 'sale' THEN f.quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN f.line_type = 'return' THEN -f.quantity ELSE 0 END) AS units_returned,
    ROUND(SUM(f.gross_amount_usd), 2) AS gross_usd,
    ROUND(SUM(f.return_amount_usd), 2) AS returns_usd,
    ROUND(SUM(f.net_amount_usd), 2) AS net_usd
FROM analytics.fact_order_line f
JOIN analytics.dim_product p
  ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY net_usd DESC;
