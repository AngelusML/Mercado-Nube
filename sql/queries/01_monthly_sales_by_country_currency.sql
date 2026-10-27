-- Pregunta 1: ventas brutas, devoluciones y ventas netas por mes, fuente,
-- pais y moneda, normalizadas a la moneda de reporte (USD).
--
-- Une las dos fuentes de ventas a traves de analytics.v_sales_movements:
--   * historical: lineas de factura de Online Retail II (moneda original GBP).
--   * events: pedidos de Colombia, Mexico y Peru (paid = venta, refunded = devolucion).
-- Regla anti doble conteo: cada devolucion aparece UNA sola vez como
-- movimiento 'return'; neto = bruto - devoluciones.
-- Las tasas son DEMO (ops.exchange_rates), no cotizaciones reales.
-- Limitacion: el historico es 2010-2011 y los eventos 2026; son series separadas.
SELECT
    d.year,
    d.month,
    m.source,
    c.country_code,
    c.country_name,
    cur.currency_code AS original_currency,
    'USD' AS reporting_currency,
    SUM(CASE WHEN m.movement_type = 'sale' THEN 1 ELSE 0 END) AS sale_movements,
    SUM(CASE WHEN m.movement_type = 'return' THEN 1 ELSE 0 END) AS return_movements,
    ROUND(SUM(m.gross_amount_usd), 2) AS gross_usd,
    ROUND(SUM(m.return_amount_usd), 2) AS returns_usd,
    ROUND(SUM(m.gross_amount_usd) - SUM(m.return_amount_usd), 2) AS net_usd,
    ROUND(
        100.0 * SUM(m.return_amount_usd) / NULLIF(SUM(m.gross_amount_usd), 0),
        2
    ) AS return_rate_pct
FROM analytics.v_sales_movements m
JOIN analytics.dim_date d
  ON d.date_key = m.date_key
JOIN analytics.dim_country c
  ON c.country_key = m.country_key
JOIN analytics.dim_currency cur
  ON cur.currency_key = m.currency_key
GROUP BY d.year, d.month, m.source, c.country_code, c.country_name, cur.currency_code
ORDER BY d.year, d.month, m.source, net_usd DESC, c.country_name;
