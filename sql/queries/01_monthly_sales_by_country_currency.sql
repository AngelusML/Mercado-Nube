-- Pregunta 1: ventas brutas, devoluciones y neto por mes, pais y moneda.
-- Montos en USD; la tasa es DEMO (ver ops.exchange_rates).
WITH monthly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        c.country_name,
        cur.currency_code,
        COUNT(*) AS line_count,
        SUM(f.gross_amount_usd) AS gross_usd,
        SUM(f.return_amount_usd) AS returns_usd,
        SUM(f.net_amount_usd) AS net_usd
    FROM analytics.fact_order_line f
    JOIN analytics.dim_date d
      ON d.date_key = f.date_key
    JOIN analytics.dim_country c
      ON c.country_key = f.country_key
    JOIN analytics.dim_currency cur
      ON cur.currency_key = f.currency_key
    GROUP BY d.year, d.month, d.month_name, c.country_name, cur.currency_code
)
SELECT
    year,
    month,
    month_name,
    country_name,
    currency_code,
    line_count,
    ROUND(gross_usd, 2) AS gross_usd,
    ROUND(returns_usd, 2) AS returns_usd,
    ROUND(net_usd, 2) AS net_usd
FROM monthly
ORDER BY year, month, net_usd DESC, country_name;
