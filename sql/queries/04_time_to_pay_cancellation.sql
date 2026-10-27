-- Pregunta 4: tiempo a pago y tasa de cancelacion por pais/moneda.
-- time_to_pay_hours solo existe cuando hay eventos created y paid validos.
SELECT
    c.country_name,
    cur.currency_code,
    COUNT(*) AS orders,
    SUM(CASE WHEN l.paid_at IS NOT NULL THEN 1 ELSE 0 END) AS paid_orders,
    ROUND(AVG(l.time_to_pay_hours), 2) AS avg_time_to_pay_hours,
    ROUND(
        (
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.time_to_pay_hours)
        )::numeric,
        2
    ) AS median_time_to_pay_hours,
    SUM(CASE WHEN l.was_cancelled THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(
        100.0 * SUM(CASE WHEN l.was_cancelled THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS cancellation_rate_pct
FROM analytics.fact_order_lifecycle l
JOIN analytics.dim_country c
  ON c.country_key = l.country_key
JOIN analytics.dim_currency cur
  ON cur.currency_key = l.currency_key
WHERE l.source = 'events'
GROUP BY c.country_name, cur.currency_code
ORDER BY orders DESC, cancellation_rate_pct DESC;
