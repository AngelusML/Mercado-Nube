-- Pregunta 5a: proporcion de eventos tardios por lote.
-- Evento tardio: ingested_at - event_time_utc > 24 horas.
SELECT
    batch_id,
    COUNT(*) AS events,
    SUM(CASE WHEN is_late THEN 1 ELSE 0 END) AS late_events,
    ROUND(
        100.0 * SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS late_event_pct,
    ROUND(AVG(CASE WHEN is_late THEN late_hours END), 2) AS avg_late_hours,
    MAX(late_hours) AS max_late_hours
FROM analytics.fact_order_event
GROUP BY batch_id
ORDER BY batch_id;

-- Pregunta 5b: ordenes reprocesadas por tener al menos un evento tardio.
-- El lifecycle se recalcula con TODO el historial de la orden, no solo con el evento nuevo.
SELECT
    COUNT(*) AS orders_with_late_events,
    SUM(event_count) AS events_in_those_orders,
    SUM(late_event_count) AS late_events_in_those_orders,
    ROUND(AVG(event_count), 2) AS avg_events_per_order
FROM analytics.fact_order_lifecycle
WHERE source = 'events'
  AND late_event_count > 0;
