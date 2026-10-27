-- Pregunta 6a: rechazos por lote/fuente/motivo en los ultimos 7 lotes publicados.
-- El denominador es rows_read del lote, por lo que el porcentaje es comparable
-- entre lotes de distinto tamano.
WITH recent_batches AS (
    SELECT
        batch_id,
        source,
        rows_read,
        finished_at
    FROM ops.pipeline_run_metrics
    WHERE layer = 'published'
    ORDER BY finished_at DESC, batch_id DESC
    LIMIT 7
)
SELECT
    r.batch_id,
    r.source,
    r.rows_read,
    q.reason_code,
    COUNT(*) AS rejected_rows,
    ROUND(100.0 * COUNT(*) / NULLIF(r.rows_read, 0), 4) AS pct_of_batch_read
FROM recent_batches r
JOIN ops.quarantine_records q
  ON q.batch_id = r.batch_id
 AND q.source = r.source
GROUP BY r.batch_id, r.source, r.rows_read, q.reason_code, r.finished_at
ORDER BY r.finished_at DESC, pct_of_batch_read DESC, q.reason_code;

-- Pregunta 6b: consolidado por fuente en esos mismos 7 lotes.
WITH recent_batches AS (
    SELECT batch_id, source, rows_read, rows_rejected
    FROM ops.pipeline_run_metrics
    WHERE layer = 'published'
    ORDER BY finished_at DESC, batch_id DESC
    LIMIT 7
)
SELECT
    source,
    SUM(rows_read) AS rows_read,
    SUM(rows_rejected) AS rows_rejected,
    ROUND(100.0 * SUM(rows_rejected) / NULLIF(SUM(rows_read), 0), 4) AS rejection_pct
FROM recent_batches
GROUP BY source
ORDER BY rejection_pct DESC;
