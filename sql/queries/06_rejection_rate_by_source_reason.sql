-- Pregunta 6: que porcentaje de registros se rechaza por fuente y motivo, y
-- como cambio durante los ultimos siete lotes de cada fuente.
-- El denominador es rows_read del lote, asi el porcentaje es comparable
-- entre lotes de distinto tamano. Fuente: ops.pipeline_run_metrics (capa
-- published) + ops.quarantine_records (motivo de cada rechazo).

-- 6a) Tendencia: tasa de rechazo de los ultimos 7 lotes de cada fuente.
WITH ranked AS (
    SELECT
        source,
        batch_id,
        rows_read,
        rows_rejected,
        rows_duplicated,
        finished_at,
        ROW_NUMBER() OVER (
            PARTITION BY source ORDER BY batch_id DESC
        ) AS batch_rank
    FROM ops.pipeline_run_metrics
    WHERE layer = 'published'
)
SELECT
    source,
    batch_id,
    rows_read,
    rows_rejected,
    ROUND(100.0 * rows_rejected / NULLIF(rows_read, 0), 4) AS rejection_pct,
    ROUND(
        100.0 * rows_rejected / NULLIF(rows_read, 0)
        - LAG(100.0 * rows_rejected / NULLIF(rows_read, 0)) OVER (
            PARTITION BY source ORDER BY batch_id
        ),
        4
    ) AS change_vs_previous_pp
FROM ranked
WHERE batch_rank <= 7
ORDER BY source, batch_id;

-- 6b) Motivos de rechazo por lote y fuente (mismos 7 lotes por fuente).
WITH ranked AS (
    SELECT
        source,
        batch_id,
        rows_read,
        ROW_NUMBER() OVER (
            PARTITION BY source ORDER BY batch_id DESC
        ) AS batch_rank
    FROM ops.pipeline_run_metrics
    WHERE layer = 'published'
)
SELECT
    r.source,
    r.batch_id,
    q.reason_code,
    COUNT(*) AS rejected_rows,
    ROUND(100.0 * COUNT(*) / NULLIF(r.rows_read, 0), 4) AS pct_of_batch_read
FROM ranked r
JOIN ops.quarantine_records q
  ON q.batch_id = r.batch_id
 AND q.source = r.source
WHERE r.batch_rank <= 7
GROUP BY r.source, r.batch_id, r.rows_read, q.reason_code
ORDER BY r.source, r.batch_id, rejected_rows DESC, q.reason_code;

-- 6c) Consolidado por fuente y motivo en esos lotes.
WITH ranked AS (
    SELECT
        source,
        batch_id,
        rows_read,
        ROW_NUMBER() OVER (
            PARTITION BY source ORDER BY batch_id DESC
        ) AS batch_rank
    FROM ops.pipeline_run_metrics
    WHERE layer = 'published'
),
totals AS (
    SELECT source, SUM(rows_read) AS rows_read
    FROM ranked
    WHERE batch_rank <= 7
    GROUP BY source
),
reasons AS (
    SELECT r.source, q.reason_code, COUNT(*) AS rejected_rows
    FROM ranked r
    JOIN ops.quarantine_records q
      ON q.batch_id = r.batch_id
     AND q.source = r.source
    WHERE r.batch_rank <= 7
    GROUP BY r.source, q.reason_code
)
SELECT
    t.source,
    COALESCE(x.reason_code, '(sin rechazos)') AS reason_code,
    t.rows_read,
    COALESCE(x.rejected_rows, 0) AS rejected_rows,
    ROUND(100.0 * COALESCE(x.rejected_rows, 0) / NULLIF(t.rows_read, 0), 4) AS rejection_pct
FROM totals t
LEFT JOIN reasons x
  ON x.source = t.source
ORDER BY t.source, rejected_rows DESC, reason_code;
