-- ============================================================
-- Mercado Nube - carga idempotente de curated/ (S3) a Redshift
--
-- Antes de ejecutar: reemplaza <TU-BUCKET> por el output data_bucket_name de
-- Terraform. El IAM_ROLE default es el rol redshift_copy (solo lee curated/).
--
-- Patron por tabla: TRUNCATE staging -> COPY Parquet -> MERGE al destino.
-- Reejecutar este archivo produce el mismo estado final (idempotente):
--   * fact_order_line   : MERGE por line_key.
--   * fact_order_event  : MERGE por (source, event_id).
--   * fact_order_lifecycle: se queda con la version mas reciente de cada
--     pedido (updated_at) y hace MERGE por (source, order_id); asi los
--     eventos tardios que reprocesaron un pedido corrigen la fila.
--   * dim_product/dim_customer: snapshot completo del lote mas reciente.
-- Si la carga falla a mitad, se vuelve a ejecutar completa: nada se duplica.
--
-- Pendiente de validar en la cuenta de laboratorio (no se ejecuto en AWS):
--   * Si COPY rechaza timestamps en nanosegundos, escribir los Parquet con
--     coerce_timestamps="us" en src/storage.put_parquet.
--   * El historico debe tener un solo batch_id en curated/ (MERGE exige
--     claves unicas en el origen).
-- ============================================================

-- ---------------------------------------------------------------
-- 0. Dimensiones de referencia (una sola vez). Exportarlas desde el
--    warehouse local para conservar las mismas claves:
--      docker exec mn_postgres psql -U mn_app -d mercado_nube -c "\copy analytics.dim_country TO STDOUT CSV HEADER" > dim_country.csv
--      docker exec mn_postgres psql -U mn_app -d mercado_nube -c "\copy analytics.dim_currency TO STDOUT CSV HEADER" > dim_currency.csv
--      docker exec mn_postgres psql -U mn_app -d mercado_nube -c "\copy ops.exchange_rates TO STDOUT CSV HEADER" > exchange_rates.csv
--      aws s3 cp dim_country.csv    s3://<TU-BUCKET>/curated/reference/dim_country.csv
--      aws s3 cp dim_currency.csv   s3://<TU-BUCKET>/curated/reference/dim_currency.csv
--      aws s3 cp exchange_rates.csv s3://<TU-BUCKET>/curated/reference/exchange_rates.csv
-- ---------------------------------------------------------------
TRUNCATE analytics.dim_country;
COPY analytics.dim_country
FROM 's3://<TU-BUCKET>/curated/reference/dim_country.csv'
IAM_ROLE default
FORMAT AS CSV IGNOREHEADER 1 TIMEFORMAT 'auto';

TRUNCATE analytics.dim_currency;
COPY analytics.dim_currency
FROM 's3://<TU-BUCKET>/curated/reference/dim_currency.csv'
IAM_ROLE default
FORMAT AS CSV IGNOREHEADER 1 TIMEFORMAT 'auto';

TRUNCATE ops.exchange_rates;
COPY ops.exchange_rates
FROM 's3://<TU-BUCKET>/curated/reference/exchange_rates.csv'
IAM_ROLE default
FORMAT AS CSV IGNOREHEADER 1;

-- ---------------------------------------------------------------
-- 1. dim_product (SCD2): snapshot completo del ultimo lote de catalogo.
--    Cambia la ruta por la carpeta batch_id mas reciente.
-- ---------------------------------------------------------------
TRUNCATE staging.stg_dim_product;
COPY staging.stg_dim_product
FROM 's3://<TU-BUCKET>/curated/table=dim_product/ingest_date=2026-10-26/batch_id=catalog_2026-10-26_v3/'
IAM_ROLE default
FORMAT AS PARQUET;

MERGE INTO analytics.dim_product
USING staging.stg_dim_product s
ON analytics.dim_product.product_key = s.product_key
WHEN MATCHED THEN UPDATE SET
    product_name = s.product_name,
    category = s.category,
    is_service = s.is_service,
    is_active = s.is_active,
    valid_from = s.valid_from,
    valid_to = s.valid_to,
    is_current = s.is_current,
    catalog_version = s.catalog_version,
    updated_at = s.updated_at,
    record_hash = s.record_hash
WHEN NOT MATCHED THEN INSERT VALUES (
    s.product_key, s.sku, s.product_name, s.category, s.is_service,
    s.is_active, s.valid_from, s.valid_to, s.is_current, s.catalog_version,
    s.updated_at, s.record_hash, s.created_at
);

-- Miembro desconocido (clave 0), igual que en local.
INSERT INTO analytics.dim_product
SELECT 0, '__UNKNOWN__', 'Unknown', 'Unknown', FALSE, FALSE,
       '1900-01-01 00:00:00+00', '9999-12-31 23:59:59+00', TRUE, 0,
       GETDATE(), 'unknown', GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM analytics.dim_product WHERE product_key = 0);

-- ---------------------------------------------------------------
-- 2. dim_customer (incluye el miembro desconocido -1).
-- ---------------------------------------------------------------
TRUNCATE staging.stg_dim_customer;
COPY staging.stg_dim_customer
FROM 's3://<TU-BUCKET>/curated/table=dim_customer/'
IAM_ROLE default
FORMAT AS PARQUET;

MERGE INTO analytics.dim_customer
USING staging.stg_dim_customer s
ON analytics.dim_customer.customer_key = s.customer_key
WHEN MATCHED THEN UPDATE SET
    country_key = s.country_key,
    is_unknown = s.is_unknown,
    updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT VALUES (
    s.customer_key, s.source, s.source_customer_id, s.country_key,
    s.is_unknown, s.created_at, s.updated_at
);

INSERT INTO analytics.dim_customer
SELECT -1, 'system', 'UNKNOWN', 0, TRUE, GETDATE(), GETDATE()
WHERE NOT EXISTS (SELECT 1 FROM analytics.dim_customer WHERE customer_key = -1);

-- ---------------------------------------------------------------
-- 3. fact_order_line: MERGE por line_key.
-- ---------------------------------------------------------------
TRUNCATE staging.stg_fact_order_line;
COPY staging.stg_fact_order_line
FROM 's3://<TU-BUCKET>/curated/table=fact_order_line/'
IAM_ROLE default
FORMAT AS PARQUET;

MERGE INTO analytics.fact_order_line
USING staging.stg_fact_order_line s
ON analytics.fact_order_line.line_key = s.line_key
WHEN MATCHED THEN UPDATE SET
    product_key = s.product_key,
    customer_key = s.customer_key,
    gross_amount_usd = CAST(s.gross_amount_usd AS DECIMAL(18,2)),
    return_amount_usd = CAST(s.return_amount_usd AS DECIMAL(18,2)),
    net_amount_usd = CAST(s.net_amount_usd AS DECIMAL(18,2)),
    record_hash = s.record_hash,
    batch_id = s.batch_id
WHEN NOT MATCHED THEN INSERT VALUES (
    s.line_key, s.invoice_no, s.source, s.product_key, s.customer_key,
    s.country_key, s.currency_key, s.date_key, s.invoice_date_utc,
    s.quantity, CAST(s.unit_price AS DECIMAL(18,4)),
    CAST(s.gross_amount AS DECIMAL(18,2)), CAST(s.return_amount AS DECIMAL(18,2)),
    CAST(s.net_amount AS DECIMAL(18,2)), CAST(s.gross_amount_usd AS DECIMAL(18,2)),
    CAST(s.return_amount_usd AS DECIMAL(18,2)), CAST(s.net_amount_usd AS DECIMAL(18,2)),
    s.line_type, s.is_cancellation, s.record_hash, s.batch_id, s.created_at
);

-- ---------------------------------------------------------------
-- 4. fact_order_event: MERGE por (source, event_id). Cada evento se publica
--    en un solo lote, asi que no hay duplicados entre archivos de curated/.
-- ---------------------------------------------------------------
TRUNCATE staging.stg_fact_order_event;
COPY staging.stg_fact_order_event
FROM 's3://<TU-BUCKET>/curated/table=fact_order_event/'
IAM_ROLE default
FORMAT AS PARQUET;

MERGE INTO analytics.fact_order_event
USING staging.stg_fact_order_event s
ON analytics.fact_order_event.source = s.source
   AND analytics.fact_order_event.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
    is_late = s.is_late,
    late_hours = CAST(s.late_hours AS DECIMAL(18,2)),
    amount_usd = CAST(s.amount_usd AS DECIMAL(18,2)),
    batch_id = s.batch_id
WHEN NOT MATCHED THEN INSERT VALUES (
    s.event_key, s.event_id, s.order_id, s.source, s.event_type,
    s.event_time_utc, s.event_time_original, s.ingested_at, s.is_late,
    CAST(s.late_hours AS DECIMAL(18,2)), s.country_key, s.currency_key,
    CAST(s.amount_original AS DECIMAL(18,2)), CAST(s.amount_usd AS DECIMAL(18,2)),
    s.record_hash, s.batch_id, s.created_at
);

-- ---------------------------------------------------------------
-- 5. fact_order_lifecycle: un pedido aparece en varios lotes (eventos
--    tardios). Se toma la version mas reciente y se hace MERGE.
-- ---------------------------------------------------------------
TRUNCATE staging.stg_fact_order_lifecycle;
COPY staging.stg_fact_order_lifecycle
FROM 's3://<TU-BUCKET>/curated/table=fact_order_lifecycle/'
IAM_ROLE default
FORMAT AS PARQUET;

CREATE TEMP TABLE lifecycle_latest AS
SELECT source, order_id, country_key, currency_key, created_at, paid_at,
       cancelled_at, refunded_at, first_event_at, last_event_at,
       current_status, time_to_pay_hours, was_cancelled, was_refunded,
       event_count, late_event_count, batch_id, updated_at
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY s.source, s.order_id
               ORDER BY s.updated_at DESC, s.event_count DESC, s.batch_id DESC
           ) AS rn
    FROM staging.stg_fact_order_lifecycle s
) ranked
WHERE rn = 1;

MERGE INTO analytics.fact_order_lifecycle
USING lifecycle_latest s
ON analytics.fact_order_lifecycle.source = s.source
   AND analytics.fact_order_lifecycle.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    country_key = s.country_key,
    currency_key = s.currency_key,
    created_at = s.created_at,
    paid_at = s.paid_at,
    cancelled_at = s.cancelled_at,
    refunded_at = s.refunded_at,
    first_event_at = s.first_event_at,
    last_event_at = s.last_event_at,
    current_status = s.current_status,
    time_to_pay_hours = CAST(s.time_to_pay_hours AS DECIMAL(18,2)),
    was_cancelled = s.was_cancelled,
    was_refunded = s.was_refunded,
    event_count = s.event_count,
    late_event_count = s.late_event_count,
    batch_id = s.batch_id,
    updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT VALUES (
    s.source, s.order_id, s.country_key, s.currency_key, s.created_at,
    s.paid_at, s.cancelled_at, s.refunded_at, s.first_event_at,
    s.last_event_at, s.current_status, CAST(s.time_to_pay_hours AS DECIMAL(18,2)),
    s.was_cancelled, s.was_refunded, s.event_count, s.late_event_count,
    s.batch_id, s.updated_at
);

DROP TABLE lifecycle_latest;

-- ---------------------------------------------------------------
-- 6. Reconciliacion rapida (debe coincidir con el warehouse local):
-- ---------------------------------------------------------------
SELECT 'fact_order_line' AS tabla, COUNT(*) AS filas FROM analytics.fact_order_line
UNION ALL SELECT 'fact_order_event', COUNT(*) FROM analytics.fact_order_event
UNION ALL SELECT 'fact_order_lifecycle', COUNT(*) FROM analytics.fact_order_lifecycle
UNION ALL SELECT 'dim_product', COUNT(*) FROM analytics.dim_product;
