-- ============================================================
-- Mercado Nube - modelo dimensional local (PostgreSQL 16)
-- Equivalente funcional a Redshift para el MVP.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------
-- STAGING (todo texto; COPY + casts)
-- ---------------------------
CREATE TABLE IF NOT EXISTS staging.order_lines (
    line_key text,
    invoice_no text,
    source text,
    product_sku text,
    customer_id text,
    country_code text,
    currency_code text,
    date_key text,
    rate_date text,
    invoice_date_utc text,
    quantity text,
    unit_price text,
    gross_amount text,
    return_amount text,
    net_amount text,
    line_type text,
    is_cancellation text,
    record_hash text,
    batch_id text,
    created_at text
);

CREATE TABLE IF NOT EXISTS staging.events (
    event_id text,
    order_id text,
    source text,
    event_type text,
    event_time_utc text,
    event_time_original text,
    ingested_at text,
    is_late text,
    late_hours text,
    country_code text,
    currency_code text,
    amount_original text,
    record_hash text,
    batch_id text,
    created_at text,
    raw_payload text
);

CREATE TABLE IF NOT EXISTS staging.catalog (
    sku text,
    name text,
    category text,
    is_service text,
    is_active text,
    valid_from text,
    valid_to text,
    updated_at text,
    catalog_version text,
    record_hash text,
    batch_id text,
    created_at text
);

CREATE TABLE IF NOT EXISTS staging.quarantine (
    quarantine_key text,
    source text,
    source_record_id text,
    reason_code text,
    reason_detail text,
    raw_payload text,
    batch_id text,
    processed_at text,
    record_hash text
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id text,
    country_code text,
    batch_id text
);

-- ---------------------------
-- DIMENSIONES
-- ---------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key integer PRIMARY KEY,
    full_date date NOT NULL UNIQUE,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    weekday smallint NOT NULL,
    month_name text NOT NULL,
    is_weekend boolean NOT NULL
);

INSERT INTO analytics.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::integer,
    d,
    EXTRACT(YEAR FROM d)::smallint,
    EXTRACT(QUARTER FROM d)::smallint,
    EXTRACT(MONTH FROM d)::smallint,
    EXTRACT(DAY FROM d)::smallint,
    EXTRACT(ISODOW FROM d)::smallint,
    TO_CHAR(d, 'TMMonth'),
    EXTRACT(ISODOW FROM d) >= 6
FROM generate_series('2010-01-01'::date, '2027-12-31'::date, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_key serial PRIMARY KEY,
    country_code text NOT NULL UNIQUE,
    country_name text NOT NULL,
    region text NOT NULL,
    source_name text,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

INSERT INTO analytics.dim_country
    (country_key, country_code, country_name, region, source_name)
VALUES
    (0, 'ZZ', 'Unknown', 'Unknown', 'system')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_currency (
    currency_key serial PRIMARY KEY,
    currency_code text NOT NULL UNIQUE,
    currency_name text NOT NULL,
    is_reporting boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

INSERT INTO analytics.dim_currency
    (currency_key, currency_code, currency_name, is_reporting)
VALUES
    (0, 'XXX', 'Unknown', false)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key bigserial PRIMARY KEY,
    source text NOT NULL,
    source_customer_id text NOT NULL,
    country_key integer REFERENCES analytics.dim_country(country_key),
    is_unknown boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (source, source_customer_id)
);

INSERT INTO analytics.dim_customer
    (customer_key, source, source_customer_id, country_key, is_unknown)
VALUES
    (-1, 'system', 'UNKNOWN', 0, true)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key bigserial PRIMARY KEY,
    sku text NOT NULL,
    product_name text NOT NULL,
    category text NOT NULL,
    is_service boolean NOT NULL DEFAULT false,
    is_active boolean NOT NULL DEFAULT true,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    catalog_version integer NOT NULL,
    updated_at timestamptz NOT NULL,
    record_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (sku, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_product_sku_current
    ON analytics.dim_product (sku, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_product_sku_dates
    ON analytics.dim_product (sku, valid_from, valid_to);

INSERT INTO analytics.dim_product
    (product_key, sku, product_name, category, is_service, is_active,
     valid_from, valid_to, is_current, catalog_version, updated_at, record_hash)
VALUES
    (0, '__UNKNOWN__', 'Unknown', 'Unknown', false, false,
     '1900-01-01T00:00:00Z', '9999-12-31T23:59:59Z', true, 0, NOW(), 'unknown')
ON CONFLICT DO NOTHING;

-- ---------------------------
-- HECHOS
-- ---------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_order_line (
    line_key text PRIMARY KEY,
    invoice_no text NOT NULL,
    source text NOT NULL,
    product_key bigint NOT NULL REFERENCES analytics.dim_product(product_key),
    customer_key bigint NOT NULL REFERENCES analytics.dim_customer(customer_key),
    country_key integer NOT NULL REFERENCES analytics.dim_country(country_key),
    currency_key integer NOT NULL REFERENCES analytics.dim_currency(currency_key),
    date_key integer NOT NULL REFERENCES analytics.dim_date(date_key),
    invoice_date_utc timestamptz NOT NULL,
    quantity integer NOT NULL,
    unit_price numeric(18,4) NOT NULL,
    gross_amount numeric(18,2) NOT NULL,
    return_amount numeric(18,2) NOT NULL,
    net_amount numeric(18,2) NOT NULL,
    gross_amount_usd numeric(18,2),
    return_amount_usd numeric(18,2),
    net_amount_usd numeric(18,2),
    line_type text NOT NULL,
    is_cancellation boolean NOT NULL,
    record_hash text NOT NULL,
    batch_id text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_line_date ON analytics.fact_order_line (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_line_product ON analytics.fact_order_line (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_line_country ON analytics.fact_order_line (country_key);
CREATE INDEX IF NOT EXISTS idx_fact_line_invoice ON analytics.fact_order_line (invoice_no);

CREATE TABLE IF NOT EXISTS analytics.fact_order_event (
    event_key bigserial PRIMARY KEY,
    event_id text NOT NULL,
    order_id text NOT NULL,
    source text NOT NULL,
    event_type text NOT NULL,
    event_time_utc timestamptz NOT NULL,
    event_time_original text,
    ingested_at timestamptz NOT NULL,
    is_late boolean NOT NULL,
    late_hours numeric(18,2),
    country_key integer NOT NULL REFERENCES analytics.dim_country(country_key),
    currency_key integer NOT NULL REFERENCES analytics.dim_currency(currency_key),
    amount_original numeric(18,2) NOT NULL,
    amount_usd numeric(18,2),
    record_hash text NOT NULL,
    batch_id text NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (source, event_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_event_order ON analytics.fact_order_event (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_event_time ON analytics.fact_order_event (event_time_utc);
CREATE INDEX IF NOT EXISTS idx_fact_event_type ON analytics.fact_order_event (event_type);

CREATE TABLE IF NOT EXISTS analytics.fact_order_lifecycle (
    source text NOT NULL,
    order_id text NOT NULL,
    country_key integer REFERENCES analytics.dim_country(country_key),
    currency_key integer REFERENCES analytics.dim_currency(currency_key),
    created_at timestamptz,
    paid_at timestamptz,
    cancelled_at timestamptz,
    refunded_at timestamptz,
    first_event_at timestamptz NOT NULL,
    last_event_at timestamptz NOT NULL,
    current_status text NOT NULL,
    time_to_pay_hours numeric(18,2),
    was_cancelled boolean NOT NULL DEFAULT false,
    was_refunded boolean NOT NULL DEFAULT false,
    event_count integer NOT NULL,
    late_event_count integer NOT NULL DEFAULT 0,
    batch_id text,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source, order_id)
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_status
    ON analytics.fact_order_lifecycle (current_status);

-- ---------------------------
-- OPS: cuarentena, metricas, manifiestos, calidad, alertas, tasas
-- ---------------------------
CREATE TABLE IF NOT EXISTS ops.quarantine_records (
    quarantine_id bigserial PRIMARY KEY,
    quarantine_key text NOT NULL UNIQUE,
    source text NOT NULL,
    source_record_id text,
    reason_code text NOT NULL,
    reason_detail text,
    raw_payload jsonb NOT NULL,
    batch_id text NOT NULL,
    processed_at timestamptz NOT NULL,
    record_hash text
);

CREATE INDEX IF NOT EXISTS idx_quarantine_batch ON ops.quarantine_records (batch_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON ops.quarantine_records (reason_code);

CREATE TABLE IF NOT EXISTS ops.pipeline_run_metrics (
    metric_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source text NOT NULL,
    layer text NOT NULL,
    dag_run_id text,
    logical_date timestamptz,
    rows_read bigint NOT NULL DEFAULT 0,
    rows_valid bigint NOT NULL DEFAULT 0,
    rows_rejected bigint NOT NULL DEFAULT 0,
    rows_duplicated bigint NOT NULL DEFAULT 0,
    rows_late bigint NOT NULL DEFAULT 0,
    rows_published bigint NOT NULL DEFAULT 0,
    rows_conflicting bigint NOT NULL DEFAULT 0,
    started_at timestamptz,
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running',
    error_message text,
    UNIQUE (batch_id, source, layer)
);

CREATE TABLE IF NOT EXISTS ops.batch_manifest (
    manifest_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    source text NOT NULL,
    layer text NOT NULL,
    object_path text NOT NULL,
    row_count bigint,
    checksum text,
    status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (batch_id, source, layer)
);

CREATE TABLE IF NOT EXISTS ops.data_quality_results (
    check_id bigserial PRIMARY KEY,
    batch_id text NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL,
    observed_value text,
    threshold text,
    details text,
    checked_at timestamptz NOT NULL DEFAULT NOW(),
    UNIQUE (batch_id, check_name)
);

CREATE TABLE IF NOT EXISTS ops.alerts (
    alert_id bigserial PRIMARY KEY,
    alert_key text NOT NULL UNIQUE,
    severity text NOT NULL,
    category text NOT NULL,
    message text NOT NULL,
    batch_id text,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS ops.exchange_rates (
    rate_date date NOT NULL,
    from_currency text NOT NULL,
    to_currency text NOT NULL,
    rate numeric(18,8) NOT NULL,
    source_note text NOT NULL DEFAULT 'DEMO - no es cotizacion real',
    PRIMARY KEY (rate_date, from_currency, to_currency)
);

-- ---------------------------
-- VISTAS ANALITICAS
-- ---------------------------
CREATE OR REPLACE VIEW analytics.v_sales_movements AS
SELECT
    'historical'::text AS source,
    'sale'::text AS movement_type,
    l.date_key,
    l.product_key,
    l.country_key,
    l.currency_key,
    l.invoice_no AS order_id,
    l.gross_amount_usd AS gross_amount_usd,
    0::numeric AS return_amount_usd,
    l.net_amount_usd AS net_amount_usd,
    l.quantity AS quantity
FROM analytics.fact_order_line l
WHERE l.line_type = 'sale'
UNION ALL
SELECT
    'historical',
    'return',
    l.date_key,
    l.product_key,
    l.country_key,
    l.currency_key,
    l.invoice_no,
    0::numeric,
    l.return_amount_usd,
    l.net_amount_usd,
    l.quantity
FROM analytics.fact_order_line l
WHERE l.line_type = 'return'
UNION ALL
SELECT
    'events',
    'sale',
    d.date_key,
    NULL::bigint,
    e.country_key,
    e.currency_key,
    e.order_id,
    e.amount_usd,
    0::numeric,
    e.amount_usd,
    NULL::integer
FROM analytics.fact_order_event e
JOIN analytics.dim_date d ON d.full_date = e.event_time_utc::date
WHERE e.event_type = 'paid'
UNION ALL
SELECT
    'events',
    'return',
    d.date_key,
    NULL::bigint,
    e.country_key,
    e.currency_key,
    e.order_id,
    0::numeric,
    e.amount_usd,
    -e.amount_usd,
    NULL::integer
FROM analytics.fact_order_event e
JOIN analytics.dim_date d ON d.full_date = e.event_time_utc::date
WHERE e.event_type = 'refunded';

CREATE OR REPLACE VIEW analytics.v_product_history AS
SELECT
    product_key,
    sku,
    product_name,
    category,
    is_service,
    is_active,
    valid_from,
    valid_to,
    is_current,
    catalog_version,
    record_hash
FROM analytics.dim_product
WHERE sku <> '__UNKNOWN__';

CREATE OR REPLACE VIEW ops.v_pipeline_health AS
SELECT
    source,
    batch_id,
    layer,
    rows_read,
    rows_valid,
    rows_rejected,
    rows_duplicated,
    rows_late,
    rows_published,
    rows_conflicting,
    CASE WHEN rows_read > 0
         THEN ROUND(rows_rejected::numeric / rows_read, 4)
         ELSE 0 END AS rejection_rate,
    CASE WHEN rows_read > 0
         THEN ROUND(rows_duplicated::numeric / rows_read, 4)
         ELSE 0 END AS duplicate_rate,
    status,
    started_at,
    finished_at,
    error_message
FROM ops.pipeline_run_metrics
ORDER BY finished_at DESC NULLS LAST;
