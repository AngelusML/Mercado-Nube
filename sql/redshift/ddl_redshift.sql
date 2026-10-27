-- ============================================================
-- Mercado Nube - DDL para Amazon Redshift Serverless
--
-- Mismo modelo dimensional que sql/ddl.sql (PostgreSQL local), traducido a
-- Redshift. Diferencias principales:
--   * serial/bigserial -> BIGINT/INTEGER: las claves sustitutas se generan en
--     la capa de transformacion y llegan en el Parquet de curated/, asi los
--     hechos y dimensiones conservan las mismas claves que en local.
--   * jsonb -> SUPER (payload de cuarentena).
--   * ON CONFLICT -> MERGE (ver sql/redshift/load_from_s3.sql).
--   * PRIMARY KEY / UNIQUE / FOREIGN KEY son informativas en Redshift (no se
--     hacen cumplir): la unicidad la garantiza el MERGE y el quality gate.
--   * Dimensiones pequenas con DISTSTYLE ALL (joins locales sin redistribuir).
--   * Hechos con DISTKEY por la clave de mayor cardinalidad que se agrupa y
--     SORTKEY por fecha (filtros por rango de tiempo).
-- Ejecutar una vez con el editor de consultas de Redshift (Query Editor v2).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------
-- DIMENSIONES
-- ---------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key    INTEGER     NOT NULL,
    full_date   DATE        NOT NULL,
    year        SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL,
    weekday     SMALLINT    NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    is_weekend  BOOLEAN     NOT NULL,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

CREATE TABLE IF NOT EXISTS analytics.dim_country (
    country_key  INTEGER      NOT NULL,
    country_code VARCHAR(2)   NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region       VARCHAR(50)  NOT NULL,
    source_name  VARCHAR(50),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (country_key),
    UNIQUE (country_code)
)
DISTSTYLE ALL
SORTKEY (country_key);

CREATE TABLE IF NOT EXISTS analytics.dim_currency (
    currency_key  INTEGER     NOT NULL,
    currency_code VARCHAR(3)  NOT NULL,
    currency_name VARCHAR(60) NOT NULL,
    is_reporting  BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (currency_key),
    UNIQUE (currency_code)
)
DISTSTYLE ALL
SORTKEY (currency_key);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key       BIGINT      NOT NULL,
    source             VARCHAR(32) NOT NULL,
    source_customer_id VARCHAR(64) NOT NULL,
    country_key        INTEGER,
    is_unknown         BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (customer_key),
    UNIQUE (source, source_customer_id)
)
DISTSTYLE ALL
SORTKEY (customer_key);

-- SCD2: una fila por version de atributos (valid_from / valid_to / is_current).
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key     BIGINT       NOT NULL,
    sku             VARCHAR(64)  NOT NULL,
    product_name    VARCHAR(256) NOT NULL,
    category        VARCHAR(64)  NOT NULL,
    is_service      BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    valid_from      TIMESTAMPTZ  NOT NULL,
    valid_to        TIMESTAMPTZ  NOT NULL,
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,
    catalog_version INTEGER      NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL,
    record_hash     VARCHAR(64)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (product_key),
    UNIQUE (sku, valid_from)
)
DISTSTYLE ALL
SORTKEY (sku, valid_from);

-- ---------------------------
-- HECHOS
-- ---------------------------
-- Grano: una linea de factura del historico.
CREATE TABLE IF NOT EXISTS analytics.fact_order_line (
    line_key          VARCHAR(64)   NOT NULL,
    invoice_no        VARCHAR(20)   NOT NULL,
    source            VARCHAR(32)   NOT NULL,
    product_key       BIGINT        NOT NULL REFERENCES analytics.dim_product (product_key),
    customer_key      BIGINT        NOT NULL REFERENCES analytics.dim_customer (customer_key),
    country_key       INTEGER       NOT NULL REFERENCES analytics.dim_country (country_key),
    currency_key      INTEGER       NOT NULL REFERENCES analytics.dim_currency (currency_key),
    date_key          INTEGER       NOT NULL REFERENCES analytics.dim_date (date_key),
    invoice_date_utc  TIMESTAMPTZ   NOT NULL,
    quantity          INTEGER       NOT NULL,
    unit_price        DECIMAL(18,4) NOT NULL,
    gross_amount      DECIMAL(18,2) NOT NULL,
    return_amount     DECIMAL(18,2) NOT NULL,
    net_amount        DECIMAL(18,2) NOT NULL,
    gross_amount_usd  DECIMAL(18,2),
    return_amount_usd DECIMAL(18,2),
    net_amount_usd    DECIMAL(18,2),
    line_type         VARCHAR(10)   NOT NULL,
    is_cancellation   BOOLEAN       NOT NULL,
    record_hash       VARCHAR(64)   NOT NULL,
    batch_id          VARCHAR(64)   NOT NULL,
    created_at        TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (line_key)
)
DISTKEY (invoice_no)
COMPOUND SORTKEY (date_key, product_key);

-- Grano: un evento de cambio de estado de pedido.
CREATE TABLE IF NOT EXISTS analytics.fact_order_event (
    event_key           BIGINT        NOT NULL,
    event_id            VARCHAR(128)  NOT NULL,
    order_id            VARCHAR(64)   NOT NULL,
    source              VARCHAR(32)   NOT NULL,
    event_type          VARCHAR(20)   NOT NULL,
    event_time_utc      TIMESTAMPTZ   NOT NULL,
    event_time_original VARCHAR(64),
    ingested_at         TIMESTAMPTZ   NOT NULL,
    is_late             BOOLEAN       NOT NULL,
    late_hours          DECIMAL(18,2),
    country_key         INTEGER       NOT NULL REFERENCES analytics.dim_country (country_key),
    currency_key        INTEGER       NOT NULL REFERENCES analytics.dim_currency (currency_key),
    amount_original     DECIMAL(18,2) NOT NULL,
    amount_usd          DECIMAL(18,2),
    record_hash         VARCHAR(64)   NOT NULL,
    batch_id            VARCHAR(64)   NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (event_key),
    UNIQUE (source, event_id)
)
DISTKEY (order_id)
SORTKEY (event_time_utc);

-- Grano: un pedido (estado actual derivado de todo su historial de eventos).
CREATE TABLE IF NOT EXISTS analytics.fact_order_lifecycle (
    source            VARCHAR(32)   NOT NULL,
    order_id          VARCHAR(64)   NOT NULL,
    country_key       INTEGER REFERENCES analytics.dim_country (country_key),
    currency_key      INTEGER REFERENCES analytics.dim_currency (currency_key),
    created_at        TIMESTAMPTZ,
    paid_at           TIMESTAMPTZ,
    cancelled_at      TIMESTAMPTZ,
    refunded_at       TIMESTAMPTZ,
    first_event_at    TIMESTAMPTZ   NOT NULL,
    last_event_at     TIMESTAMPTZ   NOT NULL,
    current_status    VARCHAR(20)   NOT NULL,
    time_to_pay_hours DECIMAL(18,2),
    was_cancelled     BOOLEAN       NOT NULL DEFAULT FALSE,
    was_refunded      BOOLEAN       NOT NULL DEFAULT FALSE,
    event_count       INTEGER       NOT NULL,
    late_event_count  INTEGER       NOT NULL DEFAULT 0,
    batch_id          VARCHAR(64),
    updated_at        TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (source, order_id)
)
DISTKEY (order_id)
SORTKEY (first_event_at);

-- ---------------------------
-- OPS (operacion y calidad)
-- ---------------------------
CREATE TABLE IF NOT EXISTS ops.quarantine_records (
    quarantine_key   VARCHAR(64)  NOT NULL,
    source           VARCHAR(32)  NOT NULL,
    source_record_id VARCHAR(128),
    reason_code      VARCHAR(64)  NOT NULL,
    reason_detail    VARCHAR(1024),
    raw_payload      SUPER        NOT NULL,
    batch_id         VARCHAR(64)  NOT NULL,
    processed_at     TIMESTAMPTZ  NOT NULL,
    record_hash      VARCHAR(64),
    PRIMARY KEY (quarantine_key)
)
DISTSTYLE AUTO
SORTKEY (batch_id, reason_code);

CREATE TABLE IF NOT EXISTS ops.pipeline_run_metrics (
    batch_id          VARCHAR(64) NOT NULL,
    source            VARCHAR(32) NOT NULL,
    layer             VARCHAR(32) NOT NULL,
    rows_read         BIGINT      NOT NULL DEFAULT 0,
    rows_valid        BIGINT      NOT NULL DEFAULT 0,
    rows_rejected     BIGINT      NOT NULL DEFAULT 0,
    rows_duplicated   BIGINT      NOT NULL DEFAULT 0,
    rows_late         BIGINT      NOT NULL DEFAULT 0,
    rows_published    BIGINT      NOT NULL DEFAULT 0,
    rows_conflicting  BIGINT      NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(20) NOT NULL,
    error_message     VARCHAR(2048),
    PRIMARY KEY (batch_id, source, layer)
)
DISTSTYLE ALL
SORTKEY (batch_id);

CREATE TABLE IF NOT EXISTS ops.exchange_rates (
    rate_date     DATE          NOT NULL,
    from_currency VARCHAR(3)    NOT NULL,
    to_currency   VARCHAR(3)    NOT NULL,
    rate          DECIMAL(18,8) NOT NULL,
    source_note   VARCHAR(100)  NOT NULL DEFAULT 'DEMO - no es cotizacion real',
    PRIMARY KEY (rate_date, from_currency, to_currency)
)
DISTSTYLE ALL
SORTKEY (from_currency, rate_date);

-- ---------------------------
-- STAGING: espejo exacto de los Parquet de curated/ (COPY mapea por posicion
-- y tipo de Parquet; los double se convierten a DECIMAL en el MERGE).
-- ---------------------------
CREATE TABLE IF NOT EXISTS staging.stg_fact_order_line (
    line_key VARCHAR(64), invoice_no VARCHAR(20), source VARCHAR(32),
    product_key BIGINT, customer_key BIGINT, country_key BIGINT,
    currency_key BIGINT, date_key BIGINT, invoice_date_utc TIMESTAMPTZ,
    quantity BIGINT, unit_price DOUBLE PRECISION, gross_amount DOUBLE PRECISION,
    return_amount DOUBLE PRECISION, net_amount DOUBLE PRECISION,
    gross_amount_usd DOUBLE PRECISION, return_amount_usd DOUBLE PRECISION,
    net_amount_usd DOUBLE PRECISION, line_type VARCHAR(10),
    is_cancellation BOOLEAN, record_hash VARCHAR(64), batch_id VARCHAR(64),
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS staging.stg_fact_order_event (
    event_key BIGINT, event_id VARCHAR(128), order_id VARCHAR(64),
    source VARCHAR(32), event_type VARCHAR(20), event_time_utc TIMESTAMPTZ,
    event_time_original VARCHAR(64), ingested_at TIMESTAMPTZ, is_late BOOLEAN,
    late_hours DOUBLE PRECISION, country_key BIGINT, currency_key BIGINT,
    amount_original DOUBLE PRECISION, amount_usd DOUBLE PRECISION,
    record_hash VARCHAR(64), batch_id VARCHAR(64), created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS staging.stg_fact_order_lifecycle (
    source VARCHAR(32), order_id VARCHAR(64), country_key BIGINT,
    currency_key BIGINT, created_at TIMESTAMPTZ, paid_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ, refunded_at TIMESTAMPTZ,
    first_event_at TIMESTAMPTZ, last_event_at TIMESTAMPTZ,
    current_status VARCHAR(20), time_to_pay_hours DOUBLE PRECISION,
    was_cancelled BOOLEAN, was_refunded BOOLEAN, event_count BIGINT,
    late_event_count BIGINT, batch_id VARCHAR(64), updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS staging.stg_dim_product (
    product_key BIGINT, sku VARCHAR(64), product_name VARCHAR(256),
    category VARCHAR(64), is_service BOOLEAN, is_active BOOLEAN,
    valid_from TIMESTAMPTZ, valid_to TIMESTAMPTZ, is_current BOOLEAN,
    catalog_version BIGINT, updated_at TIMESTAMPTZ, record_hash VARCHAR(64),
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS staging.stg_dim_customer (
    customer_key BIGINT, source VARCHAR(32), source_customer_id VARCHAR(64),
    country_key BIGINT, is_unknown BOOLEAN, created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- ---------------------------
-- Calendario 2010-2027 (misma regla de clave YYYYMMDD que en local)
-- ---------------------------
INSERT INTO analytics.dim_date
WITH RECURSIVE days (d) AS (
    SELECT CAST('2010-01-01' AS DATE)
    UNION ALL
    SELECT CAST(DATEADD(day, 1, d) AS DATE) FROM days WHERE d < CAST('2027-12-31' AS DATE)
)
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER),
    d,
    CAST(DATE_PART(year, d) AS SMALLINT),
    CAST(DATE_PART(quarter, d) AS SMALLINT),
    CAST(DATE_PART(month, d) AS SMALLINT),
    CAST(DATE_PART(day, d) AS SMALLINT),
    CAST(CASE WHEN DATE_PART(dow, d) = 0 THEN 7 ELSE DATE_PART(dow, d) END AS SMALLINT),
    TRIM(TO_CHAR(d, 'Month')),
    DATE_PART(dow, d) IN (0, 6)
FROM days
WHERE NOT EXISTS (SELECT 1 FROM analytics.dim_date);

-- ---------------------------
-- VISTAS ANALITICAS (identicas a las de PostgreSQL)
-- ---------------------------
CREATE OR REPLACE VIEW analytics.v_sales_movements AS
SELECT 'historical'::VARCHAR AS source, 'sale'::VARCHAR AS movement_type,
       l.date_key, l.product_key, l.country_key, l.currency_key,
       l.invoice_no AS order_id, l.gross_amount_usd, 0::DECIMAL(18,2) AS return_amount_usd,
       l.net_amount_usd, l.quantity
FROM analytics.fact_order_line l
WHERE l.line_type = 'sale'
UNION ALL
SELECT 'historical', 'return', l.date_key, l.product_key, l.country_key, l.currency_key,
       l.invoice_no, 0::DECIMAL(18,2), l.return_amount_usd, l.net_amount_usd, l.quantity
FROM analytics.fact_order_line l
WHERE l.line_type = 'return'
UNION ALL
SELECT 'events', 'sale', d.date_key, NULL::BIGINT, e.country_key, e.currency_key,
       e.order_id, e.amount_usd, 0::DECIMAL(18,2), e.amount_usd, NULL::INTEGER
FROM analytics.fact_order_event e
JOIN analytics.dim_date d ON d.full_date = TRUNC(e.event_time_utc)
WHERE e.event_type = 'paid'
UNION ALL
SELECT 'events', 'return', d.date_key, NULL::BIGINT, e.country_key, e.currency_key,
       e.order_id, 0::DECIMAL(18,2), e.amount_usd, -e.amount_usd, NULL::INTEGER
FROM analytics.fact_order_event e
JOIN analytics.dim_date d ON d.full_date = TRUNC(e.event_time_utc)
WHERE e.event_type = 'refunded';

CREATE OR REPLACE VIEW analytics.v_product_history AS
SELECT product_key, sku, product_name, category, is_service, is_active,
       valid_from, valid_to, is_current, catalog_version, record_hash
FROM analytics.dim_product
WHERE sku <> '__UNKNOWN__';
