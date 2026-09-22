-- =====================================================================
-- STEP 1: FAST INGESTION SCHEMA
-- Optimized for high-throughput batch loading with zero index overhead.
-- =====================================================================

CREATE SEQUENCE IF NOT EXISTS seq_payer_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_code_id START 1;

-- ---------------------------------------------------------------------
-- Dimension: Payers / Plans
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payers (
    payer_id               BIGINT PRIMARY KEY DEFAULT nextval('seq_payer_id'),
    reporting_entity_name  VARCHAR,
    reporting_entity_type  VARCHAR,
    plan_name              VARCHAR,
    plan_id                VARCHAR,
    plan_id_type           VARCHAR,
    plan_market_type       VARCHAR,
    last_updated_on        VARCHAR,
    version                VARCHAR,
    source_file            VARCHAR
);
-- Idempotent migration for existing databases created before this column existed.
ALTER TABLE payers ADD COLUMN IF NOT EXISTS version VARCHAR;

-- ---------------------------------------------------------------------
-- Dimension: Billing Codes / Procedures
-- Note: Constraints removed for maximum insert speed. Deduplication is
-- managed upstream in stream_parser via code_cache.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing_codes (
    code_id                    BIGINT PRIMARY KEY DEFAULT nextval('seq_code_id'),
    billing_code               VARCHAR NOT NULL,
    billing_code_type          VARCHAR NOT NULL,
    billing_code_type_version  VARCHAR,
    description                VARCHAR,
    name                       VARCHAR,
    negotiation_arrangement    VARCHAR
);
-- Idempotent migration for existing databases created before this column existed.
ALTER TABLE billing_codes ADD COLUMN IF NOT EXISTS name VARCHAR;

-- ---------------------------------------------------------------------
-- Fact Table: Negotiated Rates
-- Unconstrained table optimized for append-only streaming writes.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS negotiated_rates (
    payer_id                 BIGINT,
    code_id                  BIGINT,
    negotiation_arrangement  VARCHAR,
    billing_class            VARCHAR,
    setting                  VARCHAR,
    negotiated_type          VARCHAR,
    negotiated_rate          DOUBLE,
    service_code             VARCHAR[],
    billing_code_modifier    VARCHAR[],
    expiration_date          VARCHAR,
    provider_reference_ids   BIGINT[],
    source_file              VARCHAR,
    ingested_at              TIMESTAMP DEFAULT current_timestamp
);

-- ---------------------------------------------------------------------
-- Optional: Provider mappings
--
-- provider_reference_id is either:
--   - a real CMS-assigned group id (from the file's top-level
--     `provider_references` array), or
--   - a synthetic negative id we mint for provider_groups that are
--     embedded inline on a negotiated_rate with no id of their own.
-- group_key is a content hash used only for de-duplicating embedded
-- groups across parser runs/files; it is not meaningful downstream.
-- One row is stored per NPI in a group, so a group with 3 NPIs is 3 rows
-- sharing the same provider_reference_id.
-- ---------------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS seq_provider_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_synthetic_provider_ref START 1;
CREATE TABLE IF NOT EXISTS providers (
    provider_id            BIGINT PRIMARY KEY DEFAULT nextval('seq_provider_id'),
    provider_reference_id  BIGINT,
    npi                    BIGINT,
    tin_type               VARCHAR, 
    tin_value              VARCHAR,
    facility_name          VARCHAR,
    network_name           VARCHAR[],
    group_key               VARCHAR
);
-- Idempotent migrations for existing databases created before these columns existed.
ALTER TABLE providers ADD COLUMN IF NOT EXISTS group_key VARCHAR;
ALTER TABLE providers ADD COLUMN IF NOT EXISTS network_name VARCHAR[];

-- ---------------------------------------------------------------------
-- Basic Analytical Views (available immediately)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW procedure_price_stats AS
SELECT
    bc.billing_code,
    bc.billing_code_type,
    bc.description,
    COUNT(*)                         AS n_rate_entries,
    COUNT(DISTINCT r.payer_id)       AS n_payers,
    MIN(r.negotiated_rate)           AS min_rate,
    MAX(r.negotiated_rate)           AS max_rate,
    ROUND(AVG(r.negotiated_rate), 2) AS avg_rate,
    MEDIAN(r.negotiated_rate)        AS median_rate
FROM negotiated_rates r
JOIN billing_codes bc USING (code_id)
WHERE r.negotiated_type = 'negotiated'
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW procedure_price_by_payer AS
SELECT
    bc.billing_code,
    p.reporting_entity_name          AS payer,
    COUNT(*)                         AS n_rate_entries,
    ROUND(AVG(r.negotiated_rate), 2) AS avg_rate,
    MEDIAN(r.negotiated_rate)        AS median_rate
FROM negotiated_rates r
JOIN billing_codes bc USING (code_id)
JOIN payers p USING (payer_id)
WHERE r.negotiated_type = 'negotiated'
GROUP BY 1, 2;