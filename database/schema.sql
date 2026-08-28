-- Medallion pipeline reference schema (de_assessment catalog)
-- Authoritative DDL lives in bootstrap jobs; this file documents logical layout.

-- ---------------------------------------------------------------------------
-- Config
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS de_assessment.config.source_config (
  source_name STRING NOT NULL,
  target_table STRING NOT NULL,
  raw_path STRING NOT NULL,
  checkpoint_path STRING NOT NULL,
  schema_hint_path STRING NOT NULL,
  archive_path STRING,
  file_format STRING NOT NULL,
  delivery_pattern STRING NOT NULL,
  cdf_enabled BOOLEAN NOT NULL,
  schedule_hint STRING NOT NULL,
  is_active BOOLEAN NOT NULL,
  dq_schema VARIANT,
  updated_at TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Bronze (raw ingest + metadata; CDF enabled)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS de_assessment.bronze.customers (
  customer_id INT,
  customer_name STRING,
  email STRING,
  country STRING,
  signup_date DATE,
  customer_segment STRING,
  lifetime_value DECIMAL(18,2),
  _ingest_timestamp TIMESTAMP NOT NULL,
  _source_file STRING NOT NULL,
  _batch_id STRING NOT NULL,
  _delivery_pattern STRING NOT NULL,
  _rescued_data STRING,
  _row_hash STRING NOT NULL
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.bronze.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  product_id INT,
  quantity INT,
  unit_price DECIMAL(18,2),
  total_amount DECIMAL(18,2),
  order_status STRING,
  payment_date DATE,
  _ingest_timestamp TIMESTAMP NOT NULL,
  _source_file STRING NOT NULL,
  _batch_id STRING NOT NULL,
  _delivery_pattern STRING NOT NULL,
  _rescued_data STRING
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.bronze.products (
  product_id INT,
  product_name STRING,
  category STRING,
  price DECIMAL(18,2),
  cost DECIMAL(18,2),
  stock_quantity INT,
  reorder_level INT,
  _ingest_timestamp TIMESTAMP NOT NULL,
  _source_file STRING NOT NULL,
  _batch_id STRING NOT NULL,
  _delivery_pattern STRING NOT NULL,
  _rescued_data STRING
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Deprecated: bronze.ingest_manifest — replaced by ops.pipeline_manifest (layer=bronze)

-- ---------------------------------------------------------------------------
-- Silver (DQ enforcement; valid rows only; CDF on entity tables)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS de_assessment.silver.customers (
  customer_id INT,
  customer_name STRING,
  email STRING,
  country STRING,
  signup_date DATE,
  customer_segment STRING,
  lifetime_value DECIMAL(18,2),
  quality_check_result STRING NOT NULL,
  _row_hash STRING,
  _is_deleted BOOLEAN NOT NULL,
  -- Referential state: true while any foreign key of this row is unresolved.
  -- Set and cleared by refresh_orphan_flags from the data, in both directions.
  _is_orphan BOOLEAN NOT NULL,
  _silver_updated_at TIMESTAMP NOT NULL,
  _bronze_batch_id STRING NOT NULL
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.silver.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  product_id INT,
  quantity INT,
  unit_price DECIMAL(18,2),
  total_amount DECIMAL(18,2),
  order_status STRING,
  payment_date DATE,
  quality_check_result STRING NOT NULL,
  _row_hash STRING,
  _is_deleted BOOLEAN NOT NULL,
  -- Referential state: true while any foreign key of this row is unresolved.
  -- Set and cleared by refresh_orphan_flags from the data, in both directions.
  _is_orphan BOOLEAN NOT NULL,
  _silver_updated_at TIMESTAMP NOT NULL,
  _bronze_batch_id STRING NOT NULL
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.silver.products (
  product_id INT,
  product_name STRING,
  category STRING,
  price DECIMAL(18,2),
  cost DECIMAL(18,2),
  stock_quantity INT,
  reorder_level INT,
  quality_check_result STRING NOT NULL,
  _row_hash STRING,
  _is_deleted BOOLEAN NOT NULL,
  -- Referential state: true while any foreign key of this row is unresolved.
  -- Set and cleared by refresh_orphan_flags from the data, in both directions.
  _is_orphan BOOLEAN NOT NULL,
  _silver_updated_at TIMESTAMP NOT NULL,
  _bronze_batch_id STRING NOT NULL
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.silver.quarantine (
  entity_name STRING NOT NULL,
  primary_key STRING NOT NULL,
  data STRING NOT NULL,
  violations ARRAY<STRUCT<
    category: STRING,
    rule: STRING,
    column: STRING,
    value: STRING
  >> NOT NULL,
  quarantined_at TIMESTAMP NOT NULL,
  silver_run_id STRING NOT NULL,
  bronze_batch_id STRING
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS de_assessment.silver.dq_metrics (
  silver_run_id STRING NOT NULL,
  entity_name STRING NOT NULL,
  check_category STRING NOT NULL,
  rows_evaluated BIGINT NOT NULL,
  rows_passed BIGINT NOT NULL,
  rows_quarantined BIGINT NOT NULL,
  pass_pct DOUBLE NOT NULL,
  run_at TIMESTAMP NOT NULL
);

-- ---------------------------------------------------------------------------
-- Ops (checkpoints + unified run telemetry)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS de_assessment.ops.pipeline_manifest (
  run_id STRING NOT NULL,
  layer STRING NOT NULL,
  entity_name STRING NOT NULL,
  parent_run_id STRING,
  delivery_pattern STRING,
  source_path STRING,
  files_processed INT NOT NULL,
  rows_read BIGINT NOT NULL,
  rows_written BIGINT NOT NULL,
  rows_quarantined BIGINT NOT NULL,
  rows_rescued BIGINT NOT NULL,
  delta_version_before BIGINT,
  delta_version_after BIGINT,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  status STRING NOT NULL,
  error_message STRING
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ---------------------------------------------------------------------------
-- Gold (business analytics derived tables, rebuilt via CTAS each run)
-- ---------------------------------------------------------------------------
-- Gold tables are rebuilt by CREATE OR REPLACE TABLE ... AS SELECT each run.
-- The DDL below documents the produced shape and is guard-tested against
-- execution (see databricks/jobs/gold/tests/test_schema_sql_drift.py).

CREATE TABLE de_assessment.gold.sales_by_product (
  product_id INT,
  product_name STRING,
  category STRING,
  total_orders BIGINT,
  total_revenue DECIMAL(18, 2),
  avg_order_value DECIMAL(18, 2)
);

CREATE TABLE de_assessment.gold.revenue_by_customer (
  customer_id INT,
  customer_name STRING,
  customer_segment STRING,
  total_orders BIGINT,
  total_revenue DECIMAL(18, 2),
  avg_order_value DECIMAL(18, 2),
  lifetime_value_actual DECIMAL(18, 2),
  last_order_date DATE
);

CREATE TABLE de_assessment.gold.daily_weekly_trends (
  order_date DATE,
  week_start DATE,
  total_orders BIGINT,
  total_revenue DECIMAL(18, 2),
  avg_order_value DECIMAL(18, 2)
);

CREATE TABLE de_assessment.gold.customer_segmentation (
  segment_type STRING,
  customer_count BIGINT,
  avg_revenue DECIMAL(18, 2),
  total_revenue DECIMAL(18, 2)
);
