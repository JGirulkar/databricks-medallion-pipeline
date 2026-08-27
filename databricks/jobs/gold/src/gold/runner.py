"""Gold runner: one qualifying_orders view, four SQL files, one manifest row.

Full recompute per run — every table is atomically replaced from current
silver (per-table atomicity guaranteed by CREATE OR REPLACE). A mid-run failure
leaves a mixed state (completed tables from before the failure, previous
versions of tables not yet processed). The business rule (what counts as
revenue) is defined exactly once here and interpolated into both the view and
the input breakdown filter.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from gold.config import (
    DEFAULT_CATALOG,
    GOLD_SCHEMA,
    GOLD_SQL_FILES,
    GOLD_TABLES,
    SILVER_SCHEMA,
    load_sql,
    pipeline_manifest_table,
    render_sql,
)
from gold.job_log import configure_job_logger
from gold.manifest import GoldManifestRecord, append_gold_manifest

LOG = configure_job_logger("gold.runner")

# The single definition of what counts as a qualifying order (completed, not orphan, not deleted).
# Interpolated into both the view and the input breakdown to enforce one-rule-one-place.
QUALIFYING_PREDICATE = "order_status = 'Completed' AND NOT _is_orphan AND NOT _is_deleted"

QUALIFYING_ORDERS_VIEW = f"""
CREATE OR REPLACE TEMPORARY VIEW qualifying_orders AS
SELECT order_id, customer_id, product_id, order_date, total_amount
FROM {{silver}}.orders
WHERE {QUALIFYING_PREDICATE}
"""

INPUT_BREAKDOWN = f"""
SELECT
  COUNT(*) AS rows_total,
  COUNT_IF({QUALIFYING_PREDICATE}) AS rows_qualifying,
  COUNT_IF(order_status = 'Pending') AS rows_pending,
  COUNT_IF(order_status = 'Cancelled') AS rows_cancelled,
  COUNT_IF(_is_orphan) AS rows_orphan,
  COUNT_IF(_is_deleted) AS rows_deleted
FROM {{silver}}.orders
"""


def run_gold(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    *,
    silver_schema: str | None = None,
    gold_schema: str | None = None,
    manifest_table: str | None = None,
) -> str:
    silver = silver_schema or f"{catalog}.{SILVER_SCHEMA}"
    gold = gold_schema or f"{catalog}.{GOLD_SCHEMA}"
    manifest = manifest_table or pipeline_manifest_table(catalog)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    breakdown = None
    files_executed = 0

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")
        breakdown = spark.sql(INPUT_BREAKDOWN.format(silver=silver)).collect()[0]
        LOG.info(
            "gold_input run_id=%s total=%s qualifying=%s pending=%s "
            "cancelled=%s orphan=%s deleted=%s",
            run_id,
            breakdown["rows_total"],
            breakdown["rows_qualifying"],
            breakdown["rows_pending"],
            breakdown["rows_cancelled"],
            breakdown["rows_orphan"],
            breakdown["rows_deleted"],
        )
        spark.sql(QUALIFYING_ORDERS_VIEW.format(silver=silver))
        rows_written = 0
        for filename, table in zip(GOLD_SQL_FILES, GOLD_TABLES):
            spark.sql(render_sql(load_sql(filename), silver=silver, gold=gold))
            count = spark.table(f"{gold}.{table}").count()
            rows_written += count
            files_executed += 1
            LOG.info("gold_table run_id=%s table=%s rows=%s", run_id, table, count)
    except Exception as exc:
        # Write failed manifest row with actual progress: files executed and rows read
        # (if breakdown succeeded before the failure).
        rows_read = int(breakdown["rows_total"]) if breakdown is not None else 0
        append_gold_manifest(
            spark,
            GoldManifestRecord(
                run_id=run_id,
                files_processed=files_executed,
                rows_read=rows_read,
                rows_written=0,
                started_at=started_at,
                completed_at=None,
                status="failed",
                error_message=str(exc)[:1024],
            ),
            manifest,
        )
        raise

    append_gold_manifest(
        spark,
        GoldManifestRecord(
            run_id=run_id,
            files_processed=len(GOLD_SQL_FILES),
            rows_read=int(breakdown["rows_total"]),
            rows_written=rows_written,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            status="success",
        ),
        manifest,
    )
    return run_id
