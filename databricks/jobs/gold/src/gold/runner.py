"""Gold runner: one qualifying_orders view, four SQL files, one manifest row.

Full recompute per run — every table is atomically replaced from current
silver, so any run from any silver state is correct and a failed run leaves
the previous version intact. The business rule (what counts as revenue)
exists exactly once: the view defined here.
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

QUALIFYING_ORDERS_VIEW = """
CREATE OR REPLACE TEMPORARY VIEW qualifying_orders AS
SELECT order_id, customer_id, product_id, order_date, total_amount
FROM {silver}.orders
WHERE order_status = 'Completed'
  AND NOT _is_orphan
  AND NOT _is_deleted
"""

INPUT_BREAKDOWN = """
SELECT
  COUNT(*) AS rows_total,
  COUNT_IF(order_status = 'Completed' AND NOT _is_orphan AND NOT _is_deleted) AS rows_qualifying,
  COUNT_IF(order_status = 'Pending') AS rows_pending,
  COUNT_IF(order_status = 'Cancelled') AS rows_cancelled,
  COUNT_IF(_is_orphan) AS rows_orphan,
  COUNT_IF(_is_deleted) AS rows_deleted
FROM {silver}.orders
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
            LOG.info("gold_table run_id=%s table=%s rows=%s", run_id, table, count)
    except Exception as exc:
        append_gold_manifest(
            spark,
            GoldManifestRecord(
                run_id=run_id,
                files_processed=len(GOLD_SQL_FILES),
                rows_read=0,
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
