"""Resolve silver manifest metrics from Delta history and quarantine (driver-side).

On serverless Spark Connect, foreachBatch callbacks run in workers — a driver
``totals`` dict is not updated. Mirror bronze ``metrics_from_sink``: read MERGE
commits and quarantine rows after the stream completes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.config import DEFAULT_CATALOG, quarantine_table, silver_table
from silver.job_log import configure_job_logger

LOG = configure_job_logger("silver.sink_metrics")


@dataclass(frozen=True)
class SilverSinkMetrics:
    rows_read: int
    rows_written: int
    rows_quarantined: int

    @staticmethod
    def empty() -> "SilverSinkMetrics":
        return SilverSinkMetrics(rows_read=0, rows_written=0, rows_quarantined=0)


def _parse_operation_metrics(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def merge_metrics_from_history(
    spark: SparkSession,
    table_name: str,
    version_before: int | None,
    version_after: int | None,
) -> tuple[int, int]:
    """Return (rows_read, rows_written) from MERGE/WRITE commits in the version window."""
    if (
        version_before is None
        or version_after is None
        or version_after <= version_before
    ):
        return 0, 0

    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    commits = history.filter(
        (F.col("operation").isin("MERGE", "WRITE"))
        & (F.col("version") > F.lit(version_before))
        & (F.col("version") <= F.lit(version_after))
    ).collect()

    rows_written = 0
    rows_read = 0
    for row in commits:
        metrics = _parse_operation_metrics(row.operationMetrics)
        rows_written += int(metrics.get("numTargetRowsInserted", 0))
        rows_written += int(metrics.get("numTargetRowsUpdated", 0))
        rows_written += int(metrics.get("numOutputRows", 0))
        source_rows = int(metrics.get("numSourceRows", 0))
        if source_rows:
            rows_read += source_rows
        else:
            rows_read += int(metrics.get("numOutputRows", 0))

    if rows_written == 0 and version_after > version_before:
        LOG.warning(
            "history_merge_metrics_empty table=%s version_before=%s version_after=%s",
            table_name,
            version_before,
            version_after,
        )
    return rows_read, rows_written


def quarantine_rows_for_run(
    spark: SparkSession,
    run_id: str,
    catalog: str = DEFAULT_CATALOG,
) -> int:
    return (
        spark.table(quarantine_table(catalog))
        .filter(F.col("silver_run_id") == run_id)
        .count()
    )


def resolve_silver_metrics(
    spark: SparkSession,
    entity_name: str,
    run_id: str,
    catalog: str,
    version_before: int | None,
    version_after: int | None,
) -> SilverSinkMetrics:
    target = silver_table(entity_name, catalog)
    rows_read, rows_written = merge_metrics_from_history(
        spark, target, version_before, version_after
    )
    rows_quarantined = quarantine_rows_for_run(spark, run_id, catalog)
    if rows_read == 0 and (rows_written > 0 or rows_quarantined > 0):
        rows_read = rows_written + rows_quarantined
    LOG.info(
        "metrics_from_sink entity=%s run_id=%s rows_read=%s rows_written=%s "
        "rows_quarantined=%s version_before=%s version_after=%s",
        entity_name,
        run_id,
        rows_read,
        rows_written,
        rows_quarantined,
        version_before,
        version_after,
    )
    return SilverSinkMetrics(
        rows_read=rows_read,
        rows_written=rows_written,
        rows_quarantined=rows_quarantined,
    )
