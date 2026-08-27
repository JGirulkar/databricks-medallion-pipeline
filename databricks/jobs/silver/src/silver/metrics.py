"""Per-run DQ metrics by check category."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from silver.config import DEFAULT_CATALOG, dq_metrics_table
from silver.schemas import DQ_METRICS_SCHEMA


def append_dq_metrics(
    spark: SparkSession,
    rows: list[dict[str, object]],
    catalog: str = DEFAULT_CATALOG,
) -> None:
    if not rows:
        return
    metrics_df = spark.createDataFrame(rows, schema=DQ_METRICS_SCHEMA)
    metrics_df.write.format("delta").mode("append").saveAsTable(dq_metrics_table(catalog))


def build_metric_row(
    silver_run_id: str,
    entity_name: str,
    check_category: str,
    rows_evaluated: int,
    rows_passed: int,
    rows_quarantined: int,
    run_at: datetime,
) -> dict[str, object]:
    pass_pct = 0.0
    if rows_evaluated > 0:
        pass_pct = round(rows_passed / rows_evaluated * 100.0, 4)
    return {
        "silver_run_id": silver_run_id,
        "entity_name": entity_name,
        "check_category": check_category,
        "rows_evaluated": rows_evaluated,
        "rows_passed": rows_passed,
        "rows_quarantined": rows_quarantined,
        "pass_pct": pass_pct,
        "run_at": run_at,
    }
