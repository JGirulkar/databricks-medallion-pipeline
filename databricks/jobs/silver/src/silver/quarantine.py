"""Append failing rows to silver.quarantine."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.config import DEFAULT_CATALOG, quarantine_table


def entity_primary_key_column(entity_name: str) -> str:
    mapping = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
    }
    try:
        return mapping[entity_name]
    except KeyError as exc:
        raise ValueError(f"Unknown entity: {entity_name}") from exc


def write_quarantine(
    spark: SparkSession,
    df: DataFrame,
    entity_name: str,
    run_id: str,
    quarantined_at: datetime,
    catalog: str = DEFAULT_CATALOG,
) -> int:
    """Append quarantine rows; return count written."""
    if not df.take(1):
        return 0

    pk_col = entity_primary_key_column(entity_name)
    source_cols = [
        c for c in df.columns if c != "_violations" and not c.startswith("_change")
    ]
    quarantine_df = (
        df.withColumn("entity_name", F.lit(entity_name))
        .withColumn("primary_key", F.col(pk_col).cast("string"))
        .withColumn(
            "data",
            F.to_json(F.struct(*[F.col(c) for c in source_cols])),
        )
        .withColumn("violations", F.col("_violations"))
        .withColumn("quarantined_at", F.lit(quarantined_at))
        .withColumn("silver_run_id", F.lit(run_id))
        .withColumn("bronze_batch_id", F.col("_batch_id"))
        .select(
            "entity_name",
            "primary_key",
            "data",
            "violations",
            "quarantined_at",
            "silver_run_id",
            "bronze_batch_id",
        )
    )
    count = quarantine_df.count()
    quarantine_df.write.format("delta").mode("append").saveAsTable(
        quarantine_table(catalog)
    )
    return count
