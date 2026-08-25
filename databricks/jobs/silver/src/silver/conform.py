"""Bronze CDF batch conform — snapshot/incremental merge semantics."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.config import (
    DEFAULT_CATALOG,
    ENTITY_PK,
    PRODUCT_HASH_COLUMNS,
    SNAPSHOT_ENTITIES,
    silver_table,
)


def _latest_per_pk(batch_df: DataFrame, pk: str) -> DataFrame:
    window = Window.partitionBy(pk).orderBy(
        F.col("_batch_id").desc_nulls_last(),
        F.col("_ingest_timestamp").desc_nulls_last(),
    )
    return (
        batch_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_product_row_hash(df: DataFrame) -> DataFrame:
    canonical = [
        F.coalesce(F.col(name).cast("string"), F.lit(""))
        for name in PRODUCT_HASH_COLUMNS
    ]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *canonical), 256))


def conform_snapshot_batch(
    batch_df: DataFrame,
    entity: str,
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> DataFrame:
    del spark, catalog
    pk = ENTITY_PK[entity]
    latest = _latest_per_pk(batch_df, pk)
    if entity == "products" and "_row_hash" not in latest.columns:
        latest = add_product_row_hash(latest)
    return latest


def conform_incremental_batch(
    batch_df: DataFrame,
    entity: str,
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> DataFrame:
    del spark, catalog
    pk = ENTITY_PK[entity]
    return _latest_per_pk(batch_df, pk)


def prepare_silver_rows(
    df: DataFrame,
    entity: str,
    updated_at: datetime,
) -> DataFrame:
    del entity
    result = (
        df.withColumn("quality_check_result", F.lit("PASS"))
        .withColumn("_is_deleted", F.lit(False))
        .withColumn("_silver_updated_at", F.lit(updated_at))
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
    )
    if "_row_hash" not in result.columns:
        result = result.withColumn("_row_hash", F.lit(None).cast("string"))
    bronze_meta = {
        "_ingest_timestamp",
        "_source_file",
        "_batch_id",
        "_delivery_pattern",
        "_rescued_data",
        "_violations",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    }
    drop_cols = [
        c for c in result.columns if c in bronze_meta or c.startswith("_change")
    ]
    return result.drop(*drop_cols)


def merge_to_silver(
    df: DataFrame,
    entity: str,
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> int:
    if not df.take(1):
        return 0
    target = silver_table(entity, catalog)
    pk = ENTITY_PK[entity]
    from delta.tables import DeltaTable

    merge_df = prepare_silver_rows(df, entity, datetime.now(UTC))
    delta_table = DeltaTable.forName(spark, target)
    business_cols = [
        c
        for c in merge_df.columns
        if c not in {
            pk,
            "quality_check_result",
            "_row_hash",
            "_is_deleted",
            "_silver_updated_at",
            "_bronze_batch_id",
        }
    ]
    update_map = {c: f"source.{c}" for c in business_cols}
    update_map.update(
        {
            "quality_check_result": "source.quality_check_result",
            "_is_deleted": "source._is_deleted",
            "_silver_updated_at": "source._silver_updated_at",
            "_bronze_batch_id": "source._bronze_batch_id",
        }
    )
    if "_row_hash" in merge_df.columns:
        update_map["_row_hash"] = "source._row_hash"
    (
        delta_table.alias("target")
        .merge(merge_df.alias("source"), f"target.{pk} = source.{pk}")
        .whenMatchedUpdate(condition=None, set=update_map)
        .whenNotMatchedInsertAll()
        .execute()
    )
    return merge_df.count()


def apply_snapshot_soft_deletes(
    spark: SparkSession,
    entity: str,
    snapshot_df: DataFrame,
    catalog: str = DEFAULT_CATALOG,
    updated_at: datetime | None = None,
) -> int:
    if entity not in SNAPSHOT_ENTITIES:
        return 0
    pk = ENTITY_PK[entity]
    active_pks = snapshot_df.select(pk).distinct()
    target = silver_table(entity, catalog)
    silver_active = spark.table(target).where(~F.col("_is_deleted")).select(pk)
    missing = silver_active.join(active_pks, pk, "left_anti")
    if not missing.take(1):
        return 0
    from delta.tables import DeltaTable

    ts = (updated_at or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M:%S")
    delta_table = DeltaTable.forName(spark, target)
    (
        delta_table.alias("target")
        .merge(missing.alias("missing"), f"target.{pk} = missing.{pk}")
        .whenMatchedUpdate(
            condition="target._is_deleted = false",
            set={
                "_is_deleted": "true",
                "_silver_updated_at": f"CAST('{ts}' AS TIMESTAMP)",
            },
        )
        .execute()
    )
    return missing.count()
