"""Delta CDF streaming consumer for Bronze → Silver."""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.config import bronze_table, silver_checkpoint_path


def filter_cdf_post_images(batch_df: DataFrame) -> DataFrame:
    if "_change_type" not in batch_df.columns:
        return batch_df
    return batch_df.filter(
        F.col("_change_type").isin("insert", "update_postimage")
    )


def run_cdf_stream(
    spark: SparkSession,
    entity: str,
    process_batch: Callable[[DataFrame, int], None],
    catalog: str = "de_assessment",
    checkpoint: str | None = None,
) -> None:
    bronze_fqn = bronze_table(entity, catalog)
    checkpoint = checkpoint or silver_checkpoint_path(entity, catalog)
    query = (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(bronze_fqn)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
