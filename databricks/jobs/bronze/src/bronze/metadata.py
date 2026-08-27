from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from bronze.config import SourceConfig

CUSTOMER_HASH_COLUMNS = (
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
)


def add_ingest_metadata(
    df: DataFrame,
    config: SourceConfig,
    batch_id: str,
    ingest_timestamp: datetime,
    source_file_column: Column | None = None,
) -> DataFrame:
    source_file = (
        source_file_column
        if source_file_column is not None
        else F.col("_metadata.file_path")
    )
    result = (
        df.withColumn("_ingest_timestamp", F.lit(ingest_timestamp))
        .withColumn("_source_file", source_file)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_delivery_pattern", F.lit(config.delivery_pattern))
    )
    if config.source_name == "customers":
        canonical = [
            F.coalesce(F.col(name).cast("string"), F.lit(""))
            for name in CUSTOMER_HASH_COLUMNS
        ]
        result = result.withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("||", *canonical), 256),
        )
    return result
