from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
from bronze.config import SourceConfig
from bronze.metadata import add_ingest_metadata
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _customers_config() -> SourceConfig:
    return SourceConfig(
        source_name="customers",
        target_table="de_assessment.bronze.customers",
        raw_path="/Volumes/de_assessment/landing/raw/customers/",
        checkpoint_path="/Volumes/de_assessment/ops/checkpoints/customers/",
        schema_hint_path="/Volumes/de_assessment/ops/checkpoints/customers/_schema/",
        archive_path=None,
        file_format="csv",
        delivery_pattern="full_snapshot",
        cdf_enabled=True,
        schedule_hint="daily",
        is_active=True,
    )


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("bronze-metadata-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.spark
def test_add_ingest_metadata_preserves_nulls_and_duplicates(
    spark: SparkSession,
) -> None:
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("lifetime_value", DecimalType(18, 2), True),
        ]
    )
    rows = [
        (
            1,
            "Alice",
            None,
            "US",
            date(2024, 1, 1),
            "premium",
            Decimal("100.00"),
        ),
        (
            1,
            "Alice Duplicate",
            "alice@example.com",
            "US",
            date(2024, 1, 2),
            "premium",
            Decimal("150.00"),
        ),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    ingest_timestamp = datetime(2026, 8, 21, 10, 0, 0, tzinfo=UTC)

    result = add_ingest_metadata(
        df,
        _customers_config(),
        batch_id="batch-123",
        ingest_timestamp=ingest_timestamp,
        source_file_column=F.lit("/Volumes/de_assessment/landing/raw/customers/file.csv"),
    )

    assert result.count() == 2
    assert result.select("customer_id").distinct().count() == 1
    assert result.where(F.col("email").isNull()).count() == 1
    assert result.select("_batch_id").first()[0] == "batch-123"
    assert result.select("_delivery_pattern").first()[0] == "full_snapshot"
    assert result.where(F.col("_row_hash").isNull()).count() == 0


@pytest.mark.spark
def test_customer_hash_stable_when_input_columns_reordered(
    spark: SparkSession,
) -> None:
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("lifetime_value", DecimalType(18, 2), True),
        ]
    )
    row = (
        42,
        "Bob",
        "bob@example.com",
        "CA",
        date(2024, 3, 15),
        "standard",
        Decimal("250.50"),
    )
    df = spark.createDataFrame([row], schema=schema)
    reordered = df.select(
        "lifetime_value",
        "customer_segment",
        "signup_date",
        "country",
        "email",
        "customer_name",
        "customer_id",
    )
    ingest_timestamp = datetime(2026, 8, 21, 10, 0, 0, tzinfo=UTC)
    config = _customers_config()
    kwargs = {
        "batch_id": "batch-456",
        "ingest_timestamp": ingest_timestamp,
        "source_file_column": F.lit("customers.csv"),
    }

    original_hash = add_ingest_metadata(df, config, **kwargs).select("_row_hash").first()[0]
    reordered_hash = (
        add_ingest_metadata(reordered, config, **kwargs).select("_row_hash").first()[0]
    )

    assert original_hash == reordered_hash


@pytest.mark.spark
def test_add_ingest_metadata_skips_row_hash_for_orders(spark: SparkSession) -> None:
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", DateType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(18, 2), True),
            StructField("total_amount", DecimalType(18, 2), True),
            StructField("order_status", StringType(), True),
            StructField("payment_date", DateType(), True),
        ]
    )
    row = (
        1,
        10,
        date(2024, 5, 1),
        5,
        2,
        Decimal("19.99"),
        Decimal("39.98"),
        "shipped",
        None,
    )
    df = spark.createDataFrame([row], schema=schema)
    config = SourceConfig(
        source_name="orders",
        target_table="de_assessment.bronze.orders",
        raw_path="/Volumes/de_assessment/landing/raw/orders/incoming/",
        checkpoint_path="/Volumes/de_assessment/ops/checkpoints/orders/",
        schema_hint_path="/Volumes/de_assessment/ops/checkpoints/orders/_schema/",
        archive_path="/Volumes/de_assessment/landing/raw/orders/processed/",
        file_format="csv",
        delivery_pattern="incremental",
        cdf_enabled=True,
        schedule_hint="on_arrival",
        is_active=True,
    )

    result = add_ingest_metadata(
        df,
        config,
        batch_id="batch-789",
        ingest_timestamp=datetime(2026, 8, 21, 11, 0, 0, tzinfo=UTC),
        source_file_column=F.lit("orders.csv"),
    )

    assert "_row_hash" not in result.columns
    assert result.select("_delivery_pattern").first()[0] == "incremental"
