import pytest
from bronze.config import (
    SourceConfig,
    bronze_table,
    get_source_config,
    manifest_table,
    source_config_table,
)


@pytest.mark.unit
def test_fqn_helpers_use_three_level_uc_names() -> None:
    assert bronze_table("customers") == "de_assessment.bronze.customers"
    assert source_config_table() == "de_assessment.bronze.source_config"
    assert manifest_table() == "de_assessment.bronze.ingest_manifest"


@pytest.mark.unit
def test_source_config_rejects_unknown_delivery_pattern() -> None:
    with pytest.raises(ValueError, match="delivery_pattern"):
        SourceConfig(
            source_name="orders",
            target_table="de_assessment.bronze.orders",
            raw_path="/Volumes/de_assessment/landing/raw/orders/incoming/",
            checkpoint_path="/Volumes/de_assessment/ops/checkpoints/orders/",
            schema_hint_path="/Volumes/de_assessment/ops/checkpoints/orders/_schema/",
            archive_path=None,
            file_format="csv",
            delivery_pattern="merge",
            cdf_enabled=True,
            schedule_hint="on_arrival",
            is_active=True,
        )


def _valid_source_config_row(**overrides: object) -> dict[str, object]:
    row = {
        "source_name": "orders",
        "target_table": "de_assessment.bronze.orders",
        "raw_path": "/Volumes/de_assessment/landing/raw/orders/incoming/",
        "checkpoint_path": "/Volumes/de_assessment/ops/checkpoints/orders/",
        "schema_hint_path": "/Volumes/de_assessment/ops/checkpoints/orders/_schema/",
        "archive_path": None,
        "file_format": "csv",
        "delivery_pattern": "incremental",
        "cdf_enabled": True,
        "schedule_hint": "on_arrival",
        "is_active": True,
    }
    row.update(overrides)
    return row


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("bronze-config-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _source_config_schema():
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    return StructType(
        [
            StructField("source_name", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("raw_path", StringType(), False),
            StructField("checkpoint_path", StringType(), False),
            StructField("schema_hint_path", StringType(), False),
            StructField("archive_path", StringType(), True),
            StructField("file_format", StringType(), False),
            StructField("delivery_pattern", StringType(), False),
            StructField("cdf_enabled", BooleanType(), False),
            StructField("schedule_hint", StringType(), False),
            StructField("is_active", BooleanType(), False),
        ]
    )


@pytest.mark.spark
def test_get_source_config_raises_when_no_active_rows(
    spark, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "test_source_config_zero"
    monkeypatch.setattr(
        "bronze.config.source_config_table", lambda catalog="de_assessment": table_name
    )
    spark.createDataFrame(
        [_valid_source_config_row(is_active=False)], schema=_source_config_schema()
    ).createOrReplaceTempView(table_name)

    with pytest.raises(ValueError, match="Expected one active source_config"):
        get_source_config(spark, "orders")


@pytest.mark.spark
def test_get_source_config_raises_when_two_active_rows(
    spark, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "test_source_config_duplicate"
    monkeypatch.setattr(
        "bronze.config.source_config_table", lambda catalog="de_assessment": table_name
    )
    spark.createDataFrame(
        [
            _valid_source_config_row(),
            _valid_source_config_row(),
        ],
        schema=_source_config_schema(),
    ).createOrReplaceTempView(table_name)

    with pytest.raises(ValueError, match="Expected one active source_config"):
        get_source_config(spark, "orders")
