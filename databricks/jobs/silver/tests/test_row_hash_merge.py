"""The merge skips rows whose business values are unchanged.

Every delivery of a snapshot feed restates the whole world, so most rows are
identical to what silver already holds. Rewriting them costs a full file
rewrite for no change: one CE run updated 98,905 order rows while changing
nothing. `_row_hash` was already being computed for products and then never
consulted — merge_to_silver called whenMatchedUpdate with condition=None.

The hash now gates the update. Two states must still force a write even when
the values match, or the skip would strand a row in the wrong state:
a row previously soft-deleted whose key has returned, and a row flagged orphan
whose parent has since arrived.
"""

from __future__ import annotations

import pytest
from conftest import create_delta_table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver.conform import add_row_hash, merge_to_silver
from silver.schemas import silver_entity_schema

TARGET = "test_hash_silver_products"


def _bronze(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    df = spark.createDataFrame(
        rows,
        schema=(
            "product_id INT, product_name STRING, category STRING, "
            "price DECIMAL(18,2), cost DECIMAL(18,2), stock_quantity INT, "
            "reorder_level INT, _batch_id STRING"
        ),
    ).withColumn("_ingest_timestamp", F.current_timestamp())
    return add_row_hash(df, "products")


def _updated_rows(spark: SparkSession) -> int:
    hist = spark.sql(f"DESCRIBE HISTORY {TARGET}").filter("operation = 'MERGE'") \
        .orderBy(F.col("version").desc()).limit(1).collect()
    return int(hist[0]["operationMetrics"].get("numTargetRowsUpdated", 0))


@pytest.fixture
def wired(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    create_delta_table(spark, TARGET, silver_entity_schema("products"))
    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda _entity, _catalog="de_assessment": TARGET,
    )


@pytest.mark.spark
def test_identical_redelivery_updates_nothing(spark: SparkSession, wired: None) -> None:
    rows = [(1, "Widget", "Cat", None, None, 10, 5, "b1")]
    merge_to_silver(_bronze(spark, rows), "products", spark)

    merge_to_silver(_bronze(spark, rows), "products", spark)

    assert _updated_rows(spark) == 0, "unchanged rows must not be rewritten"
    assert spark.table(TARGET).count() == 1


@pytest.mark.spark
def test_changed_value_is_written(spark: SparkSession, wired: None) -> None:
    merge_to_silver(
        _bronze(spark, [(1, "Widget", "Cat", None, None, 10, 5, "b1")]), "products", spark
    )

    merge_to_silver(
        _bronze(spark, [(1, "Widget", "Cat", None, None, 99, 5, "b2")]), "products", spark
    )

    assert _updated_rows(spark) == 1
    assert spark.table(TARGET).collect()[0]["stock_quantity"] == 99


@pytest.mark.spark
def test_returning_key_is_revived_even_when_identical(
    spark: SparkSession, wired: None
) -> None:
    """A soft-deleted key that comes back must be revived by the merge.

    The row is byte-identical, so the hash matches and the value comparison
    alone would skip it — leaving it flagged deleted forever.
    """
    rows = [(1, "Widget", "Cat", None, None, 10, 5, "b1")]
    merge_to_silver(_bronze(spark, rows), "products", spark)
    spark.sql(f"UPDATE {TARGET} SET _is_deleted = true WHERE product_id = 1")

    merge_to_silver(_bronze(spark, rows), "products", spark)

    assert spark.table(TARGET).collect()[0]["_is_deleted"] is False


@pytest.mark.spark
def test_orphan_clearing_is_written_even_when_identical(
    spark: SparkSession, wired: None
) -> None:
    """Same reasoning for a row whose parent has since arrived."""
    rows = [(1, "Widget", "Cat", None, None, 10, 5, "b1")]
    merge_to_silver(_bronze(spark, rows), "products", spark)
    spark.sql(f"UPDATE {TARGET} SET _is_orphan = true WHERE product_id = 1")

    merge_to_silver(_bronze(spark, rows), "products", spark)

    assert spark.table(TARGET).collect()[0]["_is_orphan"] is False


@pytest.mark.spark
def test_hash_covers_every_business_column(spark: SparkSession) -> None:
    """Any business column changing must change the hash.

    A hash over a hand-listed subset silently ignores edits to the columns it
    forgot, so it is derived from the entity schema instead.
    """
    base = _bronze(spark, [(1, "Widget", "Cat", None, None, 10, 5, "b1")])
    base_hash = base.collect()[0]["_row_hash"]
    for changed in [
        (1, "Gadget", "Cat", None, None, 10, 5, "b1"),
        (1, "Widget", "Other", None, None, 10, 5, "b1"),
        (1, "Widget", "Cat", None, None, 11, 5, "b1"),
        (1, "Widget", "Cat", None, None, 10, 6, "b1"),
    ]:
        assert _bronze(spark, [changed]).collect()[0]["_row_hash"] != base_hash, changed
