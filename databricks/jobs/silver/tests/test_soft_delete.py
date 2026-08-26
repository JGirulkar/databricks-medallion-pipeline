"""Soft-delete behaviour for full-snapshot entities.

A snapshot feed states the complete world each delivery, so a key that was
present before and is absent now has been deleted at source. Silver marks it
`_is_deleted = true` rather than removing the row, keeping history queryable
and letting the foreign-key check stop honouring a parent that no longer
exists.

This path had no coverage. It appeared to work in the CE run only because a
generator defect was nulling primary keys in place, which made those rows
vanish from the snapshot as a side effect. With that fixed, nothing exercises
it — so these tests do.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from conftest import create_delta_table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver.conform import apply_snapshot_soft_deletes
from silver.schemas import silver_entity_schema

SILVER_PRODUCTS = "test_sd_silver_products"


def _seed_silver(spark: SparkSession, product_ids: list[int]) -> None:
    create_delta_table(spark, SILVER_PRODUCTS, silver_entity_schema("products"))
    rows = [
        (
            pid, f"P{pid}", "Cat", None, None, 10, 5,
            "PASS", None, False, datetime(2026, 1, 1, tzinfo=UTC), "b1",
        )
        for pid in product_ids
    ]
    spark.createDataFrame(rows, schema=silver_entity_schema("products")) \
        .write.format("delta").mode("append").saveAsTable(SILVER_PRODUCTS)


def _snapshot(spark: SparkSession, product_ids: list[int]) -> DataFrame:
    return spark.createDataFrame(
        [(pid,) for pid in product_ids], schema="product_id INT"
    )


def _deleted_ids(spark: SparkSession) -> set[int]:
    return {
        r["product_id"]
        for r in spark.table(SILVER_PRODUCTS).filter(F.col("_is_deleted")).collect()
    }


@pytest.fixture
def wired(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "silver.conform.silver_table",
        lambda _entity, _catalog="de_assessment": SILVER_PRODUCTS,
    )


@pytest.mark.spark
def test_key_absent_from_the_new_snapshot_is_soft_deleted(
    spark: SparkSession, wired: None
) -> None:
    _seed_silver(spark, [1, 2, 3])

    # Product 2 is gone from the source; 1 and 3 are still there.
    count = apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 3]))

    assert count == 1
    assert _deleted_ids(spark) == {2}
    # The row is marked, never removed.
    assert spark.table(SILVER_PRODUCTS).count() == 3


@pytest.mark.spark
def test_unchanged_snapshot_deletes_nothing(spark: SparkSession, wired: None) -> None:
    _seed_silver(spark, [1, 2, 3])

    count = apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 2, 3]))

    assert count == 0
    assert _deleted_ids(spark) == set()


@pytest.mark.spark
def test_soft_delete_is_idempotent(spark: SparkSession, wired: None) -> None:
    """Re-running the same snapshot must not re-mark an already-deleted row."""
    _seed_silver(spark, [1, 2, 3])

    first = apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 3]))
    second = apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 3]))

    assert first == 1
    assert second == 0, "the merge condition already excludes deleted rows"
    assert _deleted_ids(spark) == {2}


@pytest.mark.spark
def test_incremental_entity_is_never_soft_deleted(
    spark: SparkSession, wired: None
) -> None:
    """Absence from an incremental delivery means nothing.

    An incremental feed sends only what changed, so a key not present in this
    delivery is simply unchanged — treating that as a delete would wipe the
    table on the first partial file.
    """
    _seed_silver(spark, [1, 2, 3])

    count = apply_snapshot_soft_deletes(spark, "orders", _snapshot(spark, []))

    assert count == 0
    assert _deleted_ids(spark) == set()


@pytest.mark.spark
def test_returning_key_is_not_resurrected_today(
    spark: SparkSession, wired: None
) -> None:
    """KNOWN GAP, pinned so a future change is a deliberate one.

    whenMatchedUpdate is conditioned on `target._is_deleted = false`, so a key
    that disappears and later comes back stays flagged deleted. merge_to_silver
    does not clear the flag either. For a snapshot feed a returning key should
    arguably be revived; today it is not, and the foreign-key check will keep
    rejecting children of that parent.
    """
    _seed_silver(spark, [1, 2, 3])
    apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 3]))
    assert _deleted_ids(spark) == {2}

    # Product 2 reappears in the next snapshot.
    apply_snapshot_soft_deletes(spark, "products", _snapshot(spark, [1, 2, 3]))

    assert _deleted_ids(spark) == {2}, "still flagged deleted — documented gap"
