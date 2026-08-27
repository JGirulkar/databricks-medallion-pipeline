"""Contract tier: real gold SQL vs an independent pandas recompute from silver.

Builds real silver tables from real generator output pushed through the real
silver functions (see conftest.py's `silver_tables` fixture — both the seed
and the delta delivery run, so gold's filter sees real `_is_orphan` /
`_is_deleted` rows and healed orphans, not hand-built stand-ins). Runs the
real gold SQL via `run_gold`, then checks every gold table against pandas
group-bys computed independently from the silver snapshots.

Silver is gold's input contract — already proven correct by silver's own
contract suite — so these tests trust the silver snapshot and only check
gold's arithmetic against it. The expectations below are pandas only: no
Spark SQL, and no reading gold tables to build another gold table's
expectation (04_customer_segmentation.sql is exempt from that rule only in
the sense that it is *itself* derived from 02 in production; test 4 still
recomputes revenue_by_customer independently in pandas rather than reading
gold.revenue_by_customer, so the segmentation check does not inherit any bug
02's own SQL might have).
"""

from __future__ import annotations

import datetime as dt
import math

import pandas as pd
import pytest
from gold.config import HIGH_VALUE_REVENUE, INACTIVE_DAYS

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------
# Shared helpers, pandas only.
# --------------------------------------------------------------------------
def _missing(value: object) -> bool:
    """None, NaN, or NaT — whatever pandas used to stand in for a SQL NULL."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return bool(pd.isna(value))


def _qualifying(orders: pd.DataFrame) -> pd.DataFrame:
    return orders[
        (orders["order_status"] == "Completed")
        & ~orders["_is_orphan"]
        & ~orders["_is_deleted"]
    ]


def _close(a: object, b: object, tol: float = 0.01) -> bool:
    """Float-tolerant equality for money values (one cent)."""
    a = 0.0 if _missing(a) else float(a)
    b = 0.0 if _missing(b) else float(b)
    return abs(a - b) < tol


def _gold_table(spark, name: str) -> pd.DataFrame:
    return spark.table(f"gct_gold.{name}").toPandas()


def _by_product_expectation(silver: dict) -> pd.DataFrame:
    """Independent recompute of sales_by_product from the silver snapshots."""
    products = silver["products"]
    qualifying = _qualifying(silver["orders"])

    agg = (
        qualifying.groupby("product_id")["total_amount"]
        .agg(total_orders="count", total_revenue="sum")
        .reset_index()
    )
    active = products[~products["_is_deleted"]][
        ["product_id", "product_name", "category"]
    ]
    expected = active.merge(agg, on="product_id", how="left")
    expected["total_orders"] = expected["total_orders"].fillna(0).astype(int)
    expected["total_revenue"] = expected["total_revenue"].fillna(0.0)
    return expected.sort_values("product_id").reset_index(drop=True)


def _by_customer_expectation(silver: dict) -> pd.DataFrame:
    """Independent recompute of revenue_by_customer from the silver snapshots."""
    customers = silver["customers"]
    qualifying = _qualifying(silver["orders"])

    agg = qualifying.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_revenue=("total_amount", "sum"),
        last_order_date=("order_date", "max"),
    ).reset_index()
    active = customers[~customers["_is_deleted"]][
        ["customer_id", "customer_name", "customer_segment"]
    ]
    expected = active.merge(agg, on="customer_id", how="left")
    expected["total_orders"] = expected["total_orders"].fillna(0).astype(int)
    expected["total_revenue"] = expected["total_revenue"].fillna(0.0)
    return expected.sort_values("customer_id").reset_index(drop=True)


# --------------------------------------------------------------------------
# 1. sales_by_product
# --------------------------------------------------------------------------
def test_sales_by_product_matches_independent_recompute(silver_tables) -> None:
    spark, silver = silver_tables["spark"], silver_tables["silver"]
    expected = _by_product_expectation(silver)
    actual = _gold_table(spark, "sales_by_product").sort_values("product_id").reset_index(
        drop=True
    )

    assert list(actual["product_id"]) == list(expected["product_id"])
    for a, e in zip(actual.itertuples(), expected.itertuples()):
        assert a.product_id == e.product_id
        assert a.total_orders == e.total_orders, f"product {a.product_id}"
        assert _close(a.total_revenue, e.total_revenue), f"product {a.product_id}"
        if e.total_orders == 0:
            assert e.total_revenue == 0
            assert _missing(a.avg_order_value), f"product {a.product_id} should have NULL avg"
        else:
            assert _close(
                a.avg_order_value, e.total_revenue / e.total_orders
            ), f"product {a.product_id}"


# --------------------------------------------------------------------------
# 2. revenue_by_customer
# --------------------------------------------------------------------------
def test_revenue_by_customer_matches_independent_recompute(silver_tables) -> None:
    spark, silver = silver_tables["spark"], silver_tables["silver"]
    expected = _by_customer_expectation(silver)
    actual = _gold_table(spark, "revenue_by_customer").sort_values("customer_id").reset_index(
        drop=True
    )

    assert list(actual["customer_id"]) == list(expected["customer_id"])
    for a, e in zip(actual.itertuples(), expected.itertuples()):
        assert a.customer_id == e.customer_id
        assert a.total_orders == e.total_orders, f"customer {a.customer_id}"
        assert _close(a.total_revenue, e.total_revenue), f"customer {a.customer_id}"
        # lifetime_value_actual is total_revenue restated, always.
        assert _close(a.lifetime_value_actual, e.total_revenue), f"customer {a.customer_id}"
        if e.total_orders == 0:
            assert _missing(a.avg_order_value), f"customer {a.customer_id}"
            assert _missing(e.last_order_date), f"customer {a.customer_id}"
            assert _missing(a.last_order_date), f"customer {a.customer_id}"
        else:
            assert _close(
                a.avg_order_value, e.total_revenue / e.total_orders
            ), f"customer {a.customer_id}"
            assert pd.Timestamp(a.last_order_date) == pd.Timestamp(
                e.last_order_date
            ), f"customer {a.customer_id}"


# --------------------------------------------------------------------------
# 3. daily_weekly_trends
# --------------------------------------------------------------------------
def test_daily_weekly_trends_matches_independent_recompute(silver_tables) -> None:
    spark, silver = silver_tables["spark"], silver_tables["silver"]
    qualifying = _qualifying(silver["orders"])

    expected = (
        qualifying.groupby("order_date")
        .agg(total_orders=("order_id", "count"), total_revenue=("total_amount", "sum"))
        .reset_index()
        .sort_values("order_date")
        .reset_index(drop=True)
    )
    actual = (
        _gold_table(spark, "daily_weekly_trends")
        .sort_values("order_date")
        .reset_index(drop=True)
    )

    assert list(actual["order_date"]) == list(expected["order_date"])
    for a, e in zip(actual.itertuples(), expected.itertuples()):
        assert a.total_orders == e.total_orders, a.order_date
        assert _close(a.total_revenue, e.total_revenue), a.order_date
        assert _close(a.avg_order_value, e.total_revenue / e.total_orders), a.order_date
        # Monday truncation, matching DATE_TRUNC('WEEK', ...).
        order_date = pd.Timestamp(a.order_date)
        expected_week_start = order_date - dt.timedelta(days=order_date.weekday())
        assert pd.Timestamp(a.week_start) == expected_week_start, a.order_date


# --------------------------------------------------------------------------
# 4. customer_segmentation — the stated ladder
# --------------------------------------------------------------------------
def _segment(last_order_date, total_orders: int, total_revenue: float, cutoff) -> str:
    if _missing(last_order_date) or last_order_date < cutoff:
        return "Inactive"
    if total_revenue >= HIGH_VALUE_REVENUE:
        return "High-Value"
    if total_orders >= 2:
        return "Repeat"
    return "One-Time"


def test_segmentation_matches_the_stated_ladder(silver_tables) -> None:
    spark, silver = silver_tables["spark"], silver_tables["silver"]
    by_customer = _by_customer_expectation(silver)

    # last_order_date is an object column mixing datetime.date (customers with
    # qualifying orders) and float NaN (customers with none, from the left
    # merge) — numpy's max() chokes comparing date to float, so filter by
    # hand rather than call .max() on the raw column.
    present_dates = [d for d in by_customer["last_order_date"] if not _missing(d)]
    assert present_dates, "expected at least one qualifying order to anchor as_of"
    as_of = max(present_dates)
    cutoff = as_of - dt.timedelta(days=INACTIVE_DAYS)

    by_customer = by_customer.copy()
    by_customer["segment_type"] = [
        _segment(row.last_order_date, row.total_orders, row.total_revenue, cutoff)
        for row in by_customer.itertuples()
    ]
    expected = (
        by_customer.groupby("segment_type")
        .agg(customer_count=("customer_id", "count"), total_revenue=("total_revenue", "sum"))
        .reset_index()
    )
    expected["avg_revenue"] = expected["total_revenue"] / expected["customer_count"]

    actual = _gold_table(spark, "customer_segmentation")

    expected_by_type = {r.segment_type: r for r in expected.itertuples()}
    actual_by_type = {r.segment_type: r for r in actual.itertuples()}
    assert set(expected_by_type) == set(actual_by_type)
    for segment_type, e in expected_by_type.items():
        a = actual_by_type[segment_type]
        assert a.customer_count == e.customer_count, segment_type
        assert _close(a.avg_revenue, e.avg_revenue), segment_type
        assert _close(a.total_revenue, e.total_revenue), segment_type


# --------------------------------------------------------------------------
# 5. cross-footing across all four tables
# --------------------------------------------------------------------------
def test_gold_tables_cross_foot(silver_tables) -> None:
    spark = silver_tables["spark"]
    sales = _gold_table(spark, "sales_by_product")
    revenue = _gold_table(spark, "revenue_by_customer")
    trends = _gold_table(spark, "daily_weekly_trends")
    segmentation = _gold_table(spark, "customer_segmentation")

    assert segmentation["customer_count"].sum() == len(revenue)

    totals = {
        "revenue_by_customer": float(revenue["total_revenue"].sum()),
        "sales_by_product": float(sales["total_revenue"].sum()),
        "daily_weekly_trends": float(trends["total_revenue"].sum()),
        "customer_segmentation": float(segmentation["total_revenue"].sum()),
    }
    baseline = totals["revenue_by_customer"]
    for name, total in totals.items():
        assert abs(total - baseline) < 0.01, f"{name} total_revenue={total} != {baseline}"


# --------------------------------------------------------------------------
# 6. every segment is reachable
# --------------------------------------------------------------------------
def test_every_segment_is_reachable(silver_tables) -> None:
    spark = silver_tables["spark"]
    segmentation = _gold_table(spark, "customer_segmentation")
    present = {
        row.segment_type: row.customer_count for row in segmentation.itertuples()
    }
    for segment_type in ("Inactive", "High-Value", "Repeat", "One-Time"):
        assert segment_type in present, (
            f"segment {segment_type!r} is unreachable at this generator sizing "
            f"(present: {sorted(present)})"
        )
        assert present[segment_type] > 0, segment_type


# --------------------------------------------------------------------------
# 7. zero-activity rows are kept; soft-deleted products are absent
# --------------------------------------------------------------------------
def test_zero_activity_rows_are_kept(silver_tables) -> None:
    spark, silver = silver_tables["spark"], silver_tables["silver"]
    sales = _gold_table(spark, "sales_by_product")
    revenue = _gold_table(spark, "revenue_by_customer")

    zero_products = sales[sales["total_orders"] == 0]
    zero_customers = revenue[revenue["total_orders"] == 0]
    assert not zero_products.empty, "expected at least one zero-sales product"
    assert not zero_customers.empty, "expected at least one zero-activity customer"
    assert zero_products["avg_order_value"].apply(_missing).all()
    assert zero_customers["avg_order_value"].apply(_missing).all()

    # The delta wave's soft-deleted products must not appear at all.
    deleted_product_ids = set(
        silver["products"].loc[silver["products"]["_is_deleted"], "product_id"]
    )
    assert deleted_product_ids, "expected the delta wave to soft-delete some products"
    leaked = deleted_product_ids & set(sales["product_id"])
    assert not leaked, f"soft-deleted products present in sales_by_product: {leaked}"


# --------------------------------------------------------------------------
# 8. total_amount reconciles against quantity * unit_price
# --------------------------------------------------------------------------
def test_revenue_column_reconciles(silver_tables) -> None:
    qualifying = _qualifying(silver_tables["silver"]["orders"])
    assert not qualifying.empty

    mismatches = []
    for row in qualifying.itertuples():
        expected = round(float(row.quantity) * float(row.unit_price), 2)
        if not _close(row.total_amount, expected, tol=0.01):
            mismatches.append((row.order_id, row.total_amount, expected))
    assert not mismatches, f"total_amount does not reconcile for: {mismatches[:10]}"


# --------------------------------------------------------------------------
# 9. as_of is data-anchored
# --------------------------------------------------------------------------
def test_as_of_is_data_anchored(silver_tables) -> None:
    qualifying = _qualifying(silver_tables["silver"]["orders"])
    as_of = qualifying["order_date"].max()
    today = pd.Timestamp(dt.date.today())

    assert (qualifying["order_date"] <= as_of).all()
    assert pd.Timestamp(as_of) <= today, (
        "as_of is derived from qualifying order_date; silver's date-window "
        "check (max_date: today) is what keeps this true — a future-dated "
        "row reaching here would mean that check regressed"
    )
