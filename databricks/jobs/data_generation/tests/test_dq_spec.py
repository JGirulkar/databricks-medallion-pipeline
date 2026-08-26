from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

MODULE = Path(__file__).resolve().parents[1] / "src" / "generate_sample_data.py"
spec = importlib.util.spec_from_file_location("generate_sample_data", MODULE)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@pytest.mark.unit
def test_dq_issue_counts_documented() -> None:
    expected = mod.DQ_ISSUE_COUNTS
    assert sum(expected.values()) == 725


@pytest.mark.unit
def test_base_constants_match_pdf() -> None:
    assert mod.BASE_CUSTOMERS == 10_000
    assert mod.BASE_ORDERS == 100_000
    assert mod.BASE_PRODUCTS == 500


@pytest.mark.unit
def test_volume_root_for_catalog() -> None:
    assert mod.volume_root_for_catalog("de_assessment") == (
        "/Volumes/de_assessment/landing/raw"
    )


@pytest.mark.unit
def test_inject_customer_issues() -> None:
    df = pd.DataFrame(
        {
            "customer_id": list(range(1, 21)),
            "customer_name": [f"User {i}" for i in range(1, 21)],
            "email": [f"user{i}@example.com" for i in range(1, 21)],
            "country": ["US"] * 20,
            "signup_date": ["2024-01-01"] * 20,
            "customer_segment": ["Standard"] * 20,
            "lifetime_value": [100.0] * 20,
        }
    )
    counts = mod.DQ_ISSUE_COUNTS.copy()
    mod.DQ_ISSUE_COUNTS["null_emails"] = 5
    mod.DQ_ISSUE_COUNTS["duplicate_customer_ids"] = 3
    mod.DQ_ISSUE_COUNTS["null_customer_id"] = 2
    try:
        result = mod.inject_customer_issues(df)
    finally:
        mod.DQ_ISSUE_COUNTS.update(counts)

    # 20 base + 3 duplicate rows + 2 appended NULL-key rows
    assert len(result) == 25
    assert int(result["email"].isna().sum()) == 5
    assert int((result["customer_id"] == 1).sum()) == 4
    assert int(result["customer_id"].isna().sum()) == 2


@pytest.mark.unit
def test_inject_order_issues() -> None:
    df = pd.DataFrame(
        {
            "order_id": list(range(1, 51)),
            "customer_id": list(range(1, 51)),
            "order_date": ["2024-01-01"] * 50,
            "product_id": list(range(1, 51)),
            "quantity": [1] * 50,
            "unit_price": [10.0] * 50,
            "total_amount": [10.0] * 50,
            "order_status": ["Completed"] * 50,
            "payment_date": ["2024-01-02"] * 50,
        }
    )
    counts = mod.DQ_ISSUE_COUNTS.copy()
    mod.DQ_ISSUE_COUNTS["null_order_customer_id"] = 4
    mod.DQ_ISSUE_COUNTS["null_order_product_id"] = 6
    mod.DQ_ISSUE_COUNTS["orphan_customer_id"] = 3
    mod.DQ_ISSUE_COUNTS["orphan_product_id"] = 2
    mod.DQ_ISSUE_COUNTS["duplicate_order_ids"] = 2
    mod.DQ_ISSUE_COUNTS["null_order_id"] = 2
    valid_customers = set(range(1, 51))
    valid_products = set(range(1, 51))
    try:
        result = mod.inject_order_issues(df, valid_customers, valid_products)
    finally:
        mod.DQ_ISSUE_COUNTS.update(counts)

    # 50 base + 2 duplicate rows + 2 appended NULL-key rows
    assert len(result) == 54
    assert int(result["order_id"].isna().sum()) == 2
    assert int(result["customer_id"].isna().sum()) == 4
    assert int(result["product_id"].isna().sum()) == 6
    orphan_c = set(result.loc[result["customer_id"].notna(), "customer_id"].astype(int)) - valid_customers
    orphan_p = set(result.loc[result["product_id"].notna(), "product_id"].astype(int)) - valid_products
    assert len(orphan_c) == 3
    assert len(orphan_p) == 2
    assert int((result["order_id"] == 1).sum()) == 3


@pytest.mark.unit
def test_landing_filename_uses_batch_id() -> None:
    batch_id = "20260821T133045Z"
    assert mod.landing_filename("customers", batch_id) == "customers_20260821T133045Z.csv"
    assert mod.landing_paths("/Volumes/de_assessment/landing/raw", batch_id) == {
        "products": "/Volumes/de_assessment/landing/raw/products/products_20260821T133045Z.csv",
        "customers": "/Volumes/de_assessment/landing/raw/customers/customers_20260821T133045Z.csv",
        "orders": "/Volumes/de_assessment/landing/raw/orders/incoming/orders_20260821T133045Z.csv",
    }


@pytest.mark.unit
def test_landing_batch_id_is_utc_compact() -> None:
    assert mod.landing_batch_id(datetime(2026, 8, 21, 13, 30, 45, tzinfo=UTC)) == (
        "20260821T133045Z"
    )


@pytest.mark.unit
def test_generate_writes_csvs_with_small_scale(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(mod, "BASE_CUSTOMERS", 100)
    monkeypatch.setattr(mod, "BASE_PRODUCTS", 50)
    monkeypatch.setattr(mod, "BASE_ORDERS", 200)
    dq = {
        "null_emails": 5,
        "duplicate_customer_ids": 2,
        "null_order_customer_id": 4,
        "null_order_product_id": 6,
        "orphan_customer_id": 3,
        "orphan_product_id": 2,
        "duplicate_order_ids": 2,
    }
    monkeypatch.setattr(mod, "DQ_ISSUE_COUNTS", dq)

    stats = mod.generate(output_dir=tmp_path, batch_id="20260821T133045Z")

    customers = pd.read_csv(tmp_path / "customers_20260821T133045Z.csv")
    products = pd.read_csv(tmp_path / "products_20260821T133045Z.csv")
    orders = pd.read_csv(tmp_path / "orders_20260821T133045Z.csv")

    assert list(customers.columns) == mod.CUSTOMER_COLUMNS
    assert list(products.columns) == mod.PRODUCT_COLUMNS
    assert list(orders.columns) == mod.ORDER_COLUMNS

    assert len(customers) == 100 + dq["duplicate_customer_ids"]
    assert len(products) == 50
    assert len(orders) == 200 + dq["duplicate_order_ids"]

    assert customers["customer_id"].dtype in ("int64", "int32")
    assert int(customers["email"].isna().sum()) == dq["null_emails"]
    assert stats["null_emails"] == dq["null_emails"]
    assert stats["null_order_customer_id"] == dq["null_order_customer_id"]
    assert stats["null_order_product_id"] == dq["null_order_product_id"]
    assert stats["orphan_customer_id"] == dq["orphan_customer_id"]
    assert stats["orphan_product_id"] == dq["orphan_product_id"]
    assert stats["batch_id"] == "20260821T133045Z"
    assert stats["files"]["customers"].endswith("customers_20260821T133045Z.csv")

    assert set(customers["customer_segment"].unique()) <= set(mod.CUSTOMER_SEGMENTS)
    assert set(orders["order_status"].unique()) <= set(mod.ORDER_STATUSES)


@pytest.mark.unit
def test_null_pk_rows_are_appended_not_mutated() -> None:
    """A NULL primary key must not orphan existing children.

    Nulling a parent PK in place removes that key from the parent table, so
    every child row referencing it silently becomes an orphan. Measured on the
    committed dataset: 3 nulled products cascaded into 562 extra orphan
    order rows against a spec of 30, because 100k orders over 500 products
    means each product has ~200 children.

    Appending a fresh row with a NULL key gives the not_null check its data and
    cannot orphan anything, because the new row has no children.
    """
    df = pd.DataFrame(
        {
            "product_id": list(range(1, 11)),
            "product_name": [f"P{i}" for i in range(1, 11)],
            "category": ["Cat"] * 10,
            "price": [10.0] * 10,
            "cost": [5.0] * 10,
            "stock_quantity": [100] * 10,
            "reorder_level": [10] * 10,
        }
    )
    counts = mod.DQ_ISSUE_COUNTS.copy()
    mod.DQ_ISSUE_COUNTS.update(
        {k: 0 for k in mod.DQ_ISSUE_COUNTS if k.startswith(("negative", "overlong", "excessive", "duplicate"))}
    )
    mod.DQ_ISSUE_COUNTS["null_product_id_products"] = 2
    try:
        result = mod.inject_product_issues(df)
    finally:
        mod.DQ_ISSUE_COUNTS.clear()
        mod.DQ_ISSUE_COUNTS.update(counts)

    assert int(result["product_id"].isna().sum()) == 2, "the not_null check needs NULL keys"
    # Every original key survives, so no existing child is orphaned.
    surviving = set(result["product_id"].dropna().astype(int))
    assert set(range(1, 11)) <= surviving, f"original keys were destroyed: {surviving}"
    assert len(result) == 12, "NULL-key rows are appended, not overwritten"
