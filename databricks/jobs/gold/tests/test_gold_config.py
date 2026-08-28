"""Unit tier: the SQL files and their loading/substitution contract."""

from __future__ import annotations

import re

import pytest
from gold.config import (
    GOLD_SQL_FILES,
    GOLD_TABLES,
    HIGH_VALUE_REVENUE,
    INACTIVE_DAYS,
    load_sql,
    render_sql,
)

pytestmark = pytest.mark.unit


def test_the_four_files_exist_in_execution_order() -> None:
    assert GOLD_SQL_FILES == (
        "01_sales_by_product.sql",
        "02_revenue_by_customer.sql",
        "03_daily_weekly_trends.sql",
        "04_customer_segmentation.sql",
    )
    for name in GOLD_SQL_FILES:
        assert load_sql(name).strip(), f"{name} is empty"


def test_render_substitutes_every_placeholder() -> None:
    for name in GOLD_SQL_FILES:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test")
        leftover = re.findall(r"\{[a-z_]+\}", rendered)
        assert not leftover, f"{name} has unsubstituted placeholders: {leftover}"


def test_each_file_replaces_exactly_its_own_table() -> None:
    for name, table in zip(GOLD_SQL_FILES, GOLD_TABLES):
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test")
        assert f"create or replace table g_test.{table}" in rendered.lower()


def test_aggregation_files_read_the_shared_view_not_raw_orders() -> None:
    # The business rule lives in the runner's qualifying_orders view. A file
    # that reads silver.orders directly has smuggled in its own rule.
    for name in GOLD_SQL_FILES:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test").lower()
        assert "s_test.orders" not in rendered, f"{name} bypasses qualifying_orders"
    for name in GOLD_SQL_FILES[:3]:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test").lower()
        assert "qualifying_orders" in rendered


def test_segmentation_derives_from_revenue_by_customer() -> None:
    # Cross-footing by construction: 04 reads the table 02 built.
    rendered = render_sql(load_sql(GOLD_SQL_FILES[3]), silver="s_test", gold="g_test").lower()
    assert "g_test.revenue_by_customer" in rendered
    assert "qualifying_orders" not in rendered


def test_thresholds_are_the_pinned_constants() -> None:
    assert INACTIVE_DAYS == 90
    assert HIGH_VALUE_REVENUE == 5000
    rendered = render_sql(load_sql(GOLD_SQL_FILES[3]), silver="s", gold="g")
    assert "90" in rendered and "5000" in rendered
