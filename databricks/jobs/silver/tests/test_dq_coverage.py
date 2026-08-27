"""Every validation rule the seed declares must have a bad-row scenario.

One E2E run should exercise the whole validator surface. Without this gate a
rule can be added to the dq_schema seed while the generator produces no row
that violates it — the rule then reports 100% pass forever and nobody notices
it was never tested.

When this fails, either add a generator scenario for the new rule or record
here, with a reason, that it is deliberately unexercised.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from silver.bootstrap import _dq_schema_seeds
from silver.config import DqSchema
from silver.validators import column_predicates

# (entity, column, rule) -> the generator issue key that violates it.
EXERCISED_BY_GENERATOR: dict[tuple[str, str, str], str] = {
    ("customers", "email", "format_email"): "invalid_emails",
    ("customers", "customer_name", "min_length"): "short_customer_name",
    ("customers", "customer_name", "max_length"): "overlong_customer_name",
    ("customers", "country", "pattern"): "invalid_country",
    ("customers", "customer_segment", "enum"): "invalid_customer_segment",
    ("customers", "signup_date", "max_date"): "future_signup_date",
    ("customers", "lifetime_value", "minimum"): "negative_lifetime_value",
    ("products", "price", "minimum"): "negative_price",
    ("products", "cost", "minimum"): "negative_cost",
    ("products", "product_name", "max_length"): "overlong_product_name",
    ("products", "stock_quantity", "minimum"): "negative_stock_quantity",
    ("products", "stock_quantity", "maximum"): "excessive_stock_quantity",
    ("orders", "quantity", "minimum"): "non_positive_quantity",
    ("orders", "quantity", "maximum"): "excessive_quantity",
    ("orders", "unit_price", "exclusive_minimum"): "zero_unit_price",
    ("orders", "total_amount", "minimum"): "negative_total_amount",
    ("orders", "order_status", "enum"): "invalid_order_status",
    ("orders", "order_date", "min_date"): "pre_launch_order_date",
    ("orders", "order_date", "max_date"): "future_order_date",
}

# Entity-level checks, exercised by whole-row scenarios rather than a column rule.
EXERCISED_ENTITY_CHECKS: dict[tuple[str, str, str], str] = {
    ("customers", "customer_id", "not_null"): "null_customer_id",
    ("customers", "customer_id", "uniqueness"): "duplicate_customer_ids",
    ("customers", "email", "not_null"): "null_emails",
    ("products", "product_id", "not_null"): "null_product_id_products",
    ("products", "product_id", "uniqueness"): "duplicate_product_ids",
    ("orders", "order_id", "not_null"): "null_order_id",
    ("orders", "order_id", "uniqueness"): "duplicate_order_ids",
    ("orders", "customer_id", "not_null"): "null_order_customer_id",
    ("orders", "product_id", "not_null"): "null_order_product_id",
    ("orders", "customer_id", "fk_exists"): "orphan_customer_id",
    ("orders", "product_id", "fk_exists"): "orphan_product_id",
}

# Rules the framework supports but this dataset deliberately does not declare.
# multiple_of / exclusive_maximum have no natural meaning for these three
# e-commerce entities, so declaring them would be validator theatre.
DELIBERATELY_NOT_DECLARED = frozenset({"multiple_of", "exclusive_maximum"})


def _declared_column_rules() -> set[tuple[str, str, str]]:
    declared: set[tuple[str, str, str]] = set()
    for entity, raw in _dq_schema_seeds("de_assessment").items():
        schema = DqSchema.from_dict(raw)
        for column in schema.columns:
            for _category, rule_name, _predicate in column_predicates(column):
                declared.add((entity, column.name, rule_name))
    return declared


def _declared_entity_checks() -> set[tuple[str, str, str]]:
    return {
        (entity, check.column, check.kind)
        for entity, raw in _dq_schema_seeds("de_assessment").items()
        for check in DqSchema.from_dict(raw).checks
    }


@pytest.mark.spark
def test_every_declared_column_rule_has_a_generator_scenario(
    spark: SparkSession,
) -> None:
    del spark  # column_predicates builds Columns, which needs an active session
    missing = _declared_column_rules() - set(EXERCISED_BY_GENERATOR)
    assert not missing, (
        "dq_schema declares column rules with no bad-row scenario, so they "
        f"can never fail in an E2E run: {sorted(missing)}"
    )


@pytest.mark.unit
def test_every_declared_entity_check_has_a_generator_scenario() -> None:
    missing = _declared_entity_checks() - set(EXERCISED_ENTITY_CHECKS)
    assert not missing, (
        f"dq_schema declares entity checks with no bad-row scenario: {sorted(missing)}"
    )


@pytest.mark.spark
def test_coverage_map_has_no_stale_entries(spark: SparkSession) -> None:
    """Guards the other direction: a scenario mapped to a rule nobody declares."""
    del spark
    stale_columns = set(EXERCISED_BY_GENERATOR) - _declared_column_rules()
    stale_checks = set(EXERCISED_ENTITY_CHECKS) - _declared_entity_checks()
    assert not stale_columns, f"coverage map references undeclared rules: {sorted(stale_columns)}"
    assert not stale_checks, f"coverage map references undeclared checks: {sorted(stale_checks)}"


def _generator_issue_keys() -> set[str]:
    """Load the data-generation module by path — it is a separate uv workspace member."""
    module_path = (
        Path(__file__).resolve().parents[2]
        / "data_generation"
        / "src"
        / "generate_sample_data.py"
    )
    spec = importlib.util.spec_from_file_location("generate_sample_data", module_path)
    assert spec and spec.loader, f"cannot load generator at {module_path}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return set(module.DQ_ISSUE_COUNTS)


@pytest.mark.unit
def test_every_mapped_scenario_exists_in_the_generator() -> None:
    """Closes the loop: the scenario names must be real generator issue keys.

    Without this, the coverage map can name a scenario that was never
    implemented and the maps still agree with the seed.
    """
    mapped = set(EXERCISED_BY_GENERATOR.values()) | set(EXERCISED_ENTITY_CHECKS.values())
    missing = mapped - _generator_issue_keys()
    assert not missing, (
        "coverage map names generator scenarios that do not exist in "
        f"DQ_ISSUE_COUNTS: {sorted(missing)}"
    )
