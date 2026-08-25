"""Task 3: Idempotent UC Bootstrap — contract tests (all unit, no Spark needed)."""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# DDL contract tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bootstrap_ddl_creates_catalog() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "CREATE CATALOG IF NOT EXISTS de_assessment" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_creates_landing_volume() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "CREATE VOLUME IF NOT EXISTS de_assessment.landing.raw" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_creates_checkpoints_volume() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "CREATE VOLUME IF NOT EXISTS de_assessment.ops.checkpoints" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_enables_cdf_on_entity_tables() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "delta.enableChangeDataFeed' = 'true" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_has_no_merge_into() -> None:
    from bronze.bootstrap import bootstrap_ddl

    assert "MERGE INTO" not in "\n".join(bootstrap_ddl())


@pytest.mark.unit
def test_bootstrap_ddl_custom_catalog() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl("test_catalog"))
    assert "CREATE CATALOG IF NOT EXISTS test_catalog" in ddl
    assert "test_catalog.bronze" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_returns_tuple_of_strings() -> None:
    from bronze.bootstrap import bootstrap_ddl

    result = bootstrap_ddl()
    assert isinstance(result, tuple)
    assert all(isinstance(s, str) for s in result)


@pytest.mark.unit
def test_bootstrap_ddl_creates_config_schema() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "CREATE SCHEMA IF NOT EXISTS de_assessment.config" in ddl
    assert "de_assessment.config.source_config" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_creates_pipeline_manifest() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "de_assessment.ops.pipeline_manifest" in ddl
    assert "rows_quarantined BIGINT NOT NULL" in ddl


@pytest.mark.unit
def test_bootstrap_ddl_source_config_has_column_defaults() -> None:
    from bronze.bootstrap import bootstrap_ddl

    ddl = "\n".join(bootstrap_ddl())
    assert "delta.feature.allowColumnDefaults" in ddl


# ---------------------------------------------------------------------------
# Seed row contract tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_source_seed_rows_returns_exactly_three() -> None:
    from bronze.bootstrap import source_seed_rows

    rows = source_seed_rows()
    assert len(rows) == 3


@pytest.mark.unit
def test_source_seed_rows_delivery_patterns_valid() -> None:
    from bronze.bootstrap import source_seed_rows

    valid = {"full_snapshot", "incremental"}
    rows = source_seed_rows()
    for row in rows:
        assert row["delivery_pattern"] in valid, (
            f"row {row['source_name']!r} has invalid delivery_pattern: "
            f"{row['delivery_pattern']!r}"
        )


@pytest.mark.unit
def test_source_seed_rows_source_names() -> None:
    from bronze.bootstrap import source_seed_rows

    rows = source_seed_rows()
    names = {row["source_name"] for row in rows}
    assert names == {"customers", "orders", "products"}


@pytest.mark.unit
def test_source_seed_rows_required_keys_present() -> None:
    from bronze.bootstrap import source_seed_rows

    required = {
        "source_name",
        "target_table",
        "raw_path",
        "checkpoint_path",
        "schema_hint_path",
        "archive_path",
        "file_format",
        "delivery_pattern",
        "cdf_enabled",
        "schedule_hint",
        "is_active",
    }
    for row in source_seed_rows():
        assert required <= set(row), f"Missing keys in {row['source_name']!r} row"


@pytest.mark.unit
def test_source_seed_rows_raw_paths_are_volumes() -> None:
    from bronze.bootstrap import source_seed_rows

    for row in source_seed_rows():
        assert row["raw_path"].startswith("/Volumes/"), (
            f"{row['source_name']!r} raw_path must be a UC Volume path"
        )


# ---------------------------------------------------------------------------
# Seed MERGE SQL — non-destructive (WHEN NOT MATCHED only)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_seed_merge_sql_when_not_matched_present() -> None:
    from bronze.bootstrap import _SEED_MERGE_SQL

    assert "WHEN NOT MATCHED THEN INSERT" in _SEED_MERGE_SQL


@pytest.mark.unit
def test_seed_merge_sql_no_when_matched_update() -> None:
    from bronze.bootstrap import _SEED_MERGE_SQL

    assert "WHEN MATCHED THEN UPDATE" not in _SEED_MERGE_SQL


@pytest.mark.unit
def test_seed_merge_sql_references_source_config() -> None:
    from bronze.bootstrap import _SEED_MERGE_SQL, _SEED_MERGE_TARGET

    assert _SEED_MERGE_TARGET in _SEED_MERGE_SQL


# ---------------------------------------------------------------------------
# bootstrap() behaviour with mocked Spark (no cluster)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_bootstrap_calls_mkdirs_for_all_paths() -> None:
    from unittest.mock import MagicMock

    from bronze.bootstrap import _DIRS, bootstrap

    spark = MagicMock()
    mkdirs = MagicMock()
    bootstrap(spark, mkdirs, catalog="de_assessment")

    called_paths = {call.args[0] for call in mkdirs.call_args_list}
    assert called_paths == set(_DIRS)


@pytest.mark.unit
def test_bootstrap_executes_ddl_and_merge() -> None:
    from unittest.mock import MagicMock

    from bronze.bootstrap import bootstrap, bootstrap_ddl

    spark = MagicMock()
    mkdirs = MagicMock()
    bootstrap(spark, mkdirs, catalog="de_assessment")

    expected_sql_calls = len(bootstrap_ddl()) + 1  # +1 for the MERGE
    assert spark.sql.call_count == expected_sql_calls


@pytest.mark.unit
def test_bootstrap_creates_temp_view_for_seed() -> None:
    from unittest.mock import MagicMock

    from bronze.bootstrap import bootstrap

    spark = MagicMock()
    mkdirs = MagicMock()
    bootstrap(spark, mkdirs, catalog="de_assessment")

    seed_df_mock = spark.createDataFrame.return_value
    seed_df_mock.createOrReplaceTempView.assert_called_once_with("bronze_source_seed")
