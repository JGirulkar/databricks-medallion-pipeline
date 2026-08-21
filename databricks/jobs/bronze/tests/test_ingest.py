from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from bronze.config import SourceConfig
from bronze.ingest import (
    BatchMetrics,
    append_batch,
    cloudfiles_options,
    metrics_from_sink,
    rows_written_from_history,
    run_ingest,
)
from bronze.main import parse_catalog


def _orders_config() -> SourceConfig:
    return SourceConfig(
        source_name="orders",
        target_table="orders_bronze",
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


@pytest.mark.unit
def test_cloudfiles_options() -> None:
    config = _orders_config()
    assert cloudfiles_options(config) == {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaLocation": config.schema_hint_path,
        "cloudFiles.inferColumnTypes": "false",
        "rescuedDataColumn": "_rescued_data",
        "header": "true",
        "nullValue": "",
        "emptyValue": "",
    }


@pytest.mark.unit
def test_parse_catalog_defaults() -> None:
    assert parse_catalog([]) == "de_assessment"


@pytest.mark.unit
def test_parse_catalog_explicit() -> None:
    assert parse_catalog(["--catalog", "test_catalog"]) == "test_catalog"


@pytest.mark.unit
@patch("bronze.ingest.F")
def test_append_batch_stamps_user_metadata(mock_f: MagicMock) -> None:
    mock_f.col.return_value.isNotNull.return_value = MagicMock(name="rescued_pred")
    config = _orders_config()
    df = MagicMock()
    df.count.return_value = 2
    df.select.return_value.distinct.return_value.collect.return_value = [
        MagicMock(_source_file="file-a.csv"),
    ]
    df.where.return_value.count.return_value = 0
    write_chain = df.write.format.return_value.mode.return_value
    write_chain.option.return_value = write_chain

    append_batch(df, config, "run-batch-1")

    write_chain.option.assert_any_call("mergeSchema", "false")
    write_chain.option.assert_any_call("userMetadata", "run-batch-1")
    write_chain.saveAsTable.assert_called_once_with(config.target_table)


@pytest.mark.unit
def test_append_batch_empty_is_noop() -> None:
    config = _orders_config()
    df = MagicMock()
    df.count.return_value = 0

    append_batch(df, config, "run-batch-1")

    df.write.format.assert_not_called()


@pytest.mark.unit
@patch("bronze.ingest.F")
def test_rows_written_from_history_sums_stamped_writes(mock_f: MagicMock) -> None:
    spark = MagicMock()
    history_df = MagicMock()
    spark.sql.return_value = history_df
    history_df.filter.return_value.collect.return_value = [
        MagicMock(operationMetrics='{"numOutputRows":"100"}'),
        MagicMock(operationMetrics='{"numOutputRows":"20"}'),
    ]
    mock_f.col.return_value.__gt__ = MagicMock(return_value=MagicMock())
    mock_f.col.return_value.__le__ = MagicMock(return_value=MagicMock())
    mock_f.lit.return_value = MagicMock()

    total = rows_written_from_history(
        spark,
        "de_assessment.bronze.orders",
        "batch-1",
        1,
        3,
    )

    assert total == 120
    history_df.filter.assert_called_once()


@pytest.mark.unit
def test_rows_written_from_history_returns_zero_when_no_version_change() -> None:
    spark = MagicMock()

    assert (
        rows_written_from_history(spark, "t", "batch-1", 2, 2) == 0
    )
    spark.sql.assert_not_called()


@pytest.mark.unit
@patch("bronze.ingest.rows_written_from_history", return_value=0)
def test_metrics_from_sink_empty_when_no_history_rows(
    _mock_history: MagicMock,
) -> None:
    spark = MagicMock()
    config = _orders_config()

    metrics = metrics_from_sink(spark, config, "batch-1", 0, 0)

    assert metrics == BatchMetrics.empty()
    spark.table.assert_not_called()


@pytest.mark.unit
@patch("bronze.ingest.F")
@patch("bronze.ingest.rows_written_from_history", return_value=5)
def test_metrics_from_sink_reads_landed_rows(
    _mock_history: MagicMock,
    mock_f: MagicMock,
) -> None:
    config = _orders_config()
    spark = MagicMock()
    batch_df = MagicMock()
    mock_f.col.return_value.__eq__ = MagicMock(return_value=MagicMock(name="batch_pred"))
    mock_f.col.return_value.isNotNull.return_value = MagicMock(name="rescued_pred")
    spark.table.return_value.filter.return_value = batch_df
    batch_df.count.return_value = 5
    batch_df.filter.return_value.count.return_value = 1
    batch_df.select.return_value.distinct.return_value.collect.return_value = [
        MagicMock(_source_file="incoming/a.csv"),
        MagicMock(_source_file="incoming/b.csv"),
    ]

    metrics = metrics_from_sink(spark, config, "batch-1", 1, 2)

    assert metrics.rows_written == 5
    assert metrics.rows_read == 5
    assert metrics.rows_rescued == 1
    assert metrics.files == frozenset({"incoming/a.csv", "incoming/b.csv"})


@pytest.mark.unit
@patch("bronze.ingest.append_batch")
def test_run_ingest_aggregates_batches_and_archives_on_success(
    mock_append_batch: MagicMock,
) -> None:
    config = _orders_config()
    spark = MagicMock()
    spark.sql.return_value.select.return_value.collect.return_value = []
    archived: list[tuple[str, str]] = []

    def stream_runner(callback) -> None:
        callback(MagicMock(), 0)
        callback(MagicMock(), 1)

    def fake_sink_metrics(
        _spark: MagicMock,
        _config: SourceConfig,
        batch_id: str,
        _before: int | None,
        _after: int | None,
    ) -> BatchMetrics:
        assert batch_id == "fixed-batch"
        return BatchMetrics(
            frozenset(["incoming/a.csv", "incoming/b.csv"]),
            3,
            3,
            0,
        )

    record = run_ingest(
        spark,
        config,
        archive_file=lambda src, dest: archived.append((src, dest)),
        batch_id_factory=lambda: "fixed-batch",
        clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        stream_runner=stream_runner,
        sink_metrics=fake_sink_metrics,
    )

    assert record.batch_id == "fixed-batch"
    assert record.files_processed == 2
    assert record.rows_read == 3
    assert record.rows_written == 3
    assert record.status == "success"
    assert len(archived) == 2


@pytest.mark.unit
def test_run_ingest_failure_writes_failed_manifest_and_reraises() -> None:
    config = _orders_config()
    spark = MagicMock()
    spark.sql.return_value.select.return_value.collect.return_value = []

    def stream_runner(_callback) -> None:
        raise RuntimeError("stream failed")

    with pytest.raises(RuntimeError, match="stream failed"):
        run_ingest(
            spark,
            config,
            batch_id_factory=lambda: "failed-batch",
            clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
            stream_runner=stream_runner,
            sink_metrics=lambda *_args, **_kwargs: BatchMetrics.empty(),
        )

    create_df = spark.createDataFrame.call_args[0][0][0]
    assert create_df["status"] == "failed"
    assert create_df["batch_id"] == "failed-batch"
