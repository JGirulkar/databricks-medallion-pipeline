from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bronze.config import DEFAULT_CATALOG, SourceConfig, get_source_config
from bronze.job_log import configure_job_logger
from bronze.manifest import ManifestRecord, append_manifest, current_delta_version
from bronze.metadata import add_ingest_metadata
from bronze.schemas import source_schema

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LOG = configure_job_logger("bronze.ingest")

BatchCallback = Callable[[DataFrame, int], None]
StreamRunner = Callable[[BatchCallback], None]
ArchiveFile = Callable[[str, str], None]
BatchIdFactory = Callable[[], str]
Clock = Callable[[], datetime]
SinkMetricsFn = Callable[
    ["SparkSession", SourceConfig, str, int | None, int | None], "BatchMetrics"
]


@dataclass(frozen=True)
class BatchMetrics:
    files: frozenset[str]
    rows_read: int
    rows_written: int
    rows_rescued: int

    @staticmethod
    def empty() -> BatchMetrics:
        return BatchMetrics(frozenset(), 0, 0, 0)


def cloudfiles_options(config: SourceConfig) -> dict[str, str]:
    return {
        "cloudFiles.format": config.file_format,
        "cloudFiles.schemaLocation": config.schema_hint_path,
        "cloudFiles.inferColumnTypes": "false",
        "rescuedDataColumn": "_rescued_data",
        "header": "true",
        "nullValue": "",
        "emptyValue": "",
    }


def _parse_operation_metrics(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, str):
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    return {}


def rows_written_from_history(
    spark: SparkSession,
    table_name: str,
    batch_id: str,
    version_before: int | None,
    version_after: int | None,
) -> int:
    """Sum numOutputRows for WRITE commits stamped with this run's batch_id."""
    if (
        version_before is None
        or version_after is None
        or version_after <= version_before
    ):
        return 0

    history = spark.sql(f"DESCRIBE HISTORY {table_name}")
    writes = history.filter(
        (F.col("operation") == F.lit("WRITE"))
        & (F.col("userMetadata") == F.lit(batch_id))
        & (F.col("version") > F.lit(version_before))
        & (F.col("version") <= F.lit(version_after))
    ).collect()

    total = 0
    for row in writes:
        metrics = _parse_operation_metrics(row.operationMetrics)
        total += int(metrics.get("numOutputRows", 0))

    if total == 0 and version_after > version_before:
        LOG.warning(
            "history_rows_missing_user_metadata table=%s batch_id=%s "
            "version_before=%s version_after=%s",
            table_name,
            batch_id,
            version_before,
            version_after,
        )
    return total


def metrics_from_sink(
    spark: SparkSession,
    config: SourceConfig,
    batch_id: str,
    version_before: int | None,
    version_after: int | None,
) -> BatchMetrics:
    """Derive manifest metrics from Delta history and landed rows (driver-side)."""
    rows_written = rows_written_from_history(
        spark,
        config.target_table,
        batch_id,
        version_before,
        version_after,
    )
    if rows_written == 0:
        return BatchMetrics.empty()

    batch_rows = (
        spark.table(config.target_table)
        .filter(F.col("_batch_id") == batch_id)
    )
    rows_read = batch_rows.count()
    rows_rescued = batch_rows.filter(F.col("_rescued_data").isNotNull()).count()
    files = frozenset(
        row._source_file
        for row in batch_rows.select("_source_file").distinct().collect()
    )
    LOG.info(
        "metrics_from_sink target=%s batch_id=%s rows_written=%s rows_read=%s "
        "files=%s rescued=%s",
        config.target_table,
        batch_id,
        rows_written,
        rows_read,
        len(files),
        rows_rescued,
    )
    return BatchMetrics(files, rows_read, rows_written, rows_rescued)


def append_batch(df: DataFrame, config: SourceConfig, batch_id: str) -> None:
    """Write one Auto Loader micro-batch; metrics are resolved on the driver after the stream."""
    try:
        rows_read = df.count()
        if rows_read == 0:
            LOG.info("append_batch_skip_empty target=%s", config.target_table)
            return
        files = frozenset(
            row._source_file
            for row in df.select("_source_file").distinct().collect()
        )
        rows_rescued = df.where(F.col("_rescued_data").isNotNull()).count()
        LOG.info(
            "append_batch_write target=%s batch_id=%s rows=%s files=%s rescued=%s",
            config.target_table,
            batch_id,
            rows_read,
            len(files),
            rows_rescued,
        )
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .option("userMetadata", batch_id)
            .saveAsTable(config.target_table)
        )
    except Exception:
        LOG.exception("append_batch_failed target=%s batch_id=%s", config.target_table, batch_id)
        raise


def _archive_processed_files(
    config: SourceConfig,
    files: frozenset[str],
    archive_file: ArchiveFile,
) -> None:
    if config.archive_path is None or not files:
        return
    archive_root = config.archive_path.rstrip("/")
    for source in files:
        destination = f"{archive_root}/{Path(source).name}"
        archive_file(source, destination)


def _run_autoloader_stream(
    spark: SparkSession,
    config: SourceConfig,
    batch_id: str,
    started_at: datetime,
    on_batch: BatchCallback,
) -> None:
    LOG.info(
        "autoloader_start source=%s raw_path=%s checkpoint=%s batch_id=%s",
        config.source_name,
        config.raw_path,
        config.checkpoint_path,
        batch_id,
    )
    try:
        raw = (
            spark.readStream.format("cloudFiles")
            .options(**cloudfiles_options(config))
            .schema(source_schema(config.source_name))
            .load(config.raw_path)
        )
        enriched = add_ingest_metadata(
            raw,
            config,
            batch_id=batch_id,
            ingest_timestamp=started_at,
        )

        def write_micro_batch(df: DataFrame, batch_id: int) -> None:
            on_batch(df, batch_id)

        query = (
            enriched.writeStream.foreachBatch(write_micro_batch)
            .option("checkpointLocation", config.checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        LOG.info("autoloader_complete source=%s batch_id=%s", config.source_name, batch_id)
    except Exception:
        LOG.exception(
            "autoloader_failed source=%s raw_path=%s batch_id=%s",
            config.source_name,
            config.raw_path,
            batch_id,
        )
        raise


def run_ingest(
    spark: SparkSession,
    config: SourceConfig,
    *,
    archive_file: ArchiveFile | None = None,
    batch_id_factory: BatchIdFactory | None = None,
    clock: Clock | None = None,
    stream_runner: StreamRunner | None = None,
    sink_metrics: SinkMetricsFn | None = None,
    catalog: str = DEFAULT_CATALOG,
) -> ManifestRecord:
    batch_id = (batch_id_factory or (lambda: str(uuid4())))()
    started_at = (clock or (lambda: datetime.now(UTC)))()
    resolve_metrics = sink_metrics or metrics_from_sink
    LOG.info(
        "run_ingest_start source=%s catalog=%s target=%s raw_path=%s batch_id=%s",
        config.source_name,
        catalog,
        config.target_table,
        config.raw_path,
        batch_id,
    )
    version_before = current_delta_version(spark, config.target_table)

    def on_batch(df: DataFrame, _batch_id: int) -> None:
        append_batch(df, config, batch_id)

    try:
        if stream_runner is not None:
            stream_runner(on_batch)
        else:
            _run_autoloader_stream(spark, config, batch_id, started_at, on_batch)

        completed_at = (clock or (lambda: datetime.now(UTC)))()
        version_after = current_delta_version(spark, config.target_table)
        totals = resolve_metrics(
            spark, config, batch_id, version_before, version_after
        )
        record = ManifestRecord(
            batch_id=batch_id,
            source_name=config.source_name,
            delivery_pattern=config.delivery_pattern,
            source_path=config.raw_path,
            files_processed=len(totals.files),
            rows_read=totals.rows_read,
            rows_written=totals.rows_written,
            rows_rescued=totals.rows_rescued,
            delta_version_before=version_before,
            delta_version_after=version_after,
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            error_message=None,
        )
        append_manifest(spark, record, catalog=catalog)
        if archive_file is not None:
            _archive_processed_files(config, totals.files, archive_file)
        LOG.info(
            "run_ingest_success source=%s batch_id=%s rows_written=%s files=%s",
            config.source_name,
            batch_id,
            record.rows_written,
            record.files_processed,
        )
        return record
    except Exception as exc:
        LOG.exception(
            "run_ingest_failed source=%s batch_id=%s error=%s",
            config.source_name,
            batch_id,
            exc,
        )
        failed_at = (clock or (lambda: datetime.now(UTC)))()
        version_after = current_delta_version(spark, config.target_table)
        totals = resolve_metrics(
            spark, config, batch_id, version_before, version_after
        )
        failed = ManifestRecord(
            batch_id=batch_id,
            source_name=config.source_name,
            delivery_pattern=config.delivery_pattern,
            source_path=config.raw_path,
            files_processed=len(totals.files),
            rows_read=totals.rows_read,
            rows_written=totals.rows_written,
            rows_rescued=totals.rows_rescued,
            delta_version_before=version_before,
            delta_version_after=version_after,
            started_at=started_at,
            completed_at=failed_at,
            status="failed",
            error_message=str(exc),
        )
        append_manifest(spark, failed, catalog=catalog)
        raise


def active_spark() -> SparkSession:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active Spark session")
    return spark


def run_source(source_name: str, catalog: str = DEFAULT_CATALOG) -> None:
    LOG.info("run_source_start source=%s catalog=%s", source_name, catalog)
    try:
        spark = active_spark()
        config = get_source_config(spark, source_name, catalog=catalog)
        LOG.info(
            "run_source_config_loaded source=%s target=%s raw_path=%s",
            source_name,
            config.target_table,
            config.raw_path,
        )

        def archive_file(source: str, destination: str) -> None:
            from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

            LOG.info("archive_file source=%s destination=%s", source, destination)
            try:
                DBUtils(spark).fs.mv(source, destination)
            except Exception:
                LOG.exception(
                    "archive_file_failed source=%s destination=%s", source, destination
                )
                raise

        run_ingest(spark, config, archive_file=archive_file, catalog=catalog)
    except Exception:
        LOG.exception("run_source_failed source=%s catalog=%s", source_name, catalog)
        raise
