"""Silver conform orchestration — entity and conform_all runners."""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.checks import apply_entity_checks
from silver.config import (
    DEFAULT_CATALOG,
    ORCHESTRATION_ORDER,
    get_delivery_pattern,
    load_dq_schema,
    silver_checkpoint_path,
    silver_table,
)
from silver.conform import (
    apply_snapshot_soft_deletes,
    conform_incremental_batch,
    conform_snapshot_batch,
    merge_to_silver,
)
from silver.cdf import filter_cdf_post_images, run_cdf_stream
from silver.job_log import configure_job_logger
from silver.manifest import PipelineManifestRecord, append_silver_manifest, current_delta_version
from silver.metrics import append_dq_metrics, build_metric_row
from silver.quarantine import write_quarantine
from silver.validators import annotate_violations

LOG = configure_job_logger("silver.main")

PARENT_ENTITIES_FOR_ORDERS: tuple[str, ...] = ("products", "customers")


def parse_catalog(argv: Sequence[str] | None = None) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    return parser.parse_args(list(argv) if argv is not None else None).catalog


def new_run_id() -> str:
    return str(uuid.uuid4())


def _category_metrics(
    evaluated_df: DataFrame,
    passed_df: DataFrame,
    failed_df: DataFrame,
    run_id: str,
    entity_name: str,
    run_at: datetime,
) -> list[dict[str, object]]:
    categories = ["completeness", "uniqueness", "type_logic", "referential"]
    rows_evaluated = evaluated_df.count()
    rows_passed = passed_df.count()
    rows_quarantined = failed_df.count()
    return [
        build_metric_row(
            run_id,
            entity_name,
            category,
            rows_evaluated,
            rows_passed,
            rows_quarantined,
            run_at,
        )
        for category in categories
    ]


def process_conform_batch(
    spark: SparkSession,
    entity_name: str,
    batch_df: DataFrame,
    run_id: str,
    catalog: str,
    parent_run_id: str | None,
) -> tuple[int, int, int]:
    del parent_run_id
    if not batch_df.take(1):
        return 0, 0, 0

    cdf_df = filter_cdf_post_images(batch_df)
    if not cdf_df.take(1):
        return 0, 0, 0

    delivery_pattern = get_delivery_pattern(spark, entity_name, catalog)
    if delivery_pattern == "full_snapshot":
        conformed = conform_snapshot_batch(cdf_df, entity_name, spark, catalog)
    else:
        conformed = conform_incremental_batch(cdf_df, entity_name, spark, catalog)

    dq_schema = load_dq_schema(spark, entity_name, catalog)
    validated = annotate_violations(conformed, dq_schema)
    validated = apply_entity_checks(validated, dq_schema, spark, catalog)

    failed = validated.filter(F.size(F.col("_violations")) > 0)
    passed = validated.filter(F.size(F.col("_violations")) == 0)

    rows_read = conformed.count()
    rows_written = merge_to_silver(passed, entity_name, spark, catalog)
    if delivery_pattern == "full_snapshot":
        apply_snapshot_soft_deletes(spark, entity_name, conformed, catalog)

    run_at = datetime.now(UTC)
    rows_quarantined = write_quarantine(
        spark, failed, entity_name, run_id, run_at, catalog
    )
    append_dq_metrics(
        spark,
        _category_metrics(conformed, passed, failed, run_id, entity_name, run_at),
        catalog,
    )
    return rows_read, rows_written, rows_quarantined


def run_entity_conform(
    spark: SparkSession,
    entity_name: str,
    catalog: str = DEFAULT_CATALOG,
    parent_run_id: str | None = None,
    stream_runner: Callable[..., None] | None = None,
) -> str:
    run_id = new_run_id()
    started_at = datetime.now(UTC)
    target = silver_table(entity_name, catalog)
    version_before = current_delta_version(spark, target)
    delivery_pattern = get_delivery_pattern(spark, entity_name, catalog)
    source_path = silver_checkpoint_path(entity_name, catalog)

    totals = {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}

    def on_batch(batch_df: DataFrame, _batch_id: int) -> None:
        rows_read, rows_written, rows_quarantined = process_conform_batch(
            spark, entity_name, batch_df, run_id, catalog, parent_run_id
        )
        totals["rows_read"] += rows_read
        totals["rows_written"] += rows_written
        totals["rows_quarantined"] += rows_quarantined

    try:
        if stream_runner is not None:
            stream_runner(on_batch)
        else:
            run_cdf_stream(spark, entity_name, on_batch, catalog)

        version_after = current_delta_version(spark, target)
        append_silver_manifest(
            spark,
            PipelineManifestRecord(
                run_id=run_id,
                entity_name=entity_name,
                parent_run_id=parent_run_id,
                delivery_pattern=delivery_pattern,
                source_path=source_path,
                files_processed=0,
                rows_read=totals["rows_read"],
                rows_written=totals["rows_written"],
                rows_quarantined=totals["rows_quarantined"],
                rows_rescued=0,
                delta_version_before=version_before,
                delta_version_after=version_after,
                started_at=started_at,
                completed_at=datetime.now(UTC),
                status="success",
                error_message=None,
            ),
            catalog,
        )
    except Exception as exc:
        append_silver_manifest(
            spark,
            PipelineManifestRecord(
                run_id=run_id,
                entity_name=entity_name,
                parent_run_id=parent_run_id,
                delivery_pattern=delivery_pattern,
                source_path=source_path,
                files_processed=0,
                rows_read=totals["rows_read"],
                rows_written=totals["rows_written"],
                rows_quarantined=totals["rows_quarantined"],
                rows_rescued=0,
                delta_version_before=version_before,
                delta_version_after=version_before,
                started_at=started_at,
                completed_at=datetime.now(UTC),
                status="failed",
                error_message=str(exc),
            ),
            catalog,
        )
        raise

    return run_id


def run_orders_conform_with_parent_refresh(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> str:
    """Drain parent dimension CDF before orders FK checks (per-entity job model)."""
    parent_run_id = new_run_id()
    LOG.info("orders_parent_refresh_start parent_run_id=%s catalog=%s", parent_run_id, catalog)
    for entity in PARENT_ENTITIES_FOR_ORDERS:
        LOG.info("orders_parent_refresh entity=%s", entity)
        try:
            run_entity_conform(spark, entity, catalog, parent_run_id=parent_run_id)
        except Exception:
            LOG.exception("orders_parent_refresh_failed entity=%s continuing", entity)
    LOG.info("orders_conform_start parent_run_id=%s", parent_run_id)
    return run_entity_conform(spark, "orders", catalog, parent_run_id=parent_run_id)


def run_conform_all(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> None:
    parent_run_id = new_run_id()
    for entity in ORCHESTRATION_ORDER:
        LOG.info("conform_entity_start entity=%s parent_run_id=%s", entity, parent_run_id)
        try:
            run_entity_conform(spark, entity, catalog, parent_run_id=parent_run_id)
        except Exception:
            LOG.exception("conform_entity_failed entity=%s", entity)
            raise
