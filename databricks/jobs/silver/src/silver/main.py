"""Silver conform orchestration — entity and conform_all runners."""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from silver.cdf import filter_cdf_post_images, run_cdf_stream
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
    heal_orphans,
    merge_to_silver,
    split_validated_batch,
)
from silver.job_log import configure_job_logger
from silver.manifest import (
    PipelineManifestRecord,
    append_silver_manifest,
    current_delta_version,
)
from silver.metrics import append_dq_metrics, build_metric_row
from silver.quarantine import write_quarantine
from silver.sink_metrics import resolve_silver_metrics
from silver.validators import annotate_violations

LOG = configure_job_logger("silver.main")

# Conforming one of these can resolve an orphan, so healing runs afterwards.
_PARENT_ENTITIES: frozenset[str] = frozenset({"customers", "products"})

def parse_catalog(argv: Sequence[str] | None = None) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    return parser.parse_args(list(argv) if argv is not None else None).catalog


def new_run_id() -> str:
    return str(uuid.uuid4())


DQ_CATEGORIES: tuple[str, ...] = (
    "completeness",
    "uniqueness",
    "type_logic",
    "referential",
)


def _has_category(category: str) -> Callable[[Column], Column]:
    """Return a predicate testing one violation category.

    A factory rather than an inline lambda: a lambda written inside the loop
    below would close over the loop variable and late-bind it, so every
    category would end up testing the last one. Passing `category` as a call
    argument binds it per call.
    """
    return lambda violation: violation["category"] == F.lit(category)


def _category_metrics(
    tagged_df: DataFrame,
    run_id: str,
    entity_name: str,
    run_at: datetime,
) -> list[dict[str, object]]:
    """One metric row per check category, each with its own counts.

    The previous implementation counted the batch once and reused the same
    three numbers for all four categories, so the "% passed per check" report
    showed an identical pass rate for every check. Here each category is
    counted from the rows whose `_violations` actually contain it.

    rows_quarantined is the number of rows that failed THAT check. A row can
    fail several checks and is counted under each, so the categories do not
    sum to the batch size — which is correct for a per-check report.

    Computed in a SINGLE pass: one aggregation with a conditional sum per
    category. A filter().count() per category rescans the batch once per
    category, and caching to avoid that is not available — serverless compute
    rejects PERSIST with NOT_SUPPORTED_WITH_SERVERLESS.
    """
    aggregates = [F.count(F.lit(1)).alias("rows_evaluated")]
    aggregates += [
        F.sum(
            F.when(
                F.exists(F.col("_violations"), _has_category(category)), F.lit(1)
            ).otherwise(F.lit(0))
        ).alias(f"failed_{category}")
        for category in DQ_CATEGORIES
    ]
    summary = tagged_df.agg(*aggregates).collect()[0]
    rows_evaluated = int(summary["rows_evaluated"] or 0)
    return [
        build_metric_row(
            run_id,
            entity_name,
            category,
            rows_evaluated,
            rows_evaluated - int(summary[f"failed_{category}"] or 0),
            int(summary[f"failed_{category}"] or 0),
            run_at,
        )
        for category in DQ_CATEGORIES
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
    dq_schema = load_dq_schema(spark, entity_name, catalog)

    # Validate the FULL batch before survivorship. Deduping first hides every
    # duplicate from the uniqueness check.
    tagged = annotate_violations(cdf_df, dq_schema)
    tagged = apply_entity_checks(tagged, dq_schema, spark, catalog)

    survivors, passed, failed = split_validated_batch(tagged, entity_name)

    # One aggregation covers both the per-check report and rows_read, so the
    # batch is scanned once for metrics rather than once per category.
    run_at = datetime.now(UTC)
    metrics = _category_metrics(tagged, run_id, entity_name, run_at)
    rows_read = int(metrics[0]["rows_evaluated"]) if metrics else 0

    rows_written = merge_to_silver(passed, entity_name, spark, catalog)
    if delivery_pattern == "full_snapshot":
        apply_snapshot_soft_deletes(spark, entity_name, survivors, catalog)

    rows_quarantined = write_quarantine(
        spark, failed, entity_name, run_id, run_at, catalog
    )
    append_dq_metrics(spark, metrics, catalog)
    return rows_read, rows_written, rows_quarantined


def run_entity_conform(
    spark: SparkSession,
    entity_name: str,
    catalog: str = DEFAULT_CATALOG,
    parent_run_id: str | None = None,
    stream_runner: Callable[..., None] | None = None,
    checkpoint_suffix: str | None = None,
) -> str:
    run_id = new_run_id()
    started_at = datetime.now(UTC)
    target = silver_table(entity_name, catalog)
    version_before = current_delta_version(spark, target)
    delivery_pattern = get_delivery_pattern(spark, entity_name, catalog)
    source_path = silver_checkpoint_path(
        entity_name, catalog, suffix=checkpoint_suffix
    )

    totals = {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}

    def on_batch(batch_df: DataFrame, _batch_id: int) -> None:
        rows_read, rows_written, rows_quarantined = process_conform_batch(
            spark, entity_name, batch_df, run_id, catalog, parent_run_id
        )
        # Worker-side totals are unreliable on Spark Connect; manifest uses sink_metrics.
        totals["rows_read"] += rows_read
        totals["rows_written"] += rows_written
        totals["rows_quarantined"] += rows_quarantined

    try:
        if stream_runner is not None:
            stream_runner(on_batch)
        else:
            run_cdf_stream(
                spark,
                entity_name,
                on_batch,
                catalog,
                checkpoint=source_path,
            )

        version_after = current_delta_version(spark, target)
        sink = resolve_silver_metrics(
            spark,
            entity_name,
            run_id,
            catalog,
            version_before,
            version_after,
        )
        append_silver_manifest(
            spark,
            PipelineManifestRecord(
                run_id=run_id,
                entity_name=entity_name,
                parent_run_id=parent_run_id,
                delivery_pattern=delivery_pattern,
                source_path=source_path,
                files_processed=0,
                rows_read=sink.rows_read,
                rows_written=sink.rows_written,
                rows_quarantined=sink.rows_quarantined,
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
        version_after = current_delta_version(spark, target)
        sink = resolve_silver_metrics(
            spark,
            entity_name,
            run_id,
            catalog,
            version_before,
            version_after,
        )
        append_silver_manifest(
            spark,
            PipelineManifestRecord(
                run_id=run_id,
                entity_name=entity_name,
                parent_run_id=parent_run_id,
                delivery_pattern=delivery_pattern,
                source_path=source_path,
                files_processed=0,
                rows_read=sink.rows_read,
                rows_written=sink.rows_written,
                rows_quarantined=sink.rows_quarantined,
                rows_rescued=0,
                delta_version_before=version_before,
                delta_version_after=version_after or version_before,
                started_at=started_at,
                completed_at=datetime.now(UTC),
                status="failed",
                error_message=str(exc),
            ),
            catalog,
        )
        raise

    return run_id


def run_conform_with_healing(
    spark: SparkSession,
    entity_name: str,
    catalog: str = DEFAULT_CATALOG,
) -> str:
    """Conform one entity, then clear orphan flags its arrival resolves.

    Replaces the parent-refresh model, in which the orders job drained the
    parent CDF through a second checkpoint before checking foreign keys. That
    consumed each parent twice, raced with the parent's own job over the same
    silver table, and still could not help an order whose customer arrived
    afterwards — the order was already quarantined, and nothing revisited it.

    Now each entity conforms on its own trigger only. Referential failures are
    not rejected; they land in silver flagged `_is_orphan`. When a parent
    conforms, the keys it just wrote are handed to heal_orphans, which clears
    the flag on the children waiting for them. Healing reads parent changes and
    writes only to the child table, so it cannot re-trigger itself.
    """
    run_id = run_entity_conform(spark, entity_name, catalog)
    if entity_name not in _PARENT_ENTITIES:
        return run_id
    try:
        healed = heal_orphans(spark, catalog)
        LOG.info("healed_orphans after=%s rows=%s", entity_name, healed)
    except Exception:
        # Healing is a repair pass. A failure must not fail the conform that
        # already succeeded; the next parent delivery retries it.
        LOG.exception("heal_orphans_failed after=%s", entity_name)
    return run_id


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
