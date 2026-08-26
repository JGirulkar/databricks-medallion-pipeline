"""Bronze CDF batch conform — snapshot/incremental merge semantics."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.config import (
    DEFAULT_CATALOG,
    ENTITY_EVENT_TIME,
    ENTITY_PK,
    SNAPSHOT_ENTITIES,
    load_dq_schema,
    silver_table,
)
from silver.job_log import configure_job_logger
from silver.schemas import business_columns

LOG = configure_job_logger("silver.conform")


def add_row_hash(df: DataFrame, entity: str) -> DataFrame:
    """Hash every business column so the merge can skip unchanged rows.

    Columns come from the entity schema, not a hand-listed subset: a list can
    silently omit a column, and edits to it would then never be detected.
    """
    canonical = [
        F.coalesce(F.col(name).cast("string"), F.lit(""))
        for name in business_columns(entity)
        if name in df.columns
    ]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *canonical), 256))


def _survivorship_order(df: DataFrame, entity: str) -> list[Column]:
    """Ordering that decides which row wins for a key.

    Later delivery first, then the later business event date, then ingest time,
    then the content hash. The hash is last and exists only so the result is
    reproducible: without it, rows sharing a batch and a timestamp tie and Spark
    picks arbitrarily, so the same input could put a key in silver on one run
    and in quarantine on the next.
    """
    order = [F.col("_batch_id").desc_nulls_last()]
    event_column = ENTITY_EVENT_TIME.get(entity)
    if event_column and event_column in df.columns:
        order.append(F.col(event_column).desc_nulls_last())
    order.append(F.col("_ingest_timestamp").desc_nulls_last())
    if "_row_hash" in df.columns:
        order.append(F.col("_row_hash").desc_nulls_last())
    return order


def _rank_within_pk(df: DataFrame, pk: str, entity: str) -> DataFrame:
    """Rank rows within a primary key across the whole batch, latest first."""
    window = Window.partitionBy(pk).orderBy(*_survivorship_order(df, entity))
    return df.withColumn("_pk_rank", F.row_number().over(window))


def _rank_within_delivery(df: DataFrame, pk: str, entity: str) -> Column:
    """Rank rows within a primary key INSIDE one delivery.

    Rank > 1 means the key was repeated in a single bronze file — a duplicate,
    and a defect. Contrast _rank_within_pk, where rank > 1 across deliveries
    just means superseded.
    """
    partition = [pk, "_batch_id"] if "_batch_id" in df.columns else [pk]
    # Same ordering as the cross-delivery rank, so the two cannot disagree about
    # which row wins within a single delivery.
    window = Window.partitionBy(*partition).orderBy(*_survivorship_order(df, entity))
    return F.row_number().over(window)


def split_validated_batch(
    tagged_df: DataFrame,
    entity: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split an already-validated batch into (survivors, passed, failed).

    Survivorship runs AFTER validation. Deduping first — which is what the
    previous conform_*_batch functions did — collapsed every duplicate before
    the uniqueness check ran, so `count(*) over (partition by pk) > 1` was
    never true and duplicates vanished with no audit trail.

    A uniqueness flag alone does not block the surviving row: once the losers
    are removed it genuinely is unique. Any other violation does block it.

    survivors: latest row per key, valid or not. Snapshot soft-deletes need the
               full set of keys present in the batch, not just the clean ones.
    passed:    survivors carrying no violation other than uniqueness.
    failed:    quarantined rows — survivorship losers, plus any row carrying a
               blocking violation.
    """
    pk = ENTITY_PK[entity]
    # Hash first: survivorship uses it as the final, deterministic tie-break.
    tagged_df = add_row_hash(tagged_df, entity)
    ranked = (
        _rank_within_pk(tagged_df, pk, entity)
        .withColumn("_delivery_rank", _rank_within_delivery(tagged_df, pk, entity))
        .withColumn(
            # Permanent defects. Uniqueness is excluded because survivorship
            # resolves it; referential is excluded because a later delivery of
            # the parent can resolve it.
            "_blocking",
            F.filter(
                F.col("_violations"),
                lambda v: ~v["category"].isin("uniqueness", "referential"),
            ),
        )
        .withColumn(
            "_orphan",
            F.exists(
                F.col("_violations"),
                lambda v: v["category"] == F.lit("referential"),
            ),
        )
    )
    survivors = ranked.filter(F.col("_pk_rank") == 1)
    passed = survivors.filter(
        (F.size(F.col("_blocking")) == 0) & (F.col("_delivery_rank") == 1)
    ).withColumn("_is_orphan", F.col("_orphan"))
    # Quarantine holds defects only: blocking violations, and the losing rows of
    # a duplicate INSIDE one delivery. A row superseded by a later delivery is
    # normal CDC — it is neither merged nor quarantined, and bronze retains it.
    failed = ranked.filter(
        (F.size(F.col("_blocking")) > 0) | (F.col("_delivery_rank") > 1)
    )

    scratch = ("_pk_rank", "_delivery_rank", "_blocking", "_orphan")
    survivors, passed, failed = (
        survivors.drop(*scratch),
        passed.drop(*scratch),
        failed.drop(*scratch),
    )
    survivors = add_row_hash(survivors, entity)
    passed = add_row_hash(passed, entity)
    return survivors, passed, failed


def prepare_silver_rows(
    df: DataFrame,
    entity: str,
    updated_at: datetime,
) -> DataFrame:
    del entity
    result = (
        df.withColumn("quality_check_result", F.lit("PASS"))
        .withColumn("_is_deleted", F.lit(False))
        .withColumn(
            "_is_orphan",
            F.coalesce(F.col("_is_orphan"), F.lit(False))
            if "_is_orphan" in df.columns
            else F.lit(False),
        )
        .withColumn("_silver_updated_at", F.lit(updated_at))
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
    )
    if "_row_hash" not in result.columns:
        result = result.withColumn("_row_hash", F.lit(None).cast("string"))
    bronze_meta = {
        "_ingest_timestamp",
        "_source_file",
        "_batch_id",
        "_delivery_pattern",
        "_rescued_data",
        "_violations",
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    }
    drop_cols = [
        c for c in result.columns if c in bronze_meta or c.startswith("_change")
    ]
    return result.drop(*drop_cols)


def merge_to_silver(
    df: DataFrame,
    entity: str,
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
) -> int:
    if not df.take(1):
        return 0
    target = silver_table(entity, catalog)
    pk = ENTITY_PK[entity]
    from delta.tables import DeltaTable

    merge_df = prepare_silver_rows(df, entity, datetime.now(UTC))
    delta_table = DeltaTable.forName(spark, target)
    business_cols = [
        c
        for c in merge_df.columns
        if c not in {
            pk,
            "quality_check_result",
            "_row_hash",
            "_is_deleted",
            "_silver_updated_at",
            "_bronze_batch_id",
        }
    ]
    update_map = {c: f"source.{c}" for c in business_cols}
    update_map.update(
        {
            "quality_check_result": "source.quality_check_result",
            "_is_deleted": "source._is_deleted",
            "_is_orphan": "source._is_orphan",
            "_silver_updated_at": "source._silver_updated_at",
            "_bronze_batch_id": "source._bronze_batch_id",
        }
    )
    if "_row_hash" in merge_df.columns:
        update_map["_row_hash"] = "source._row_hash"
    (
        delta_table.alias("target")
        .merge(merge_df.alias("source"), f"target.{pk} = source.{pk}")
        .whenMatchedUpdate(
            # Skip rows whose business values are unchanged — a snapshot feed
            # restates the whole world every delivery, so most rows are
            # identical and rewriting them costs a file rewrite for nothing.
            #
            # Two states must be written even when the values match, or the
            # skip would strand the row: a key that was soft-deleted and has
            # returned, and a row flagged orphan whose parent has arrived.
            condition=(
                "target._row_hash IS NULL "
                "OR target._row_hash <> source._row_hash "
                "OR target._is_deleted = true "
                # Null-safe comparison in BOTH directions. An earlier version
                # only fired when clearing the flag, so a row already in silver
                # whose values had not changed never got flagged in the first
                # place: the hash matched, the merge skipped it, and the orphan
                # stayed invisible. NULL on the target side matters too, for
                # rows written before the column existed.
                "OR NOT (target._is_orphan <=> source._is_orphan)"
            ),
            set=update_map,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return merge_df.count()


def apply_snapshot_soft_deletes(
    spark: SparkSession,
    entity: str,
    snapshot_df: DataFrame,
    catalog: str = DEFAULT_CATALOG,
    updated_at: datetime | None = None,
) -> int:
    if entity not in SNAPSHOT_ENTITIES:
        return 0
    pk = ENTITY_PK[entity]
    active_pks = snapshot_df.select(pk).distinct()
    target = silver_table(entity, catalog)
    silver_active = spark.table(target).where(~F.col("_is_deleted")).select(pk)
    missing = silver_active.join(active_pks, pk, "left_anti")
    # Materialise the count BEFORE the merge. `missing` is a lazy plan over the
    # target table filtered to _is_deleted = false, so evaluating it afterwards
    # re-reads the table and finds nothing — the rows it would have counted
    # have just been flagged. Counting after the write always returned 0.
    missing_count = missing.count()
    if missing_count == 0:
        return 0
    from delta.tables import DeltaTable

    ts = (updated_at or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M:%S")
    delta_table = DeltaTable.forName(spark, target)
    (
        delta_table.alias("target")
        .merge(missing.alias("missing"), f"target.{pk} = missing.{pk}")
        .whenMatchedUpdate(
            condition="target._is_deleted = false",
            set={
                "_is_deleted": "true",
                "_silver_updated_at": f"CAST('{ts}' AS TIMESTAMP)",
            },
        )
        .execute()
    )
    return missing_count


def _is_concurrency_conflict(exc: BaseException) -> bool:
    """Delta raises several distinct concurrency errors; match on the family."""
    name = type(exc).__name__
    return "Concurrent" in name or "concurrent" in str(exc).lower()


def refresh_orphan_flags(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    entity: str = "orders",
    attempts: int = 3,
) -> int:
    """Recompute `_is_orphan` from the data, setting AND clearing it.

    Symmetric on purpose. An earlier version only cleared the flag, on the
    theory that the interesting event is a parent arriving late. But a parent
    can also LEAVE: soft-deleting 3 products left 624 orders pointing at nothing
    while still marked valid, because nothing re-evaluated rows already in
    silver. Deciding the flag from the data covers both directions and needs no
    knowledge of which parent changed.

    An earlier version also cleared the flag whenever ONE parent arrived, which
    wrongly cleared 38 orders whose customer was still missing. A row is an
    orphan if ANY of its foreign keys is unresolved, so every key is checked.

    The foreign keys come from the same dq_schema the validation check reads, so
    the two cannot drift apart. Returns the number of rows whose flag changed.
    """
    # Each parent's job runs this after conforming, so with parallel triggers
    # several of them write to the child table at once and Delta rejects the
    # loser. Recompute on retry rather than replaying the old plan: another
    # writer may already have corrected some of the rows.
    for attempt in range(1, attempts + 1):
        try:
            return _refresh_orphan_flags_once(spark, catalog, entity)
        except Exception as exc:
            if not _is_concurrency_conflict(exc) or attempt == attempts:
                raise
            LOG.info(
                "refresh_orphan_flags_conflict attempt=%s/%s entity=%s",
                attempt, attempts, entity,
            )
            time.sleep(5 * attempt)
    return 0


def _refresh_orphan_flags_once(
    spark: SparkSession,
    catalog: str,
    entity: str,
) -> int:
    dq_schema = load_dq_schema(spark, entity, catalog)
    fk_checks = [check for check in dq_schema.checks if check.kind == "fk_exists"]
    if not fk_checks:
        return 0

    target = silver_table(entity, catalog)
    pk = ENTITY_PK[entity]
    rows = spark.table(target).select(
        pk, "_is_orphan", *[check.column for check in fk_checks]
    )

    unresolved = None
    for index, check in enumerate(fk_checks):
        parent_alias = f"_parent_{index}"
        parents = (
            spark.table(check.ref_table)
            .where(~F.col("_is_deleted"))
            .select(F.col(check.ref_column or check.column).alias(parent_alias))
            .distinct()
        )
        rows = rows.join(
            parents, F.col(check.column) == F.col(parent_alias), "left"
        )
        # A NULL foreign key is a completeness failure, already quarantined, so
        # it is not treated as an orphan here.
        missing = F.col(check.column).isNotNull() & F.col(parent_alias).isNull()
        unresolved = missing if unresolved is None else (unresolved | missing)

    desired = rows.withColumn("_should_orphan", unresolved).select(
        pk, "_is_orphan", "_should_orphan"
    )
    # Null-safe: a row written before the column existed has a NULL flag and
    # must be decided, not skipped.
    changes = desired.filter(
        ~F.col("_is_orphan").eqNullSafe(F.col("_should_orphan"))
    ).select(pk, "_should_orphan")

    # Count before the update; the predicate selects on the column it writes.
    changed = changes.count()
    if changed == 0:
        return 0

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, target)
        .alias("target")
        .merge(changes.alias("source"), f"target.{pk} = source.{pk}")
        .whenMatchedUpdate(set={"_is_orphan": "source._should_orphan"})
        .execute()
    )
    LOG.info("refresh_orphan_flags entity=%s changed=%s", entity, changed)
    return changed
