#!/usr/bin/env python3
"""Bronze + Silver CE E2E — individual bronze ingests, silver table-update triggers, SQL verify.

Skips bronze bootstrap when bronze entity tables already exist AND ops.pipeline_manifest exists.
Always runs silver bootstrap when silver schema / entity tables are missing.

Usage:
  medallion_e2e.py run [--deploy] [--catalog de_assessment] [--force-bronze-bootstrap]

Prints: === E2E JSON === ...
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from bronze_e2e import (
    DEFAULT_CATALOG,
    EXPECTED_BATCH_ROWS,
    deploy,
    ensure_env,
    extract_batch_id,
    job_ids,
    poll_run,
    profile,
    run_cmd,
    run_now,
    run_now_with_params,
    show_filtered_logs,
    sql_query,
    tail_logs,
    task_run_id,
    verify_batch,
    wait_orders_trigger,
)

SILVER_JOB_NAMES = {
    "silver_bootstrap": "de_assessment_silver_bootstrap",
    "silver_products": "de_assessment_silver_products",
    "silver_customers": "de_assessment_silver_customers",
    "silver_orders": "de_assessment_silver_orders",
}

SILVER_JOB_KEY_FOR_ENTITY = {
    "products": "silver_products",
    "customers": "silver_customers",
    "orders": "silver_orders",
}

BRONZE_ENTITIES = ("products", "customers", "orders")
SILVER_ENTITIES = ("products", "customers", "orders")
ENTITY_PK: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
}

SILVER_LOG_KEYWORDS = (
    "INFO",
    "ERROR",
    "conform",
    "quarantine",
    "rows_quarantined",
    "rows_written",
    "silver_run_id",
    "cdf",
    "merge",
    "dq_metrics",
)


@dataclass
class MedallionResult:
    status: str = "pending"
    batch_id: str = ""
    catalog: str = DEFAULT_CATALOG
    runs: dict[str, str] = field(default_factory=dict)
    manifest: dict[str, dict[str, Any]] = field(default_factory=dict)
    bronze_batch_rows: dict[str, int] = field(default_factory=dict)
    passed: bool = False
    errors: list[str] = field(default_factory=list)
    ce_state: dict[str, bool] = field(default_factory=dict)
    bootstrap_runs: dict[str, str] = field(default_factory=dict)
    silver_runs: list[str] = field(default_factory=list)
    silver_manifest: dict[str, dict[str, Any]] = field(default_factory=dict)
    silver_batch_rows: dict[str, int] = field(default_factory=dict)
    quarantine_batch_rows: dict[str, int] = field(default_factory=dict)
    started_at_ms: int = 0
    dq_metrics_rows: int = 0
    silver_pk_dupes: dict[str, int] = field(default_factory=dict)
    quarantine_by_category: dict[str, int] = field(default_factory=dict)
    silver_soft_deleted: dict[str, int] = field(default_factory=dict)
    cdc: dict[str, int] = field(default_factory=dict)
    unaccounted_keys: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "batch_id": self.batch_id,
            "catalog": self.catalog,
            "runs": self.runs,
            "manifest": self.manifest,
            "bronze_batch_rows": self.bronze_batch_rows,
            "passed": self.passed,
            "errors": self.errors,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "ce_state": self.ce_state,
            "bootstrap_runs": self.bootstrap_runs,
            "silver_runs": self.silver_runs,
            "silver_manifest": self.silver_manifest,
            "silver_batch_rows": self.silver_batch_rows,
            "quarantine_batch_rows": self.quarantine_batch_rows,
            "dq_metrics_rows": self.dq_metrics_rows,
            "silver_pk_dupes": self.silver_pk_dupes,
            "quarantine_by_category": self.quarantine_by_category,
            "silver_soft_deleted": self.silver_soft_deleted,
            "cdc": self.cdc,
            "unaccounted_keys": self.unaccounted_keys,
        }


def table_exists(catalog: str, schema: str, table: str) -> bool:
    try:
        rows = sql_query(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'")
        return any(str(row[1]) == table for row in rows if len(row) > 1)
    except RuntimeError:
        return False


def assess_ce_state(catalog: str) -> dict[str, bool]:
    bronze_tables = all(
        table_exists(catalog, "bronze", entity) for entity in BRONZE_ENTITIES
    )
    ops_manifest = table_exists(catalog, "ops", "pipeline_manifest")
    silver_tables = all(
        table_exists(catalog, "silver", entity) for entity in SILVER_ENTITIES
    )
    silver_quarantine = table_exists(catalog, "silver", "quarantine")
    return {
        "bronze_tables": bronze_tables,
        "ops_pipeline_manifest": ops_manifest,
        "silver_tables": silver_tables,
        "silver_quarantine": silver_quarantine,
        "needs_bronze_bootstrap": not (bronze_tables and ops_manifest),
        "needs_silver_bootstrap": not (silver_tables and silver_quarantine),
    }


def wait_job_run_after(
    job_id: int,
    label: str,
    after_ms: int,
    *,
    timeout_sec: int = 900,
) -> str:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = run_cmd(
            [
                "databricks",
                "jobs",
                "list-runs",
                "--job-id",
                str(job_id),
                "--profile",
                profile(),
                "-o",
                "json",
                "--limit",
                "10",
            ]
        )
        runs = json.loads(proc.stdout)
        runs = runs if isinstance(runs, list) else runs.get("runs", [])
        candidate = ""
        for run in runs:
            if run.get("start_time", 0) < after_ms - 60_000:
                continue
            candidate = str(run["run_id"])
            repair = run.get("repair_count", 0)
            state = run.get("state", {})
            life = state.get("life_cycle_state", "")
            result = state.get("result_state", "")
            if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                if repair > 0 and result == "FAILED":
                    logs = tail_logs(task_run_id(candidate))
                    raise RuntimeError(
                        f"{label} failed on retry repair_count={repair} run={candidate}\n{logs}"
                    )
                if result != "SUCCESS":
                    logs = tail_logs(task_run_id(candidate))
                    raise RuntimeError(
                        f"{label} failed run={candidate} repair={repair}: {state}\n{logs}"
                    )
                return candidate
            print(
                f"{datetime.now(UTC).strftime('%H:%M:%S')} {label} "
                f"run={candidate} {life} {result}"
            )
            break
        if not candidate:
            print(
                f"{datetime.now(UTC).strftime('%H:%M:%S')} waiting for {label} trigger..."
            )
        time.sleep(10)
    raise TimeoutError(f"{label} did not complete within {timeout_sec}s after {after_ms}")


def show_silver_logs(run_id: str, label: str) -> None:
    print(f"===== {label} run={run_id} =====")
    logs = tail_logs(task_run_id(run_id), limit=8000)
    for line in logs.split("\n"):
        if any(k in line for k in SILVER_LOG_KEYWORDS):
            print(line[:260])


def verify_silver(
    catalog: str,
    landing_batch_id: str,
    result: MedallionResult,
    run_ids: list[str] | None = None,
) -> None:
    """Verify silver rows for a landing wave.

    ``landing_batch_id`` is the data-gen stamp (e.g. 20260825T090022Z) embedded in
    landing file paths. Bronze ``_batch_id`` is a per-ingest UUID; silver
    ``_bronze_batch_id`` copies that UUID — not the landing stamp.
    """
    manifest_rows = sql_query(
        f"""
        SELECT entity_name, status, rows_written, rows_quarantined, started_at
        FROM {catalog}.ops.pipeline_manifest
        WHERE layer = 'silver'
        ORDER BY started_at DESC
        LIMIT 12
        """
    )
    latest_by_entity: dict[str, dict[str, Any]] = {}
    for row in manifest_rows:
        entity = str(row[0])
        if entity not in latest_by_entity:
            latest_by_entity[entity] = {
                "status": row[1],
                "rows_written": int(row[2]),
                "rows_quarantined": int(row[3]),
                "started_at": row[4],
            }
    result.silver_manifest = latest_by_entity

    for entity in SILVER_ENTITIES:
        bronze_batch_rows = sql_query(
            f"""
            SELECT DISTINCT _batch_id
            FROM {catalog}.bronze.{entity}
            WHERE _source_file LIKE '%{landing_batch_id}%'
            LIMIT 1
            """
        )
        if not bronze_batch_rows:
            result.errors.append(
                f"silver.{entity}: no bronze _batch_id for landing wave {landing_batch_id}"
            )
            continue
        bronze_ingest_batch_id = str(bronze_batch_rows[0][0])

        rows = sql_query(
            f"""
            SELECT COUNT(*)
            FROM {catalog}.silver.{entity}
            WHERE _bronze_batch_id = '{bronze_ingest_batch_id}'
            """
        )
        count = int(rows[0][0]) if rows else 0
        result.silver_batch_rows[entity] = count
        # count is how many rows this batch CHANGED, so zero is legitimate on a
        # re-delivery of identical data. Only the upper bound is meaningful; the
        # key-level accounting below is what proves nothing was lost.
        _lo, hi = EXPECTED_BATCH_ROWS[entity]
        if count > hi:
            result.errors.append(
                f"silver.{entity}: valid rows={count} unexpectedly above bronze max {hi}"
            )

        qrows = sql_query(
            f"""
            SELECT COUNT(*)
            FROM {catalog}.silver.quarantine
            WHERE entity_name = '{entity}'
              AND bronze_batch_id = '{bronze_ingest_batch_id}'
            """
        )
        qcount = int(qrows[0][0]) if qrows else 0
        result.quarantine_batch_rows[entity] = qcount

        # Every bronze row must end up either valid in silver or quarantined.
        # The only legitimate shortfall is the duplicate primary keys conform
        # collapses, so allow 1%. Without this, silver could drop nearly the
        # whole batch and still pass on the count > 0 check alone.
        # Account for the batch by KEY, not by counting rows stamped with this
        # batch id. Since the merge skips rows whose values are unchanged,
        # _bronze_batch_id now records the batch that last CHANGED a row, not
        # the batch that last delivered it — so a re-delivery of identical data
        # legitimately stamps almost nothing. The invariant that still holds is
        # that every key delivered is either present in silver or quarantined.
        pk_col = ENTITY_PK[entity]
        unaccounted = sql_query(
            f"""
            SELECT COUNT(*) FROM (
              SELECT DISTINCT {pk_col} AS k
              FROM {catalog}.bronze.{entity}
              WHERE _batch_id = '{bronze_ingest_batch_id}' AND {pk_col} IS NOT NULL
            ) d
            WHERE NOT EXISTS (
                SELECT 1 FROM {catalog}.silver.{entity} s WHERE s.{pk_col} = d.k
              )
              AND NOT EXISTS (
                SELECT 1 FROM {catalog}.silver.quarantine q
                WHERE q.entity_name = '{entity}'
                  AND q.primary_key = CAST(d.k AS STRING)
              )
            """
        )
        missing_keys = int(unaccounted[0][0]) if unaccounted else 0
        result.unaccounted_keys[entity] = missing_keys
        if missing_keys:
            result.errors.append(
                f"silver.{entity}: {missing_keys} keys delivered in bronze batch "
                f"{bronze_ingest_batch_id} are neither in silver nor quarantined"
            )

    total_quarantine = sum(result.quarantine_batch_rows.values())
    if total_quarantine < 50:
        result.errors.append(
            f"quarantine: total batch rows={total_quarantine} expected intentional DQ issues"
        )

    metrics = sql_query(
        f"""
        SELECT COUNT(*)
        FROM {catalog}.silver.dq_metrics
        WHERE run_at >= current_timestamp() - INTERVAL 6 HOURS
        """
    )
    result.dq_metrics_rows = int(metrics[0][0]) if metrics else 0
    if result.dq_metrics_rows <= 0:
        result.errors.append("dq_metrics: no rows for recent silver run")

    for entity in SILVER_ENTITIES:
        m = latest_by_entity.get(entity)
        if not m:
            result.errors.append(f"silver manifest: no row for entity={entity}")
            continue
        if m["status"] != "success":
            result.errors.append(f"silver manifest: {entity} status={m['status']}")
        # rows_written == 0 is legitimate: the merge skips rows whose values are
        # unchanged, so a re-delivery of identical data correctly writes nothing.
        # Loss is caught by the key-level accounting below, which does not depend
        # on anything having been rewritten.

    # PK uniqueness in valid silver tables
    for entity, pk in ENTITY_PK.items():
        dup_rows = sql_query(
            f"""
            SELECT COUNT(*) FROM (
              SELECT {pk}
              FROM {catalog}.silver.{entity}
              GROUP BY {pk}
              HAVING COUNT(*) > 1
            )
            """
        )
        dup_count = int(dup_rows[0][0]) if dup_rows else 0
        result.silver_pk_dupes[entity] = dup_count
        if dup_count > 0:
            result.errors.append(f"silver.{entity}: {dup_count} duplicate PK groups")

    # Soft-deleted dimension rows (snapshot entities only)
    for entity in ("products", "customers"):
        sd_rows = sql_query(
            f"""
            SELECT COUNT(*) FROM {catalog}.silver.{entity}
            WHERE _is_deleted = true
            """
        )
        result.silver_soft_deleted[entity] = int(sd_rows[0][0]) if sd_rows else 0

    # Quarantine breakdown by DQ category, scoped to the runs THIS E2E produced.
    # Scoping by bronze batch instead would sum every silver run that ever
    # processed that batch: one batch here had been quarantined by two runs and
    # reported 20,020 rows for a 10,010-row delivery.
    if run_ids:
        cat_rows = sql_query(
            f"""
            SELECT violation.category, COUNT(*) AS cnt
            FROM {catalog}.silver.quarantine q
            LATERAL VIEW explode(q.violations) exploded AS violation
            WHERE q.silver_run_id IN ({quoted(run_ids)})
            GROUP BY violation.category
            ORDER BY violation.category
            """
        )
        for row in cat_rows:
            result.quarantine_by_category[str(row[0])] = int(row[1])
        if not result.quarantine_by_category and total_quarantine > 0:
            result.errors.append(
                "quarantine: rows present but category breakdown empty"
            )


def silver_run_ids_since(catalog: str, since_ms: int) -> list[str]:
    """The silver run ids this E2E produced, from ops.pipeline_manifest.

    Quarantine carries two lineage columns and they answer different questions.
    `bronze_batch_id` says where a row CAME FROM; `silver_run_id` says which run
    REJECTED it. Scoping "what did this run quarantine" by bronze batch sums
    every silver run that ever processed that batch — one batch here had been
    quarantined by two runs, reporting 20,020 rows for a 10,010-row delivery.
    """
    rows = sql_query(
        f"""
        SELECT DISTINCT run_id
        FROM {catalog}.ops.pipeline_manifest
        WHERE layer = 'silver'
          AND started_at >= TIMESTAMP_MILLIS({since_ms})
        """
    )
    return [str(r[0]) for r in rows if r and r[0]]


def quoted(values: list[str]) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values) or "''"


def cmd_run(args: argparse.Namespace) -> int:
    """Wrapper that guarantees a report. See _cmd_run for the flow."""
    result = MedallionResult(catalog=args.catalog)
    try:
        return _cmd_run(args, result)
    except Exception as exc:  # noqa: BLE001 - the report matters more than the trace
        import traceback

        traceback.print_exc()
        result.errors.append(f"aborted: {type(exc).__name__}: {exc}")
        result.passed = False
        result.status = "aborted"
        emit_result(result)
        return 1


def _cmd_run(args: argparse.Namespace, result: MedallionResult) -> int:
    ensure_env()
    # `result` is owned by cmd_run so that anything recorded before an
    # unexpected failure still reaches the report.
    result.started_at_ms = int(time.time() * 1000)
    result.ce_state = assess_ce_state(args.catalog)
    print("=== CE state ===", json.dumps(result.ce_state))

    if args.deploy:
        deploy(args.catalog)

    all_names = {
        **{k: v for k, v in {
            "data_gen": "de_assessment_data_generation",
            "bootstrap": "de_assessment_bronze_bootstrap",
            "products": "de_assessment_bronze_products",
            "customers": "de_assessment_bronze_customers",
            "orders": "de_assessment_bronze_orders",
        }.items()},
        **SILVER_JOB_NAMES,
    }
    ids = job_ids(all_names)
    missing = [k for k in all_names if k not in ids]
    if missing:
        result.status = "failed"
        result.errors.append(f"missing job ids for: {missing}")
        emit_result(result)
        return 1

    print("job_ids", " ".join(f"{k}={ids[k]}" for k in sorted(ids)))

    run_bronze_bootstrap = args.force_bronze_bootstrap or result.ce_state["needs_bronze_bootstrap"]
    if run_bronze_bootstrap:
        print("=== bronze bootstrap (ops.pipeline_manifest or bronze DDL) ===")
        boot_run = run_now(ids["bootstrap"])
        result.bootstrap_runs["bronze"] = boot_run
        poll_run(boot_run, "bronze_bootstrap")
        show_filtered_logs(boot_run, "bronze_bootstrap")
    else:
        print("=== SKIP bronze bootstrap (bronze tables + ops.pipeline_manifest present) ===")

    # Always run silver bootstrap. It is idempotent (DDL IF NOT EXISTS, and
    # seed_dq_schema is a MERGE), and gating it on table existence meant a
    # CHANGED dq_schema seed was silently skipped: the tables were already
    # there, so the new validation rules never reached config.source_config
    # and every rule added since the last bootstrap reported a perfect pass.
    print("=== silver bootstrap (always — reseeds dq_schema) ===")
    sb_run = run_now(ids["silver_bootstrap"])
    result.bootstrap_runs["silver"] = sb_run
    poll_run(sb_run, "silver_bootstrap")
    show_silver_logs(sb_run, "silver_bootstrap")

    print("=== data generation ===")
    gen_start_ms = int(time.time() * 1000)
    gen_run = run_now(ids["data_gen"])
    result.runs["data_gen"] = gen_run
    poll_run(gen_run, "data_gen")
    show_filtered_logs(gen_run, "data_gen")
    batch_id = extract_batch_id(gen_run)
    if not batch_id:
        result.status = "failed"
        result.errors.append("could not parse batch_id from data gen logs")
        emit_result(result)
        return 1
    result.batch_id = batch_id
    print(f"batch_id={batch_id}")

    # Individual bronze ingests — products, customers manual; orders file-arrival
    ingest_sequence: list[tuple[str, str, str]] = [
        ("products", "products", "manual"),
        ("customers", "customers", "manual"),
        ("orders", "orders", "file_arrival"),
    ]

    ingest_all(ids, result, gen_start_ms, ingest_sequence)

    print("=== verify bronze ===")
    verify_batch(args.catalog, batch_id, result)

    print("=== verify silver ===")
    verify_silver(
        args.catalog,
        batch_id,
        result,
        run_ids=silver_run_ids_since(args.catalog, result.started_at_ms),
    )

    # ---- second delivery: change data capture -------------------------------
    # The seed batch alone cannot prove updates, deletes or orphan healing: it
    # is the first delivery, so everything in it is an insert.
    print("=== data generation (delta delivery) ===")
    delta_start_ms = int(time.time() * 1000)
    delta_gen_run = run_now_with_params(
        ids["data_gen"], ["--catalog", args.catalog, "--mode", "delta"]
    )
    result.runs["data_gen_delta"] = delta_gen_run
    poll_run(delta_gen_run, "data_gen_delta")
    show_filtered_logs(delta_gen_run, "data_gen_delta")

    ingest_all(ids, result, delta_start_ms, ingest_sequence, phase="delta")

    print("=== verify change data capture ===")
    verify_cdc(args.catalog, result)

    result.passed = not result.errors
    result.status = "success" if result.passed else "failed"
    emit_result(result)
    return 0 if result.passed else 1


def ingest_all(
    ids: dict[str, int],
    result: MedallionResult,
    gen_start_ms: int,
    ingest_sequence: list[tuple[str, str, str]],
    phase: str = "",
) -> None:
    """Ingest every entity concurrently, then wait for the silver jobs.

    Was sequential — products, wait for its silver job, then customers, then
    orders — because orders' foreign-key check needed its parents already
    conformed. That dependency is gone: a referential failure now lands in
    silver flagged and a later parent arrival clears it, so the three entities
    are independent and the whole wave runs in parallel.

    The bronze jobs write to separate tables. The silver jobs write to separate
    entity tables and append to shared quarantine, metrics and manifest tables,
    which Delta handles. The one real contention is refresh_orphan_flags: every
    parent's job writes to silver.orders, so it retries on a Delta concurrency
    conflict.
    """
    suffix = f"_{phase}" if phase else ""
    launched: dict[str, tuple[str, int]] = {}

    # Launch every bronze ingest before waiting on any of them.
    for job_key, label, mode in ingest_sequence:
        if mode == "file_arrival":
            print(f"=== wait orders{suffix} file-arrival (up to 5 min) ===")
            auto_run = wait_orders_trigger(ids["orders"], gen_start_ms)
            if not auto_run:
                result.errors.append(
                    f"orders{suffix} file-arrival did not trigger within 5 min"
                )
                continue
            result.runs[f"orders{suffix}"] = auto_run
            launched[label] = (auto_run, gen_start_ms)
        else:
            print(f"=== launch bronze {label}{suffix} ===")
            started = int(time.time() * 1000)
            run_id = run_now(ids[job_key])
            result.runs[f"{label}{suffix}"] = run_id
            launched[label] = (run_id, started)

    # Then collect them.
    for label, (ingest_run, started) in launched.items():
        poll_run(ingest_run, f"bronze_{label}{suffix}")
        show_filtered_logs(ingest_run, f"bronze_{label}{suffix}")

    for label, (_ingest_run, started) in launched.items():
        print(f"=== wait silver {label}{suffix} ===")
        try:
            silver_run = wait_job_run_after(
                ids[SILVER_JOB_KEY_FOR_ENTITY[label]],
                f"silver_after_{label}{suffix}",
                started,
                timeout_sec=900,
            )
            result.silver_runs.append(silver_run)
            show_silver_logs(silver_run, f"silver_after_{label}{suffix}")
        except (RuntimeError, TimeoutError) as exc:
            result.errors.append(f"silver after {label}{suffix}: {exc}")


def merge_metrics(catalog: str, entity: str) -> dict[str, int]:
    """Insert/update counts from the most recent MERGE on a silver table."""
    rows = sql_query(
        f"""
        SELECT operationMetrics['numTargetRowsInserted'],
               operationMetrics['numTargetRowsUpdated']
        FROM (DESCRIBE HISTORY {catalog}.silver.{entity})
        WHERE operation = 'MERGE'
        ORDER BY version DESC
        LIMIT 1
        """
    )
    if not rows:
        return {"inserted": 0, "updated": 0}
    return {"inserted": int(rows[0][0] or 0), "updated": int(rows[0][1] or 0)}


def verify_cdc(catalog: str, result: MedallionResult) -> None:
    """Assert the delta delivery produced inserts, updates, deletes and healing.

    Each assertion names the rows it expects rather than counting differences,
    because the seed batch also corrupts some of the same columns the delta
    changes — a raw diff between batches is larger than the delta's own edits.
    """
    def scalar(sql: str) -> int:
        rows = sql_query(sql)
        return int(rows[0][0]) if rows else 0

    # INSERT — incremental orders arrive as genuinely new keys.
    new_orders = scalar(
        f"SELECT COUNT(*) FROM {catalog}.silver.orders WHERE order_id >= 200001"
    )
    result.cdc["new_orders"] = new_orders
    if new_orders != 500:
        result.errors.append(f"cdc: expected 500 new orders in silver, found {new_orders}")

    # UPDATE — and the hash gate means only changed rows are rewritten, not all 10k.
    cust = merge_metrics(catalog, "customers")
    result.cdc["customers_updated"] = cust["updated"]
    result.cdc["customers_inserted"] = cust["inserted"]
    if cust["updated"] == 0:
        result.errors.append("cdc: no customer rows updated by the delta delivery")
    if cust["updated"] > 1000:
        result.errors.append(
            f"cdc: {cust['updated']} customers rewritten — the row hash is not gating "
            "the merge; a snapshot re-delivery should touch only changed rows"
        )

    # DELETE — a snapshot feed expresses deletion by omission.
    deleted = scalar(
        f"SELECT COUNT(*) FROM {catalog}.silver.products WHERE _is_deleted"
    )
    result.cdc["products_soft_deleted"] = deleted
    if deleted < 3:
        result.errors.append(f"cdc: expected >=3 soft-deleted products, found {deleted}")

    # HEALING — parents that arrived clear their children's orphan flag; parents
    # still missing leave it set. Both halves matter: healing everything would
    # not distinguish healed from never-checked.
    # The flag must agree with the data in BOTH directions. An earlier version
    # only checked that some rows were still flagged, which passed while 38
    # orders whose customer did not exist had been wrongly cleared — healing had
    # reacted to one parent arriving instead of re-evaluating the row.
    missing_parent = f"""
        (NOT EXISTS (SELECT 1 FROM {catalog}.silver.customers c
                     WHERE c.customer_id = o.customer_id AND NOT c._is_deleted)
         OR NOT EXISTS (SELECT 1 FROM {catalog}.silver.products p
                        WHERE p.product_id = o.product_id AND NOT p._is_deleted))
    """
    wrongly_cleared = scalar(
        f"SELECT COUNT(*) FROM {catalog}.silver.orders o "
        f"WHERE NOT o._is_orphan AND {missing_parent}"
    )
    wrongly_flagged = scalar(
        f"SELECT COUNT(*) FROM {catalog}.silver.orders o "
        f"WHERE o._is_orphan AND NOT {missing_parent}"
    )
    still = scalar(f"SELECT COUNT(*) FROM {catalog}.silver.orders WHERE _is_orphan")
    unflagged = scalar(
        f"SELECT COUNT(*) FROM {catalog}.silver.orders WHERE _is_orphan IS NULL"
    )
    result.cdc["orphans_still_waiting"] = still
    result.cdc["orphan_flag_wrongly_cleared"] = wrongly_cleared
    result.cdc["orphan_flag_wrongly_set"] = wrongly_flagged
    result.cdc["orphan_flag_null"] = unflagged
    if wrongly_cleared:
        result.errors.append(
            f"cdc: {wrongly_cleared} orders are not flagged orphan although a parent "
            "is missing — healing cleared a flag another parent still earns"
        )
    if wrongly_flagged:
        result.errors.append(
            f"cdc: {wrongly_flagged} orders are flagged orphan although every parent "
            "exists — healing did not run or did not resolve them"
        )
    if unflagged:
        result.errors.append(
            f"cdc: {unflagged} orders have a NULL orphan flag — the merge skipped "
            "rows that needed the flag set"
        )
    if still == 0:
        result.errors.append(
            "cdc: no orders left flagged orphan — healing cannot be distinguished "
            "from the check never running"
        )


def emit_result(result: MedallionResult) -> None:
    print("=== E2E JSON ===")
    print(json.dumps(result.to_dict(), indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bronze + Silver CE E2E orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="Full medallion E2E")
    run_p.add_argument("--catalog", default=DEFAULT_CATALOG)
    run_p.add_argument("--deploy", action="store_true")
    run_p.add_argument(
        "--force-bronze-bootstrap",
        action="store_true",
        help="Run bronze bootstrap even when CE bronze tables exist",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "run":
        return cmd_run(args)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
