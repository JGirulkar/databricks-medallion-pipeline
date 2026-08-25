#!/usr/bin/env python3
"""Bronze layer CE E2E orchestrator — deploy, data gen, ingest, SQL verify, JSON report.

Usage:
  bronze_e2e.py run [--deploy] [--bootstrap] [--catalog de_assessment]
  bronze_e2e.py verify --batch-id YYYYMMDDTHHMMSSZ [--catalog de_assessment]

Prints a machine-readable block: === E2E JSON === ... for agents to parse.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CATALOG = "de_assessment"
DEFAULT_PROFILE = "de-assessment-ce"
DEFAULT_HOST = "https://dbc-06f970f4-0f19.cloud.databricks.com"
WAREHOUSE_ID = "3579c90d6618d56d"

JOB_NAMES = {
    "data_gen": "de_assessment_data_generation",
    "bootstrap": "de_assessment_bronze_bootstrap",
    "products": "de_assessment_bronze_products",
    "customers": "de_assessment_bronze_customers",
    "orders": "de_assessment_bronze_orders",
}

# Assessment base row counts per generated batch (header=true; Int64 CSV ids).
EXPECTED_BATCH_ROWS: dict[str, tuple[int, int]] = {
    "products": (500, 500),
    "customers": (10_010, 10_010),
    "orders": (100_020, 100_020),
}

LOG_KEYWORDS = (
    "INFO",
    "ERROR",
    "batch_id",
    "rows_written",
    "metrics_from_sink",
    "run_ingest_success",
    "append_batch",
    "volume_write",
    "generate_complete",
    "archive_file",
)


@dataclass
class E2EResult:
    status: str = "pending"
    batch_id: str = ""
    catalog: str = DEFAULT_CATALOG
    runs: dict[str, str] = field(default_factory=dict)
    manifest: dict[str, dict[str, Any]] = field(default_factory=dict)
    bronze_batch_rows: dict[str, int] = field(default_factory=dict)
    passed: bool = False
    errors: list[str] = field(default_factory=list)

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
        }


def profile() -> str:
    return os.environ.get("DATABRICKS_CONFIG_PROFILE", DEFAULT_PROFILE)


def ensure_env() -> None:
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", DEFAULT_PROFILE)
    os.environ.setdefault("DATABRICKS_HOST", DEFAULT_HOST)


def run_cmd(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def job_ids(names: dict[str, str] | None = None) -> dict[str, int]:
    names = names or JOB_NAMES
    proc = run_cmd(["databricks", "jobs", "list", "--profile", profile(), "-o", "json"])
    data = json.loads(proc.stdout)
    jobs = data if isinstance(data, list) else data.get("jobs", [])
    by_name = {
        j.get("settings", {}).get("name", ""): int(j["job_id"])
        for j in jobs
        if j.get("settings", {}).get("name")
    }
    return {key: by_name[name] for key, name in names.items() if name in by_name}


def run_now(job_id: int) -> str:
    proc = run_cmd(
        ["databricks", "jobs", "run-now", str(job_id), "--profile", profile(), "-o", "json"]
    )
    run_id = json.loads(proc.stdout).get("run_id", "")
    if not run_id:
        raise RuntimeError(f"run-now returned no run_id for job_id={job_id}")
    return str(run_id)


def task_run_id(run_id: str) -> str:
    proc = run_cmd(
        ["databricks", "jobs", "get-run", run_id, "--profile", profile(), "-o", "json"]
    )
    return str(json.loads(proc.stdout)["tasks"][0]["run_id"])


def poll_run(run_id: str, label: str, *, timeout_sec: int = 900) -> None:
    for i in range(timeout_sec // 10):
        proc = run_cmd(
            ["databricks", "jobs", "get-run", run_id, "--profile", profile(), "-o", "json"]
        )
        state = json.loads(proc.stdout)["state"]
        life = state["life_cycle_state"]
        result = state.get("result_state", "")
        if i % 3 == 0:
            ts = datetime.now(UTC).strftime("%H:%M:%S")
            print(f"{ts} {label} run={run_id} {life} {result}")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result != "SUCCESS":
                logs = tail_logs(task_run_id(run_id))
                raise RuntimeError(f"{label} failed run={run_id}: {state}\n{logs}")
            return
        time.sleep(10)
    raise TimeoutError(f"{label} timed out run={run_id}")


def tail_logs(task_run_id_val: str, *, limit: int = 3000) -> str:
    proc = run_cmd(
        [
            "databricks",
            "jobs",
            "get-run-output",
            task_run_id_val,
            "--profile",
            profile(),
            "-o",
            "json",
        ],
        check=False,
    )
    if proc.returncode != 0:
        return proc.stderr or proc.stdout
    logs = str(json.loads(proc.stdout).get("logs", ""))
    return logs[-limit:]


def show_filtered_logs(run_id: str, label: str) -> None:
    print(f"===== {label} run={run_id} =====")
    logs = tail_logs(task_run_id(run_id), limit=8000)
    for line in logs.split("\n"):
        if any(k in line for k in LOG_KEYWORDS):
            print(line[:260])


def extract_batch_id(run_id: str) -> str:
    logs = tail_logs(task_run_id(run_id), limit=12000)
    for pat in (
        r"batch_id=([0-9TZ]+)",
        r"'batch_id': '([^']+)'",
        r"products_([0-9TZ]+)\.csv",
    ):
        match = re.search(pat, logs)
        if match:
            return match.group(1)
    return ""


def sql_query(statement: str) -> list[list[Any]]:
    payload = json.dumps(
        {"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "50s"}
    )
    proc = run_cmd(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--profile",
            profile(),
            "--json",
            payload,
            "-o",
            "json",
        ]
    )
    result = json.loads(proc.stdout)
    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed state={state} error={err} stmt={statement[:120]}")
    return result.get("result", {}).get("data_array", [])


def verify_batch(catalog: str, batch_id: str, result: E2EResult) -> None:
    manifest_rows = sql_query(
        f"""
        SELECT entity_name, status, files_processed, rows_written, rows_read,
               delta_version_before, delta_version_after, started_at
        FROM {catalog}.ops.pipeline_manifest
        WHERE layer = 'bronze'
        ORDER BY started_at DESC
        LIMIT 6
        """
    )
    latest_by_source: dict[str, dict[str, Any]] = {}
    for row in manifest_rows:
        source = str(row[0])
        if source not in latest_by_source:
            latest_by_source[source] = {
                "status": row[1],
                "files_processed": int(row[2]),
                "rows_written": int(row[3]),
                "rows_read": int(row[4]),
                "delta_version_before": row[5],
                "delta_version_after": row[6],
                "started_at": row[7],
            }
    result.manifest = latest_by_source

    for entity, (lo, hi) in EXPECTED_BATCH_ROWS.items():
        rows = sql_query(
            f"""
            SELECT COUNT(*)
            FROM {catalog}.bronze.{entity}
            WHERE _source_file LIKE '%{batch_id}%'
            """
        )
        count = int(rows[0][0]) if rows else 0
        result.bronze_batch_rows[entity] = count
        if not (lo <= count <= hi):
            result.errors.append(
                f"{entity}: batch rows={count} expected between {lo} and {hi}"
            )

    for entity in ("products", "customers", "orders"):
        m = latest_by_source.get(entity)
        if not m:
            result.errors.append(f"manifest: no row for source={entity}")
            continue
        if m["status"] != "success":
            result.errors.append(f"manifest: {entity} status={m['status']}")
        if m["rows_written"] <= 0:
            result.errors.append(f"manifest: {entity} rows_written=0")

    result.passed = not result.errors


def wait_orders_trigger(orders_job_id: int, gen_start_ms: int) -> str:
    for _ in range(30):
        proc = run_cmd(
            [
                "databricks",
                "jobs",
                "list-runs",
                "--job-id",
                str(orders_job_id),
                "--profile",
                profile(),
                "-o",
                "json",
                "--limit",
                "5",
            ]
        )
        runs = json.loads(proc.stdout)
        runs = runs if isinstance(runs, list) else runs.get("runs", [])
        for run in runs:
            if run.get("start_time", 0) >= gen_start_ms - 120_000:
                return str(run["run_id"])
        print(f"{datetime.now(UTC).strftime('%H:%M:%S')} waiting for orders file-arrival...")
        time.sleep(10)
    return ""


def deploy(catalog: str) -> None:
    script = REPO_ROOT / "scripts" / "deploy-all-ce-jobs.sh"
    print(f"=== deploy catalog={catalog} ===")
    subprocess.run([str(script), catalog], check=True)


def cmd_run(args: argparse.Namespace) -> int:
    ensure_env()
    result = E2EResult(catalog=args.catalog)

    if args.deploy:
        deploy(args.catalog)

    ids = job_ids()
    missing = [k for k in JOB_NAMES if k != "bootstrap" and k not in ids]
    if missing:
        result.status = "failed"
        result.errors.append(f"missing job ids for: {missing}")
        emit_result(result)
        return 1

    print("job_ids", " ".join(f"{k}={ids[k]}" for k in sorted(ids)))

    if args.bootstrap:
        print("=== bootstrap ===")
        boot_run = run_now(ids["bootstrap"])
        result.runs["bootstrap"] = boot_run
        poll_run(boot_run, "bootstrap")
        show_filtered_logs(boot_run, "bootstrap")

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

    print("=== wait orders file-arrival (up to 5 min) ===")
    orders_run = wait_orders_trigger(ids["orders"], gen_start_ms)
    if orders_run:
        result.runs["orders"] = orders_run
        print(f"orders_auto_run_id={orders_run}")
    else:
        result.errors.append("orders file-arrival did not trigger within 5 min")

    print("=== manual products + customers ===")
    prod_run = run_now(ids["products"])
    cust_run = run_now(ids["customers"])
    result.runs["products"] = prod_run
    result.runs["customers"] = cust_run

    for rid, lbl in [
        (orders_run, "orders"),
        (prod_run, "products"),
        (cust_run, "customers"),
    ]:
        if rid:
            poll_run(rid, lbl)
            show_filtered_logs(rid, lbl)

    print("=== verify ===")
    verify_batch(args.catalog, batch_id, result)
    result.status = "success" if result.passed else "failed"
    emit_result(result)
    return 0 if result.passed else 1


def cmd_verify(args: argparse.Namespace) -> int:
    ensure_env()
    result = E2EResult(catalog=args.catalog, batch_id=args.batch_id, status="verify")
    verify_batch(args.catalog, args.batch_id, result)
    result.status = "success" if result.passed else "failed"
    emit_result(result)
    return 0 if result.passed else 1


def emit_result(result: E2EResult) -> None:
    print("=== E2E JSON ===")
    print(json.dumps(result.to_dict(), indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bronze CE E2E orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Full E2E: optional deploy, data gen, ingest, verify")
    run_p.add_argument("--catalog", default=DEFAULT_CATALOG)
    run_p.add_argument(
        "--deploy",
        action="store_true",
        help="Upload code and upsert all CE jobs before run",
    )
    run_p.add_argument(
        "--bootstrap",
        action="store_true",
        help="Run bootstrap job before data gen (first-time / DDL changes)",
    )

    verify_p = sub.add_parser("verify", help="SQL verify manifest + batch rows only")
    verify_p.add_argument("--catalog", default=DEFAULT_CATALOG)
    verify_p.add_argument("--batch-id", required=True)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "run":
        return cmd_run(args)
    if args.command == "verify":
        return cmd_verify(args)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
