#!/usr/bin/env python3
"""Register or update all assessment CE jobs without delete (preserves run history)."""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr
        )
    return proc


def list_jobs(profile: str) -> dict[str, int]:
    proc = run(["databricks", "jobs", "list", "--profile", profile, "-o", "json"])
    data = json.loads(proc.stdout)
    jobs = data if isinstance(data, list) else data.get("jobs", [])
    return {
        j.get("settings", {}).get("name", ""): int(j["job_id"])
        for j in jobs
        if j.get("settings", {}).get("name")
    }


def delete_legacy_jobs(profile: str, existing: dict[str, int]) -> None:
    """Remove retired CE jobs so triggers cannot double-process silver."""
    for name in ("de_assessment_silver_conform_all",):
        job_id = existing.get(name)
        if job_id is None:
            continue
        run(["databricks", "jobs", "delete", str(job_id), "--profile", profile])
        print(f"  deleted legacy job {name} (job_id={job_id})")
        del existing[name]


def upsert_job(profile: str, settings: dict[str, Any], existing: dict[str, int]) -> int:
    name = settings["name"]
    if name in existing:
        job_id = existing[name]
        payload = json.dumps({"job_id": job_id, "new_settings": settings})
        run(
            [
                "databricks",
                "jobs",
                "update",
                "--json",
                payload,
                "--profile",
                profile,
            ]
        )
        print(f"  updated {name} (job_id={job_id})")
        return job_id

    payload = json.dumps(settings)
    proc = run(
        [
            "databricks",
            "jobs",
            "create",
            "--json",
            payload,
            "--profile",
            profile,
            "-o",
            "json",
        ]
    )
    job_id = int(json.loads(proc.stdout)["job_id"])
    print(f"  created {name} -> job_id={job_id}")
    return job_id


def spark_task(ws_root: str, catalog: str, task_key: str, python_file: str) -> dict[str, Any]:
    return {
        "task_key": task_key,
        "environment_key": "default",
        "max_retries": 0,
        "spark_python_task": {
            "python_file": f"{ws_root}/{python_file}",
            "parameters": ["--catalog", catalog],
            "source": "WORKSPACE",
        },
    }


def base_job(
    ws_root: str,
    catalog: str,
    name: str,
    task_key: str,
    python_file: str,
    *,
    schedule: dict[str, Any] | None = None,
    trigger: dict[str, Any] | None = None,
    dependencies: list[str] | None = None,
) -> dict[str, Any]:
    env_spec: dict[str, Any] = {"client": "4"}
    if dependencies:
        env_spec["dependencies"] = dependencies
    job: dict[str, Any] = {
        "name": name,
        "max_concurrent_runs": 1,
        "environments": [{"environment_key": "default", "spec": env_spec}],
        "tasks": [spark_task(ws_root, catalog, task_key, python_file)],
    }
    if schedule:
        job["schedule"] = schedule
    if trigger:
        job["trigger"] = trigger
    return job


def all_job_settings(
    catalog: str,
    bronze_ws: str,
    data_gen_ws: str,
    silver_ws: str,
) -> list[dict[str, Any]]:
    orders_incoming = f"/Volumes/{catalog}/landing/raw/orders/incoming/"
    return [
        base_job(
            data_gen_ws,
            catalog,
            "de_assessment_data_generation",
            "generate",
            "generate_sample_data.py",
            dependencies=["faker>=24.0", "pandas>=2.0"],
        ),
        base_job(bronze_ws, catalog, "de_assessment_bronze_bootstrap", "bootstrap", "bootstrap_bronze.py"),
        base_job(
            bronze_ws,
            catalog,
            "de_assessment_bronze_products",
            "ingest_products",
            "ingest_products.py",
            schedule={
                "quartz_cron_expression": "0 0 6 ? * MON",
                "timezone_id": "UTC",
                "pause_status": "PAUSED",
            },
        ),
        base_job(
            bronze_ws,
            catalog,
            "de_assessment_bronze_customers",
            "ingest_customers",
            "ingest_customers.py",
            schedule={
                "quartz_cron_expression": "0 0 6 * * ?",
                "timezone_id": "UTC",
                "pause_status": "PAUSED",
            },
        ),
        base_job(
            bronze_ws,
            catalog,
            "de_assessment_bronze_orders",
            "ingest_orders",
            "ingest_orders.py",
            trigger={
                "pause_status": "UNPAUSED",
                "file_arrival": {
                    "url": orders_incoming,
                    "wait_after_last_change_seconds": 120,
                    "min_time_between_triggers_seconds": 60,
                },
            },
        ),
        base_job(bronze_ws, catalog, "de_assessment_bronze_ingest_all", "ingest_all", "ingest_all.py"),
        base_job(
            silver_ws,
            catalog,
            "de_assessment_silver_bootstrap",
            "bootstrap",
            "bootstrap_silver.py",
        ),
        base_job(
            silver_ws,
            catalog,
            "de_assessment_silver_conform_products",
            "conform_products",
            "conform_products.py",
            trigger={
                "pause_status": "UNPAUSED",
                "table_update": {
                    "table_names": [f"{catalog}.bronze.products"],
                },
            },
        ),
        base_job(
            silver_ws,
            catalog,
            "de_assessment_silver_conform_customers",
            "conform_customers",
            "conform_customers.py",
            trigger={
                "pause_status": "UNPAUSED",
                "table_update": {
                    "table_names": [f"{catalog}.bronze.customers"],
                },
            },
        ),
        base_job(
            silver_ws,
            catalog,
            "de_assessment_silver_conform_orders",
            "conform_orders",
            "conform_orders.py",
            trigger={
                "pause_status": "UNPAUSED",
                "table_update": {
                    "table_names": [f"{catalog}.bronze.orders"],
                    "min_time_between_triggers_seconds": 60,
                },
            },
        ),
    ]


def main() -> int:
    profile = os.environ["PROFILE"]
    catalog = os.environ["CATALOG"]
    bronze_ws = os.environ["BRONZE_WS"]
    data_gen_ws = os.environ["DATA_GEN_WS"]
    silver_ws = os.environ["SILVER_WS"]

    existing = list_jobs(profile)
    delete_legacy_jobs(profile, existing)
    settings_list = all_job_settings(catalog, bronze_ws, data_gen_ws, silver_ws)
    print(f"==> Upserting ALL {len(settings_list)} assessment jobs (update-in-place, no delete)")
    for settings in settings_list:
        job_id = upsert_job(profile, settings, existing)
        existing[settings["name"]] = job_id

    print("==> Registered jobs:")
    refreshed = list_jobs(profile)
    for name in sorted(refreshed):
        if name.startswith("de_assessment_"):
            print(f"  {refreshed[name]}\t{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
