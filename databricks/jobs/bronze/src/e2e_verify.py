"""One-off CE verification: bronze row counts + ingest_manifest for E2E report."""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="de_assessment")
    parser.add_argument(
        "--batch-id",
        default="",
        help="Optional landing batch stamp to filter manifest rows",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.getActiveSession()
    if spark is None:
        print("ERROR no_spark_session", file=sys.stderr)
        raise RuntimeError("No active SparkSession")

    catalog = args.catalog
    expected = {
        "products": 500,
        "customers": 10_010,
        "orders": 100_020,
    }

    print("=== BRONZE E2E VERIFY ===")
    print(f"catalog={catalog} batch_id_filter={args.batch_id or '(none)'}")

    counts: dict[str, int] = {}
    for table in ("products", "customers", "orders"):
        fqn = f"{catalog}.bronze.{table}"
        n = spark.table(fqn).count()
        counts[table] = n
        ok = "OK" if n >= expected[table] else "MISMATCH"
        print(f"TABLE {fqn} rows={n} expected>={expected[table]} status={ok}")

    manifest_sql = f"""
        SELECT batch_id, source_name, source_path, files_processed,
               rows_read, rows_written, rows_rescued, status, error_message,
               started_at, completed_at
        FROM {catalog}.bronze.ingest_manifest
        ORDER BY started_at DESC
        LIMIT 20
    """
    manifest = spark.sql(manifest_sql).collect()
    print("=== INGEST MANIFEST (latest 20) ===")
    for row in manifest:
        print(
            f"source={row.source_name} status={row.status} "
            f"files={row.files_processed} read={row.rows_read} "
            f"written={row.rows_written} rescued={row.rows_rescued} "
            f"path={row.source_path} error={row.error_message}"
        )

    summary = {
        "counts": counts,
        "expected": expected,
        "manifest_rows": len(manifest),
        "all_tables_meet_minimum": all(
            counts[t] >= expected[t] for t in expected
        ),
    }
    print("=== JSON SUMMARY ===")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
