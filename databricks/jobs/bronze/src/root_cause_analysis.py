"""Deep root-cause analysis for E2E ingest miss — run via jobs submit on CE."""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="de_assessment")
    parser.add_argument("--batch-id", default="20260821T084559Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    batch_id = args.batch_id
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")

    from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

    dbutils = DBUtils(spark)

    print("=== ROOT CAUSE ANALYSIS ===")
    print(f"catalog={catalog} batch_id={batch_id}")

    # 1. Landing files present?
    landing_paths = [
        f"/Volumes/{catalog}/landing/raw/products/products_{batch_id}.csv",
        f"/Volumes/{catalog}/landing/raw/customers/customers_{batch_id}.csv",
        f"/Volumes/{catalog}/landing/raw/orders/incoming/orders_{batch_id}.csv",
    ]
    print("\n--- LANDING FILES (batch) ---")
    for path in landing_paths:
        try:
            info = dbutils.fs.ls(path)
            size = info[0].size if info else 0
            print(f"PRESENT path={path} size={size}")
        except Exception as exc:
            print(f"MISSING path={path} error={exc}")

    print("\n--- LANDING DIRECTORY LISTINGS ---")
    for root in [
        f"/Volumes/{catalog}/landing/raw/products/",
        f"/Volumes/{catalog}/landing/raw/customers/",
        f"/Volumes/{catalog}/landing/raw/orders/incoming/",
        f"/Volumes/{catalog}/landing/raw/orders/processed/",
    ]:
        try:
            entries = dbutils.fs.ls(root)
            names = [e.name for e in entries[:20]]
            print(f"{root} count={len(entries)} files={names}")
        except Exception as exc:
            print(f"{root} error={exc}")

    # 2. source_config paths
    print("\n--- SOURCE_CONFIG ---")
    spark.table(f"{catalog}.config.source_config").show(truncate=False)

    # 3. Bronze row counts + batch metadata
    print("\n--- BRONZE ROW COUNTS ---")
    for table in ("products", "customers", "orders"):
        fqn = f"{catalog}.bronze.{table}"
        total = spark.table(fqn).count()
        print(f"{fqn} total_rows={total}")
        if "_batch_id" in spark.table(fqn).columns:
            spark.sql(
                f"""
                SELECT _batch_id, COUNT(*) AS rows
                FROM {fqn}
                GROUP BY _batch_id
                ORDER BY rows DESC
                LIMIT 10
                """
            ).show(truncate=False)
        if "_source_file" in spark.table(fqn).columns:
            spark.sql(
                f"""
                SELECT _source_file, COUNT(*) AS rows
                FROM {fqn}
                GROUP BY _source_file
                ORDER BY rows DESC
                LIMIT 10
                """
            ).show(truncate=False)

    # 4. Manifest for E2E window
    print("\n--- INGEST MANIFEST (all rows) ---")
    manifest = spark.sql(
        f"""
        SELECT source_name, status, files_processed, rows_read, rows_written,
               started_at, completed_at, error_message, source_path
        FROM {catalog}.bronze.ingest_manifest
        ORDER BY started_at DESC
        """
    )
    manifest.show(20, truncate=80)

    # 5. Checkpoint dirs exist?
    print("\n--- CHECKPOINT DIRS ---")
    for src in ("products", "customers", "orders"):
        cp = f"/Volumes/{catalog}/ops/checkpoints/{src}/"
        try:
            entries = dbutils.fs.ls(cp)
            print(f"{cp} entries={len(entries)} names={[e.name for e in entries[:15]]}")
        except Exception as exc:
            print(f"{cp} error={exc}")

    # 6. Does batch file path appear in bronze _source_file?
    print("\n--- BATCH FILE IN BRONZE? ---")
    for entity, path in [
        ("products", landing_paths[0]),
        ("customers", landing_paths[1]),
        ("orders", landing_paths[2]),
    ]:
        fqn = f"{catalog}.bronze.{entity}"
        if "_source_file" not in spark.table(fqn).columns:
            print(f"{entity}: no _source_file column")
            continue
        cnt = spark.table(fqn).where(f"_source_file = '{path}'").count()
        print(f"{entity} rows_from_batch_file={cnt} path={path}")

    print("\n=== END ANALYSIS ===")


if __name__ == "__main__":
    main()
