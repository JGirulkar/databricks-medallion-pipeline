---
name: conventions-medallion
description: Medallion job patterns for databricks/jobs. Use when creating or editing pipeline jobs.
---

# Conventions — Medallion Jobs

Adapted from Intelo conventions-databricks (assessment subset).

## Job layout

```
databricks/jobs/{layer}/{job_name}/
├── pyproject.toml
├── src/.../main.py
└── tests/
```

## Entry point

```python
def main() -> None:
    spark = SparkSession.getActiveSession()
    if not spark:
        raise ValueError("No active Spark session")
    # implementation

if __name__ == "__main__":
    main()
```

Never call `sys.exit()` — Databricks treats as failure.

## Rules

- One job per major stage for bundle wiring
- Bronze: raw ingest + metadata logging
- Silver: flag-not-delete DQ pattern
- Gold: SQL or PySpark aggregations per spec
