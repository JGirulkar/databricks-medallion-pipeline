# Specification — Medallion Pipeline

## Scope

Bronze → Silver → Gold → Dashboard for customers, orders, products CSVs.

## Bronze

Ingest raw CSVs; log row counts and timestamp. No transforms.

## Silver

DQ checks with `quality_check_result` flag column. Quality metrics report.

## Gold

1. sales_by_product
2. revenue_by_customer
3. customer_segmentation

## Dashboard

Top 10 products (bar), revenue distribution (histogram), segmentation (pie).

## DQ intentional issues

~700 problematic rows — see data-quality-strategy.md.
