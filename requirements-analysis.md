# Requirement Analysis

## Problem Statement

E-commerce company needs daily sales data ingested into Databricks via medallion architecture for analytics.

## Functional Requirements

- [ ] Sample data generator with intentional DQ issues
- [ ] Bronze ingest (customers, orders, products)
- [ ] Silver DQ checks (4 types) with flag-not-delete
- [ ] Gold aggregations (3 tables)
- [ ] Dashboard (3+ visualizations)

## Non-Functional Requirements

- Isolated CE profile `de-assessment-ce`
- Local pytest before CE deploy
- Full prompt history in `ai-prompts/`

## Assumptions

- CE workspace available; bundle deploy from laptop

## Edge Cases

- Orphan FKs, NULL critical fields, duplicates — intentional in sample data
