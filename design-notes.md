# Design Notes

## Architecture Overview

Bronze (raw CSV → Delta) → Silver (DQ flags) → Gold (aggregations) → SQL Dashboard.

## Data Model

See [data-model.md](data-model.md) and [database/schema.sql](database/schema.sql).

## Layer Design

Documented per layer during implementation. Optional Canvas visuals summarized here.

## DQ Strategy

See [data-quality-strategy.md](data-quality-strategy.md).
