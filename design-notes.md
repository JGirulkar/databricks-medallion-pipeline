# Design Notes

## Architecture Overview

Bronze (raw CSV → Delta) → Silver (DQ flags) → Gold (aggregations) → SQL Dashboard.

**High-level architecture anchor (2026-08-20):** [docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md](docs/superpowers/specs/2026-08-20-medallion-bronze-architecture-design.md)

Layer details (Bronze, Silver, Gold) are hardened in separate implementation chats from this doc.

**Bronze layer design (2026-08-20):** [docs/superpowers/specs/2026-08-20-bronze-layer-design.md](docs/superpowers/specs/2026-08-20-bronze-layer-design.md)

## Data Model

See [data-model.md](data-model.md) and [database/schema.sql](database/schema.sql).

## Layer Design

Documented per layer during implementation. Optional Canvas visuals summarized here.

## DQ Strategy

See [data-quality-strategy.md](data-quality-strategy.md).
