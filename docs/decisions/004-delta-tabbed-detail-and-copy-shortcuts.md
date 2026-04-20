---
id: "004"
title: "Tabbed Delta table detail and multi-format copy shortcuts"
status: accepted
date: 2026-03-28
tags: [tui, delta, clipboard, ux]
---

## Context

The Delta table detail panel showed all metadata in a flat scrollable view — schema, version, file count, size. Users needed richer insight: data preview, transaction history, and Change Data Feed status. For copy operations, users needed fast access to both named and GUID variants of HTTPS and ABFSS URIs.

## Decision

### Tabbed Delta detail (Textual `TabbedContent`)

Replace the flat table detail with four tabs:

| Tab | Loading | Content |
|-----|---------|---------|
| Schema | Auto | Version, files, size, partitions, column DataTable |
| Data | Lazy (button) | First 100 rows via `deltalake` `to_pyarrow_dataset().head()` |
| History | Auto | Transaction log from `_delta_log/*.json` commitInfo (last 20 versions) |
| CDF | Conditional + lazy | Only visible when `delta.enableChangeDataFeed=true` in table properties |

Data preview is lazy to avoid expensive reads on large tables. The user clicks "Load Data Preview" to trigger it.

### Copy menu (`y`) with four URI targets

| Trigger | Format | Example |
|---------|--------|---------|
| `y` → option 1 | HTTPS with names | `https://onelake.dfs.fabric.microsoft.com/MyWorkspace/MyLakehouse/Tables/customers` |
| `y` → option 2 | HTTPS with GUIDs | `https://onelake.dfs.fabric.microsoft.com/{wsGUID}/{itemGUID}/Tables/customers` |
| `y` → option 3 | ABFSS with names | `abfss://MyWorkspace@onelake.dfs.fabric.microsoft.com/MyLakehouse/Tables/customers` |
| `y` → option 4 | ABFSS with GUIDs | `abfss://{wsGUID}@onelake.dfs.fabric.microsoft.com/{itemGUID}/Tables/customers` |

Clipboard uses platform-native command paths with graceful fallback when no clipboard backend is available.

## Consequences

- `fastavro` added as dependency for Avro file preview (same session).
- Tabbed UI requires Textual ≥ 0.27 (we depend on ≥ 1.0, so fine).
- Transaction log reads up to 20 JSON files from `_delta_log/` using bounded parallelism for lower latency.
- CDF tab uses `deltalake` `load_cdf()` which may not be available in all delta-rs versions. Fails gracefully.
