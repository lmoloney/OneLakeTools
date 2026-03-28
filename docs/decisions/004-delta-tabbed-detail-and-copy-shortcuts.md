---
id: "004"
title: "Tabbed Delta table detail and multi-format copy shortcuts"
status: accepted
date: 2026-03-28
tags: [tui, delta, clipboard, ux]
---

## Context

The Delta table detail panel showed all metadata in a flat scrollable view — schema, version, file count, size. Users needed richer insight: data preview, transaction history, and Change Data Feed status. Separately, the single `y` shortcut only copied the named `onelake://` path, but ABFSS and HTTPS URL formats are needed for Spark notebooks and REST API calls respectively.

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

### Three copy formats

| Key | Format | Example |
|-----|--------|---------|
| `y` | Named `onelake://` | `onelake://MyWorkspace/MyLakehouse/Tables/customers` |
| `Y` (Shift) | ABFSS with GUIDs | `abfss://{wsGUID}@onelake.dfs.fabric.microsoft.com/{itemGUID}/Tables/customers` |
| `Ctrl+Y` | HTTPS DFS URL | `https://onelake.dfs.fabric.microsoft.com/{wsGUID}/{itemGUID}/Tables/customers` |

All use the existing `pbcopy` mechanism (macOS; graceful fallback to notification display).

## Consequences

- `fastavro` added as dependency for Avro file preview (same session).
- Tabbed UI requires Textual ≥ 0.27 (we depend on ≥ 1.0, so fine).
- Transaction log reads up to 20 JSON files from `_delta_log/` — could be slow on high-version tables. Capped to last 20 commits.
- CDF tab uses `deltalake` `load_cdf()` which may not be available in all delta-rs versions. Fails gracefully.
