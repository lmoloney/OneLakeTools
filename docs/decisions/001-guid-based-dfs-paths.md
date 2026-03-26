---
id: "001"
title: "GUID-based DFS paths over displayName.Type"
status: accepted
date: 2026-03-24
tags: [onelake, dfs, api]
---

## Context

OneLake DFS supports two path formats for listing item contents: name-based (`/{wsName}/{displayName}.{Type}?resource=filesystem`) and GUID-based (`/{wsGUID}?resource=filesystem&directory={itemGUID}`). Name-based paths fail for some item types — the DFS layer's type suffix doesn't always match the Fabric REST API's type field (e.g. `MirroredDatabase` in REST vs unknown equivalent in DFS). Live testing against MSIT confirmed `MirroredDatabase` and `MountedRelationalDatabase` both 404.

## Decision

Use GUID-based paths exclusively for all DFS listing operations. Workspace GUID as the filesystem, item GUID via the `directory` query parameter.

## Rationale

- **(A) Name + Type** — readable but fails for `MirroredDatabase`, `KQLDatabase`, and potentially others. No complete mapping from REST types to DFS type suffixes exists.
- **(B) GUID paths** — universal, works for all item types, reveals hidden folders like `TableMaintenance`. Slightly less readable in logs but correct by construction.

## Consequences

All DFS list operations use `?directory={guid}` instead of URL path segments. Single-file GET/HEAD still uses URL paths (`/{wsGUID}/{itemGUID}/Files/file.csv`) which works fine. Response paths are GUID-prefixed (e.g. `{itemGUID}/Files`) requiring prefix stripping for display names.
