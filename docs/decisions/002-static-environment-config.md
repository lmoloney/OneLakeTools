---
id: "002"
title: "Static environment config over DFS host auto-detection"
status: accepted
date: 2026-03-24
tags: [configuration, environments]
---

## Context

The TUI needs to work against multiple Fabric deployment rings (PROD, MSIT, DXT, DAILY), each with different API hostnames. Initial approach auto-detected the DFS hostname by finding the first lakehouse and extracting the host from its `oneLakeFilesPath` property. This failed when the first N workspaces had no lakehouses.

## Decision

Replace auto-detection with a static `FabricEnvironment` dataclass and `--env` CLI flag. Four predefined environments with known hostnames.

## Rationale

- **(A) Auto-detection** — zero config but fragile. Fails when no lakehouses visible, adds latency (extra API calls), can't distinguish DXT from DAILY.
- **(B) `--env` flag** — deterministic, instant, supports all rings. Cost: user must know which ring they're targeting (reasonable for internal Microsoft users).

## Consequences

Removed `detect_dfs_host()`, `_extract_host()`, `set_dfs_host()`. Environment flows through constructor injection (`env=`) to all clients. Non-PROD environment shown in TUI subtitle.
