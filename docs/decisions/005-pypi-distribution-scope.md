---
id: "005"
title: "PyPI distribution scope for 0.2.0b1"
status: accepted
date: 2026-04-16
tags: [packaging, pypi, distribution]
---

## Context

During beta release preparation, documentation and PR text mixed two names: `onelake-tools` and `onelake-tui`. The repository currently ships two Python packages (`onelake_tui` and `onelake_client`) from one distribution, which caused confusion about what users should install.

## Decision

- The published PyPI distribution for this project is **`onelake-tui`**.
- The `onelake-tui` distribution includes **both**:
  - `onelake_tui` (terminal UI app)
  - `onelake_client` (standalone async client library)
- Install and release guidance must consistently use:
  - `pip install onelake-tui`
  - `pip install --pre onelake-tui` for pre-releases
- A separate client-only distribution is a possible future direction, but is **not** part of `0.2.0b1`.

## Consequences

- User-facing docs and release notes must avoid `onelake-tools` install commands.
- `onelake_client` continues to be importable when users install `onelake-tui`.
- If a client-only package is introduced later, we should add a new ADR covering migration and naming.
