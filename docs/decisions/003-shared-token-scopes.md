---
id: "003"
title: "Shared token scopes across deployment rings"
status: accepted
date: 2026-03-24
tags: [auth, environments]
---

## Context

When adding multi-ring support, the natural assumption was that each ring has its own token audience (e.g. `https://api.msit.fabric.microsoft.com/.default` for MSIT). Testing revealed this is wrong — `AADSTS500011: resource principal not found in tenant`.

## Decision

Use the same token scopes for all deployment rings: `https://api.fabric.microsoft.com/.default` for Fabric REST, `https://storage.azure.com/.default` for DFS. Only API base URLs vary per ring.

## Rationale

Entra (Azure AD) app registrations for Fabric exist only once in the Microsoft tenant, not per ring. The ring separation is purely at the API gateway level — different hostnames route to different deployments but all accept the same bearer token.

## Consequences

`FabricEnvironment` defines `fabric_scope` and `storage_scope` as class-level defaults, not per-instance. Simplifies auth — a single `DefaultAzureCredential` token works against any ring without re-auth.
