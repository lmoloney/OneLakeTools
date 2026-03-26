# OneLakeTools — Copilot Instructions

Terminal UI and async Python client for browsing Microsoft Fabric workspaces and OneLake storage. Built with Textual (TUI) and httpx (client).

## Build / Test / Lint Commands

```bash
cd TUI
uv sync                              # Install deps
uv run pytest                        # Run tests
uv run pytest tests/integration/     # Integration tests (needs az login)
uv run ruff check src/ tests/        # Lint
uv run ruff format src/ tests/       # Format
uv run onelake-tui                   # Launch TUI (PROD)
uv run onelake-tui --env msit        # Launch TUI (MSIT ring)
```

## Project Structure

```
TUI/src/
├── onelake_client/              # Standalone async Python client library
│   ├── __init__.py              #   OneLakeClient facade
│   ├── auth.py                  #   Dual-scope token management (Fabric + DFS)
│   ├── environment.py           #   FabricEnvironment config (PROD/MSIT/DXT/DAILY)
│   ├── exceptions.py            #   Typed exceptions (NotFoundError, ApiError)
│   ├── _http.py                 #   httpx client factory, retry, DFS pagination
│   ├── fabric/                  #   Fabric REST API (control plane)
│   │   └── client.py
│   ├── dfs/                     #   OneLake DFS API (data plane)
│   │   └── client.py
│   ├── models/                  #   Pydantic data models
│   │   ├── fabric.py            #     Workspace, Item, Lakehouse
│   │   └── filesystem.py        #     PathInfo, FileProperties
│   └── tables/                  #   Delta + Iceberg metadata readers
│       ├── delta.py
│       └── iceberg.py
└── onelake_tui/                 # Textual-based terminal UI
    ├── app.py                   #   Main app, CLI args, keybindings, search
    ├── app.tcss                 #   Textual CSS styles
    ├── tree.py                  #   OneLakeTree — lazy-loading tree widget
    ├── detail.py                #   DetailPanel — contextual info viewer
    ├── nodes.py                 #   Node dataclasses (WorkspaceNode, ItemNode, etc.)
    ├── status_bar.py            #   Footer status bar
    └── banner.py                #   ASCII art splash screen
```

## Module Responsibilities

- **onelake_client**: Standalone async library. No Textual dependency. Could be extracted to its own package.
  - **auth**: Manages two separate `DefaultAzureCredential` tokens — one for Fabric REST API, one for DFS. Token scopes are the same across all deployment rings.
  - **environment**: `FabricEnvironment` dataclass with PROD/MSIT/DXT/DAILY definitions. Only API hostnames differ per ring — scopes are shared.
  - **fabric/client**: Fabric REST API — `list_workspaces()`, `list_items()`, `get_lakehouse()`. Paginated via `continuationToken`.
  - **dfs/client**: OneLake DFS — `list_paths()`, `read_file()`, `get_properties()`, `exists()`. Uses ADLS Gen2 protocol.
  - **models/**: Pydantic v2 models with aliases for API field names (camelCase → snake_case).
  - **tables/**: Reads Delta `_delta_log/` and Iceberg `metadata/` to extract schema, version, partition info.

- **onelake_tui**: Textual TUI consuming the client library.
  - **tree**: Lazy-loads: workspaces → items → DFS paths. Uses `@work` decorators for async loading.
  - **detail**: Updates contextually based on selected node type. All dynamic values escaped with `rich.markup.escape()`.
  - **app**: Composes tree + detail + status bar. Handles `--env` CLI flag, `/` search, clipboard copy.

## Key Conventions

### DFS Path Format (CRITICAL)
- **Listing operations** use `directory` query parameter: `GET /{wsGUID}?resource=filesystem&directory={itemGUID}`
- **Single file ops** use URL path: `GET /{wsGUID}/{itemGUID}/Files/file.csv`
- **Never** put item GUID in URL path for listing — returns 400 `EmptyRequestPath`
- See [ADR-001](../docs/decisions/001-guid-based-dfs-paths.md)

### Environment / Auth
- Token scopes are ring-agnostic: `https://api.fabric.microsoft.com/.default` (Fabric) and `https://storage.azure.com/.default` (DFS) for ALL rings
- Only API hostnames change per ring. See [ADR-003](../docs/decisions/003-shared-token-scopes.md)
- MSIT Fabric REST: `msitapi.fabric.microsoft.com` (NOT `api.msit.fabric.microsoft.com`)

### Rich Markup Safety
- All dynamic values in `Static()` widgets MUST be wrapped with `rich.markup.escape()`
- Error notifications MUST use `markup=False`
- API responses can contain `[` and `]` which Rich interprets as markup tags

### Testing
- Unit tests mock httpx responses via `pytest-httpx`
- Integration tests in `tests/integration/` require `az login` and real Fabric access
- Marker `@pytest.mark.integration` for integration tests

## Decision Records

Architecture decisions live in [`docs/decisions/`](../docs/decisions/). Lightweight ADR format with YAML front matter (id, title, status, date, tags). When making architectural decisions, create a new record numbered sequentially.

## Debugging

Logs written to `~/.onelake-tui/debug.log` at DEBUG level:
```bash
tail -f ~/.onelake-tui/debug.log
```
