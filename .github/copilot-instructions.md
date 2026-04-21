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
│       ├── delta.py             #     Uses deltalake lib (arro3 Table API)
│       └── iceberg.py
└── onelake_tui/                 # Textual-based terminal UI
    ├── app.py                   #   Main app, keybindings, event wiring
    ├── app.tcss                 #   Textual CSS (3-panel layout + footer)
    ├── workspace_picker.py      #   Flat filterable workspace OptionList
    ├── item_list.py             #   Item list for selected workspace
    ├── tree.py                  #   OneLakeTree — DFS file tree for one item
    ├── detail.py                #   DetailPanel — properties + rich file preview
    ├── sprite.py                #   OneLake block-art logo + shimmer animation
    ├── nodes.py                 #   Node dataclasses (FolderNode, FileNode, TableNode)
    ├── status_bar.py            #   3-line footer (path, shortcuts, auth)
    └── banner.py                #   Welcome screen delegate
```

## Module Responsibilities

- **onelake_client**: Standalone async library. No Textual dependency. Could be extracted to its own package.
  - **auth**: Manages two separate `DefaultAzureCredential` tokens — one for Fabric REST API, one for DFS. Token scopes are the same across all deployment rings.
  - **environment**: `FabricEnvironment` dataclass with PROD/MSIT/DXT/DAILY definitions. Only API hostnames differ per ring — scopes are shared.
  - **fabric/client**: Fabric REST API — `list_workspaces()`, `list_items()`, `get_lakehouse()`. Paginated via `continuationToken`.
  - **dfs/client**: OneLake DFS — `list_paths()`, `read_file()`, `get_properties()`, `exists()`. Uses ADLS Gen2 protocol.
  - **models/**: Pydantic v2 models with aliases for API field names (camelCase → snake_case).
  - **tables/delta**: Reads Delta `_delta_log/` to extract schema, version, partition info. Uses `deltalake` lib with environment-aware abfss:// URIs. Returns arro3 Tables (not pyarrow) in deltalake >= 1.0.

- **onelake_tui**: Textual TUI consuming the client library. Three-panel navigation architecture.
  - **workspace_picker**: Flat `OptionList` of workspaces. `filter(query)` rebuilds options for live search. Posts `WorkspaceSelected` message.
  - **item_list**: Flat `OptionList` of Fabric items for selected workspace. Posts `ItemSelected` message.
  - **tree**: `OneLakeTree` — loads DFS paths for a single item (not all items). `load_item()` clears and repopulates. Handles folder expansion, schema-aware table detection (checks for `_delta_log`/`metadata` to distinguish schema folders from real tables).
  - **detail**: Properties on highlight, rich file preview on Enter. Supports Markdown, JSON (NDJSON), CSV, Parquet (pyarrow), syntax-highlighted code. All previews in `TextArea(read_only=True)` for copy-paste. Delta table metadata (schema, version, files, size) via `_load_table_metadata`.
  - **sprite**: OneLake block-art logo with shimmer animation (3-char highlight sweep).
  - **app**: Composes picker-panel (search + picker + items) + content-panel (tree + detail). Handles `--env` CLI flag, `/` search, clipboard copy, `WorkspaceSelected` → items, `ItemSelected` → tree, `NodeHighlighted` → detail, `NodeSelected` → preview.

## Key Conventions

### Navigation Model
- Three-stage: Workspace (picker) → Item (item list) → DFS files (tree)
- Each stage is a flat widget — no TreeNode.display filtering (it's a no-op in Textual)
- `on_tree_node_highlighted` → updates detail panel metadata
- `on_tree_node_selected` (Enter) → triggers file preview or table metadata

### DFS Path Format (CRITICAL)
- **Listing operations** use `directory` query parameter: `GET /{wsGUID}?resource=filesystem&directory={itemGUID}`
- **Single file ops** use URL path: `GET /{wsGUID}/{itemGUID}/Files/file.csv`
- **Never** put item GUID in URL path for listing — returns 400 `EmptyRequestPath`
- See [ADR-001](../docs/decisions/001-guid-based-dfs-paths.md)

### Table Detection
- Tables under `Tables/` may be one-level (`Tables/mytable`) or two-level (`Tables/SCHEMA/mytable`)
- Detect by checking directory contents: if `_delta_log` or `metadata` exists → it's a real table; otherwise → schema folder, children are tables
- `TableNode.table_name` may contain `/` (e.g. `APPUSER/SENSOR_READINGS`)
- Delta URIs use `_build_table_uri(workspace, item_path, table_name, dfs_host)` — host varies per ring

### deltalake Library Compat (v1.5+)
- `dt.schema()` returns `Schema` object — iterate via `.fields`, not directly
- `dt.file_uris()` replaces old `dt.files()`
- `dt.get_add_actions(flatten=True)` returns `arro3.core.Table`, not pyarrow — use `.column(name).to_pylist()` instead of `.to_pydict()`
- All `@work` decorators on detail panel use `exclusive=True` to prevent double-mount crashes

### Environment / Auth
- Token scopes are ring-agnostic: `https://api.fabric.microsoft.com/.default` (Fabric) and `https://storage.azure.com/.default` (DFS) for ALL rings
- Only API hostnames change per ring. See [ADR-003](../docs/decisions/003-shared-token-scopes.md)
- MSIT Fabric REST: `msitapi.fabric.microsoft.com` (NOT `api.msit.fabric.microsoft.com`)
- Delta abfss:// URIs must use the correct DFS host per ring (passed from `FabricEnvironment.dfs_host`)

### Rich Markup Safety
- All dynamic values in `Static()` widgets MUST be wrapped with `rich.markup.escape()`
- Error notifications MUST use `markup=False`
- API responses can contain `[` and `]` which Rich interprets as markup tags

### Widget ID Safety
- Never use hardcoded `id=` on widgets mounted in `@work` async methods — they can fire multiple times
- Use CSS classes instead (e.g. `.table-loading` not `id="table-loading"`)
- Exception: `id="preview-content"` is safe because `_clear()` removes all children first

### Testing
- Unit tests mock httpx responses via `pytest-httpx`
- Integration tests in `tests/integration/` require `az login` and real Fabric access
- Marker `@pytest.mark.integration` for integration tests

## How to Add Things

### Adding a new file format preview

1. In `onelake_tui/detail.py`, add an `elif` branch in `_load_preview()` matching the file extension.
2. Read the file bytes via `self._client.dfs.read_file(ws_id, path)`.
3. Parse/render into a string and set it on the `TextArea` widget.
4. Add a test in `tests/` that mocks the HTTP response and verifies the preview renders.

### Adding a new Fabric item type

1. Add the item type constant to `onelake_client/models/fabric.py` if not already present.
2. If the item type needs special DFS handling (e.g. different root folders), update `onelake_tui/tree.py` → `load_item()`.
3. Update `onelake_tui/nodes.py` if a new node dataclass is needed.
4. Add integration test coverage in `tests/integration/`.

### Adding a new environment / ring

1. Add a new `FabricEnvironment` instance in `onelake_client/environment.py` with the correct API and DFS hostnames.
2. Register it in the `ENVIRONMENTS` dict in the same file.
3. Add the `--env` choice to the CLI arg parser in `onelake_tui/app.py`.
4. Token scopes do NOT change per ring — only hostnames differ (see [ADR-003](../docs/decisions/003-shared-token-scopes.md)).

## Changelog

Update `CHANGELOG.md` under `[Unreleased]` for every user-facing change. Follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format with `Added`, `Changed`, `Fixed`, `Removed` sections. Reference the GitHub issue number where applicable (e.g. `(#19)`).

## Decision Records

Architecture decisions live in [`docs/decisions/`](../docs/decisions/). Lightweight ADR format with YAML front matter (id, title, status, date, tags). When making architectural decisions, create a new record numbered sequentially.

## Documentation

When a change affects user-facing behavior, CLI flags, keybindings, or supported file formats, update the relevant docs:
- `README.md` for usage, installation, or feature descriptions
- `docs/runbooks/` for operational procedures
- `.github/copilot-instructions.md` if the change affects project structure, conventions, or module responsibilities

## Debugging

Logs written to `~/.onelake-tui/debug.log` at DEBUG level:
```bash
tail -f ~/.onelake-tui/debug.log
```
