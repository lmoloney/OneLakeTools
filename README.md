# OneLakeTools

Unofficial developer tools for [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/) OneLake.

## Tools

### 🖥️ OneLake TUI (Unofficial) ([`TUI/`](TUI/))

An unofficial terminal UI for browsing Fabric workspaces, lakehouses, and Delta tables — built with [Textual](https://textual.textualize.io/).

**Features:**
- Three-panel layout: workspace picker → item list → DFS tree + preview
- Animated OneLake splash art (chafa ASCII logo + figlet `ansi_shadow` wordmark with shimmer)
- Live `/` search filtering workspaces
- Rich file preview: Markdown, JSON (NDJSON), CSV, Parquet (pyarrow), Avro (fastavro), syntax-highlighted code
- Selectable/copyable preview text
- Delta table tabbed detail: schema, data preview, transaction history, CDF
- Schema-aware table detection (mirrored DB `Tables/schema/table` + lakehouse `Tables/table`)
- Expandable tables — browse raw `_delta_log/`, parquet files
- Human-readable `onelake://` paths everywhere
- Multi-environment support via `--env` flag (PROD, MSIT, DXT, DAILY)
- 3-line status bar with keyboard shortcuts always visible
- Keyboard-driven, zero-config (uses `az login`)

**Quick start:**

```bash
cd TUI
uv sync
uv run onelake-tui              # default (PROD)
uv run onelake-tui --env msit   # Microsoft internal testing
```

See [`TUI/README.md`](TUI/README.md) for full documentation.

### 📦 OneLake Client Library ([`TUI/src/onelake_client/`](TUI/src/onelake_client/))

A standalone async Python client covering three OneLake API surfaces, with built-in environment support for PROD, MSIT, DXT, and DAILY rings:

| API | Module | Purpose |
|-----|--------|---------|
| Fabric REST | `fabric/` | Workspace/item enumeration (control plane) |
| OneLake DFS | `dfs/` | File/folder operations via ADLS Gen2 (data plane) |
| Table APIs | `tables/` | Delta Lake + Iceberg metadata (metadata plane) |

```python
from onelake_client import OneLakeClient

async with OneLakeClient() as client:
    workspaces = await client.fabric.list_workspaces()
    paths = await client.dfs.list_paths(ws_id, "MyLakehouse.Lakehouse")
```

## Authentication

All tools use [`DefaultAzureCredential`](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential), supporting:

| Method | Use case |
|--------|----------|
| `az login` | Local development |
| Service principal env vars | CI/CD pipelines |
| Managed identity | Azure-hosted environments |

## Environment Configuration

Use the `--env` flag to target different Fabric rings:

```bash
uv run onelake-tui              # PROD (default)
uv run onelake-tui --env msit   # Microsoft internal testing
uv run onelake-tui --env dxt    # Developer testing
uv run onelake-tui --env daily  # Daily builds
```

Each environment maps to the correct Fabric REST and OneLake DFS hostnames automatically.

## Development

```bash
cd TUI
uv sync --all-extras    # Install all dependencies
uv run pytest           # Run tests (43 unit + 6 integration)
uv run ruff check src/  # Lint
uv run onelake-tui      # Launch the TUI
```

## Project Structure

```
OneLakeTools/
├── TUI/
│   ├── src/
│   │   ├── onelake_client/    # Standalone async Python client library
│   │   │   ├── auth.py        #   Dual-scope token management
│   │   │   ├── _http.py       #   httpx retry + pagination
│   │   │   ├── environment.py #   Environment ring config (PROD/MSIT/DXT/DAILY)
│   │   │   ├── fabric/        #   Fabric REST API (control plane)
│   │   │   ├── dfs/           #   OneLake DFS API (data plane)
│   │   │   ├── tables/        #   Delta + Iceberg readers (metadata plane)
│   │   │   └── models/        #   Pydantic data models
│   │   └── onelake_tui/       # Textual-based terminal UI
│   │       ├── app.py         #   Main app, keybindings, event wiring
│   │       ├── workspace_picker.py  # Flat filterable workspace list
│   │       ├── item_list.py   #   Item list for selected workspace
│   │       ├── tree.py        #   DFS file tree (single item)
│   │       ├── detail.py      #   Detail/preview with rich rendering
│   │       ├── sprite.py      #   OneLake-inspired splash art + animation
│   │       ├── status_bar.py  #   3-line footer
│   │       └── nodes.py       #   Node dataclasses
│   ├── tests/                 # Unit + integration tests
│   ├── pyproject.toml         # uv-managed project config
│   └── README.md              # TUI-specific docs
└── README.md                  # This file
```

## Roadmap

| Tool | Status |
|------|--------|
| OneLake TUI (Unofficial) | ✅ Working (browse, preview, inspect, copy path) |
| File preview (MD/JSON/CSV/Parquet/Avro) | ✅ Done |
| Delta table detail (schema/data/history/CDF) | ✅ Done |
| Workspace search/filter | ✅ Done |
| OneLake CLI | 🔲 Planned (`onelake ls`, `onelake cat`, `onelake cp`) |
| Download/upload | 🔲 Planned |
| VSCode extension | 💭 Future |

## License

MIT
