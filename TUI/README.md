# OneLake TUI (Unofficial)

A terminal UI for browsing Microsoft Fabric workspaces, lakehouses, and Delta tables — right from your terminal. No portal, no notebooks, no Spark cluster required.

```bash
pip install onelake-tui
az login
onelake-tui
```

`onelake-tui` is the published PyPI package and currently bundles both the terminal UI and the `onelake_client` library.

![OneLake TUI — browsing a Fabric lakehouse](https://raw.githubusercontent.com/lmoloney/OneLakeTools/main/docs/branding/TUI-Open.jpg)

![Delta table detail view](https://raw.githubusercontent.com/lmoloney/OneLakeTools/main/docs/branding/Table-Detail.jpg)

## What is this?

OneLake TUI gives you a keyboard-driven file-manager experience for Microsoft Fabric's OneLake storage. Browse workspaces, inspect lakehouses and warehouses, preview files, and explore Delta table metadata — all without leaving the terminal.

Built with [Textual](https://textual.textualize.io/) and an async Python client for the Fabric REST and OneLake DFS APIs.

## Features

- **Three-panel layout** — workspace picker → item list → DFS file tree + detail/preview
- **Rich file preview** — press Enter on any file:
  - Markdown, JSON (NDJSON), CSV, Parquet, Avro, syntax-highlighted code
  - All previews are selectable and copyable
- **Delta table inspector** — tabbed detail view:
  - Schema, data preview (first 100 rows), transaction history, Change Data Feed
- **Schema-aware table detection** — handles both `Tables/schema/table` (mirrored DBs) and `Tables/table` (lakehouses)
- **Live search** — press `/` to filter workspaces
- **Multi-environment** — PROD, MSIT, DXT, DAILY via `--env` flag
- **Copy menu** — press `y` to choose HTTPS/ABFSS + named/GUID URI formats
- **Vim-friendly navigation** — `j/k/g/G` + `h/l`, with `Tab`/`Shift+Tab` still supported
- **Toggleable footer** — `Ctrl+F` hides/shows the status bar
- **Zero config** — uses `az login`, no service keys or config files required

## Installation

**Prerequisites:** Python 3.11+, Azure CLI (`az login`)

```bash
pip install onelake-tui    # from PyPI
onelake-tui                # launch (PROD)
onelake-tui --env msit     # Microsoft internal testing ring
```

**From source** (for development):

```bash
cd TUI
uv sync
uv run onelake-tui
```

## Quick Start

1. **Authenticate** — `az login` (if you haven't already)
2. **Launch** — `onelake-tui`
3. **Select workspace** — pick from the left panel
4. **Select item** — choose a lakehouse, warehouse, or mirrored DB
5. **Browse files** — expand folders in the tree
6. **Preview** — press Enter on any file
7. **Explore tables** — expand `Tables/` for Delta metadata
8. **Search** — press `/` to filter workspaces

## Keybindings

| Key | Action |
|-----|--------|
| `↑` / `↓` | Navigate |
| `j` / `k` | Navigate (vim-style) |
| `g` / `G` | Jump to top / bottom |
| `←` / `→` | Collapse/expand tree nodes |
| `Enter` | Preview file / Expand folder |
| `/` | Search/filter workspaces |
| `Escape` | Close search / go back |
| `h` / `l` | Previous/next panel |
| `Tab` / `Shift+Tab` | Switch panels |
| `y` | Open copy menu (HTTPS/ABFSS, named/GUID) |
| `Ctrl+F` | Toggle footer visibility |
| `r` | Refresh |
| `?` | Show help |
| `q` | Quit |

## Architecture

```
TUI/src/
├── onelake_client/            # Standalone async Python client library
│   ├── auth.py                #   Dual-scope token management
│   ├── environment.py         #   Environment ring config (PROD/MSIT/DXT/DAILY)
│   ├── _http.py               #   httpx retry + pagination
│   ├── fabric/client.py       #   Fabric REST API (control plane)
│   ├── dfs/client.py          #   OneLake DFS API (data plane)
│   ├── tables/delta.py        #   Delta table metadata reader
│   ├── tables/iceberg.py      #   Iceberg table metadata reader
│   └── models/                #   Pydantic data models
└── onelake_tui/               # Textual-based terminal UI
    ├── app.py                 #   Main app, keybindings, event wiring
    ├── app.tcss               #   Layout CSS (3-panel + footer)
    ├── workspace_picker.py    #   Flat filterable workspace list
    ├── item_list.py           #   Item list for selected workspace
    ├── tree.py                #   DFS file tree (single item)
    ├── detail.py              #   Detail/preview panel with rich rendering
    ├── sprite.py              #   OneLake-inspired splash art + shimmer animation
    ├── status_bar.py          #   3-line footer (path, shortcuts, auth)
    ├── nodes.py               #   Node dataclasses
    └── banner.py              #   Welcome screen delegate
```

**Navigation model:** Workspace → Item → DFS files/folders/tables. Each stage is a separate widget:
- `WorkspacePicker` (flat OptionList, filterable)
- `ItemList` (flat OptionList per workspace)
- `OneLakeTree` (lazy-loading tree per item)
- `DetailPanel` (context-aware: metadata on highlight, preview on Enter)

## Auth Configuration

Authentication uses `DefaultAzureCredential`, which supports (in order):

| Method | Use case |
|--------|----------|
| `az login` | Local development |
| Service principal | CI/CD pipelines |
| Managed identity | Azure-hosted environments |

## Environment Configuration

```bash
uv run onelake-tui              # PROD (default)
uv run onelake-tui --env msit   # Microsoft internal testing
uv run onelake-tui --env dxt    # Developer testing
uv run onelake-tui --env daily  # Daily builds
```

Each environment maps to the correct Fabric REST and OneLake DFS hostnames automatically.

## Debugging

```bash
tail -f ~/.onelake-tui/debug.log
```

## Development

```bash
cd TUI
uv sync --all-extras
uv run pytest           # Unit + integration tests
uv run ruff check src/  # Lint
uv run ruff format src/ # Format
```

## License

MIT
