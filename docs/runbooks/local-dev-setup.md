# Local Development Setup

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | [python.org](https://www.python.org/downloads/) or `brew install python` |
| uv | Latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Azure CLI | Latest | `brew install azure-cli` or [docs](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) |

## Setup

```bash
# Clone the repo
git clone https://github.com/lmoloney/OneLakeTools.git
cd OneLakeTools/TUI

# Install dependencies
uv sync --all-extras

# Authenticate to Azure (required for Fabric API access)
az login
```

## Running the TUI

```bash
cd TUI
uv run onelake-tui              # PROD (default)
uv run onelake-tui --env msit   # Microsoft internal testing
uv run onelake-tui --env dxt    # Developer testing
uv run onelake-tui --env daily  # Daily builds
```

## Running Tests

```bash
cd TUI

# Unit tests (no Azure login required)
uv run pytest

# Integration tests (requires az login + real Fabric access)
uv run pytest tests/integration/
```

## Linting & Formatting

```bash
cd TUI
uv run ruff check src/ tests/    # Lint
uv run ruff format src/ tests/   # Auto-format
uv run ruff format --check src/ tests/  # Check format without changing
```

## Debugging

Logs are written to `~/.onelake-tui/debug.log` at DEBUG level:

```bash
tail -f ~/.onelake-tui/debug.log
```

## Common Issues

### `DefaultAzureCredential` fails with no credential found

Run `az login` first. The TUI uses `DefaultAzureCredential` which checks the Azure CLI token cache.

### Integration tests fail with 401/403

Your Azure identity needs access to at least one Fabric workspace. Check that `az account show` returns the correct tenant.

### `uv sync` fails on deltalake or pyarrow

These packages have native extensions. Ensure you're on a supported Python version (3.11–3.13) and platform. On macOS, Xcode Command Line Tools must be installed (`xcode-select --install`).
