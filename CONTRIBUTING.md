# Contributing to OneLakeTools

Thanks for your interest in contributing! This guide covers the basics.

## Reporting Bugs

Open an [issue](https://github.com/lmoloney/OneLakeTools/issues/new?template=bug_report.md) using the **bug report** template. Include:

- Steps to reproduce
- Expected vs actual behaviour
- Environment details (OS, Python version, `--env` flag)
- Relevant log output from `~/.onelake-tui/debug.log`

## Suggesting Features

Open an [issue](https://github.com/lmoloney/OneLakeTools/issues/new?template=feature_request.md) using the **feature request** template.

## Development Setup

```bash
cd TUI
uv sync --all-extras          # Install all deps
uv run pytest                 # Run unit tests
uv run ruff check src/ tests/ # Lint
uv run ruff format src/ tests/ # Format
uv run onelake-tui            # Launch the TUI
```

See [`docs/runbooks/local-dev-setup.md`](docs/runbooks/local-dev-setup.md) for the full setup guide.

## Submitting a Pull Request

1. Fork the repo and create a feature branch from `main`.
2. Make your changes. Follow existing code style (enforced by `ruff`).
3. Add or update tests as appropriate.
4. Ensure `uv run pytest` and `uv run ruff check src/ tests/` pass.
5. Update `CHANGELOG.md` under `[Unreleased]` with your changes.
6. Open a PR against `main` using the PR template.

## Code Style

- Python code is formatted and linted with [Ruff](https://docs.astral.sh/ruff/).
- Pydantic v2 models with `alias` for API field mapping (camelCase → snake_case).
- All dynamic text in Rich/Textual `Static()` widgets must use `rich.markup.escape()`.
- Unit tests mock HTTP responses via `pytest-httpx`.

## Architecture

- **`onelake_client`** is a standalone async library — no Textual dependency.
- **`onelake_tui`** is the Textual TUI consuming the client.
- Architecture decisions are recorded in [`docs/decisions/`](docs/decisions/).
