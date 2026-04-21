---
applyTo: "**"
---

# Code Review Instructions

When performing a code review, apply these checks in addition to standard correctness and security review.

## Changelog

- If the PR contains user-facing changes (bug fixes, new features, changed behavior) and `CHANGELOG.md` was not modified, flag this and request an entry under `[Unreleased]`.
- Changelog entries should follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format with `Added`, `Changed`, `Fixed`, or `Removed` sections.
- Changelog entries should reference the GitHub issue number where applicable (e.g. `(#19)`).
- Do not flag changelog for PRs that only change CI configuration, tests, or internal refactors with no user-visible impact.

## Documentation

- If the PR changes CLI flags, keybindings, supported file formats, or public API surface, check that `README.md` was updated accordingly. Flag if not.
- If the PR changes project structure, module responsibilities, or key conventions, check that `.github/copilot-instructions.md` was updated. Flag if not.
- Do not flag documentation for PRs that only change CI configuration, tests, or internal refactors with no user-visible impact.

## Rich markup safety

- Any dynamic string rendered in a Textual `Static()` widget must be wrapped with `rich.markup.escape()`. Flag unescaped dynamic values.
- Error notifications must use `markup=False`. Flag if not.

## Delta / pyarrow conventions

- `deltalake` >= 1.5 returns `arro3.core.Table` from `get_add_actions(flatten=True)` — use `.column(name).to_pylist()`, not `.to_pydict()`.
- `dt.schema()` returns a `Schema` object — iterate via `.fields`, not directly.
- `dt.file_uris()` replaces the old `dt.files()`.
