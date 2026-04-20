"""Tests for the copy-format modal menu."""

from __future__ import annotations

import pytest
from textual.app import App, ComposeResult
from textual.widgets import Static

from onelake_tui.copy_menu import CopyFormatMenu


class _CopyMenuHarness(App):
    """Minimal harness that opens CopyFormatMenu on mount."""

    def __init__(self) -> None:
        super().__init__()
        self.result: str | None | object = "__unset__"

    def compose(self) -> ComposeResult:
        yield Static("host")

    def on_mount(self) -> None:
        self.push_screen(CopyFormatMenu(), callback=self._on_result)

    def _on_result(self, value: str | None) -> None:
        self.result = value


@pytest.mark.asyncio
async def test_copy_menu_numeric_shortcut_selects_format():
    """Pressing 1 should select HTTPS named format."""
    app = _CopyMenuHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("1")
        await pilot.pause()
        assert app.result == "https_named"


@pytest.mark.asyncio
async def test_copy_menu_escape_dismisses_with_none():
    """Escape should dismiss menu without selecting a format."""
    app = _CopyMenuHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("escape")
        await pilot.pause()
        assert app.result is None


@pytest.mark.asyncio
async def test_copy_menu_enter_selects_highlighted_option():
    """Arrow + Enter should select the highlighted option."""
    app = _CopyMenuHarness()

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down", "enter")
        await pilot.pause()
        assert app.result == "https_guid"
