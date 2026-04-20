"""Full-screen help overlay showing all keyboard shortcuts."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Static

_HELP_TEXT = """\
[b]Navigation[/b]
  [cyan]j / ↓[/cyan]          Move down
  [cyan]k / ↑[/cyan]          Move up
  [cyan]g / Home[/cyan]       Go to top
  [cyan]G / End[/cyan]        Go to bottom
  [cyan]Enter[/cyan]          Expand / preview

[b]Panels[/b]
  [cyan]h / Shift+Tab[/cyan]  Previous panel
  [cyan]l / Tab[/cyan]        Next panel

[b]Actions[/b]
  [cyan]y[/cyan]              Copy path (choose format)
  [cyan]/[/cyan]              Search workspaces
  [cyan]r[/cyan]              Refresh
  [cyan]S[/cyan]              Screenshot (SVG)

[b]Display[/b]
  [cyan]Ctrl+f[/cyan]         Toggle footer
  [cyan]?[/cyan]              This help screen

[b]Quit[/b]
  [cyan]q[/cyan]              Exit

[dim]Press Esc, q, or ? to close[/dim]\
"""


class HelpScreen(ModalScreen[None]):
    """Full-screen help overlay with all keyboard shortcuts."""

    BINDINGS = [
        Binding("escape", "dismiss_help", "Close", show=False),
        Binding("q", "dismiss_help", "Close", show=False),
        Binding("question_mark", "dismiss_help", "Close", show=False),
    ]

    DEFAULT_CSS = """
    HelpScreen {
        background: $surface 70%;
    }
    HelpScreen #help-root {
        width: 1fr;
        height: 1fr;
        background: $surface;
        padding: 1 3;
    }
    HelpScreen .help-title {
        width: 100%;
        text-align: left;
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }
    HelpScreen .help-scroll {
        height: 1fr;
    }
    HelpScreen .help-body {
        width: 100%;
    }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="help-root"):
            yield Static("⌨️  Keyboard Shortcuts", classes="help-title")
            with VerticalScroll(classes="help-scroll"):
                yield Static(_HELP_TEXT, classes="help-body")

    def action_dismiss_help(self) -> None:
        self.dismiss(None)
