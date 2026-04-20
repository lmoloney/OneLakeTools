"""Copy-format menu — modal popup for choosing a clipboard URI format."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, OptionList
from textual.widgets.option_list import Option


class CopyFormatMenu(ModalScreen[str | None]):
    """Modal that lets the user pick a URI format for copying.

    Returns the chosen format key (e.g. ``"https_named"``) or *None* if
    dismissed with Escape.
    """

    BINDINGS = [
        Binding("escape", "dismiss_menu", "Cancel", show=False),
        Binding("1", "pick('https_named')", "HTTPS (named)", show=False),
        Binding("2", "pick('https_guid')", "HTTPS (GUID)", show=False),
        Binding("3", "pick('abfss_named')", "ABFSS (named)", show=False),
        Binding("4", "pick('abfss_guid')", "ABFSS (GUID)", show=False),
    ]

    DEFAULT_CSS = """
    CopyFormatMenu {
        align: center middle;
    }
    CopyFormatMenu > Vertical {
        width: 40;
        height: auto;
        max-height: 12;
        background: $surface;
        border: tall $primary;
        padding: 1 2;
    }
    CopyFormatMenu Label {
        width: 100%;
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }
    CopyFormatMenu OptionList {
        height: auto;
        max-height: 6;
        background: $surface;
    }
    """

    # Maps option list index → format key
    _FORMATS = [
        ("1  HTTPS (named)", "https_named"),
        ("2  HTTPS (GUID)", "https_guid"),
        ("3  ABFSS (named)", "abfss_named"),
        ("4  ABFSS (GUID)", "abfss_guid"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label("Copy path as…")
            yield OptionList(
                *[Option(label, id=key) for label, key in self._FORMATS],
            )

    def on_mount(self) -> None:
        ol = self.query_one(OptionList)
        ol.highlighted = 0
        ol.focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option.id:
            self.dismiss(event.option.id)

    def action_dismiss_menu(self) -> None:
        self.dismiss(None)

    def action_pick(self, format_key: str) -> None:
        self.dismiss(format_key)
