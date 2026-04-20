"""Status bar widget for the unofficial OneLake TUI."""

from __future__ import annotations

from rich.markup import escape
from textual.reactive import reactive
from textual.widgets import Static


class StatusBar(Static):
    """3-line status bar: path, shortcuts, and auth info."""

    path: reactive[str] = reactive("", layout=False)
    item_count: reactive[int] = reactive(0, layout=False)
    auth_method: reactive[str] = reactive("az-cli", layout=False)
    env_name: reactive[str] = reactive("PROD", layout=False)
    identity: reactive[str] = reactive("", layout=False)

    def render(self) -> str:
        # Line 1: location (truncate long paths to fit)
        path_display = self.path
        if len(path_display) > 80:
            path_display = "…" + path_display[-(79):]
        line1_parts = [f"📍 {escape(path_display)}"]
        if self.item_count > 0:
            line1_parts.append(f"{self.item_count} items")
        line1 = "  │  ".join(line1_parts)

        # Line 2: keyboard shortcuts (always visible)
        line2 = (
            "j/k Navigate │ g/G Top/Bottom │ Enter Preview │ / Search │ "
            "h/l Tab Panels │ y Copy │ ^F Footer │ q Quit"
        )

        # Line 3: auth + identity + environment
        # Rich's escape() only handles [lowercase…] tags — uppercase like
        # [MSIT] slips through and gets swallowed.  Use manual \[ escaping.
        env_tag = f"\\[{self.env_name}]" if self.env_name != "PROD" else "PROD"
        identity_part = f" ({escape(self.identity)})" if self.identity else ""
        line3 = f"🔑 {self.auth_method}{identity_part}  │  {env_tag}  │  \\[?] Help"

        return f"{line1}\n{line2}\n{line3}"

    def update_path(self, path: str, item_count: int = 0) -> None:
        self.path = path
        self.item_count = item_count
