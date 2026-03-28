"""Unofficial OneLake-inspired splash sprite with shimmer animation.

Renders a wide lozenge shape using Unicode block characters and a deep
navy-to-cyan gradient reminiscent of the public OneLake/Fabric palette. A
single left-to-right shimmer highlight sweeps on first display, then settles.
"""

from __future__ import annotations

from textual.widgets import Static

# ── Fabric palette: deep navy → bright cyan ──────────────────────────────
_ROW_COLORS = [
    "#1B3A5C",
    "#1F4470",
    "#244E82",
    "#2B579A",
    "#1A6DB5",
    "#0078D4",
    "#0088E0",
    "#009AE5",
    "#00AAEB",
    "#00BCF2",
    "#28D4F8",
    "#50E6FF",
]
_SHIMMER_COLOR = "#E8F4FD"

# ── Wide lozenge shape, closer to the public OneLake mark ───────────────
_SHAPE = [
    "             ▄▄▄▄▄▄▄▄             ",
    "         ▄██████████████▄         ",
    "      ▄████████████████████▄      ",
    "   ▄██████████████████████████▄   ",
    " ▄██████████████████████████████▄ ",
    "▐████████████████████████████████▌",
    " ▀██████████████████████████████▀ ",
    "   ▀██████████████████████████▀   ",
    "      ▀████████████████████▀      ",
    "         ▀██████████████▀         ",
    "            ▀████████▀            ",
]

_WIDTH = len(_SHAPE[0])
_SHIMMER_BAND = 3

WORDMARK = "[bold #0078D4]O N E L A K E[/]"
BADGE = "[bold #FFB900]UNOFFICIAL TUI[/]"
TAGLINE = "[dim]Community-built terminal UI for Microsoft Fabric OneLake[/]"
DISCLAIMER = "[dim]Not affiliated with Microsoft[/]"


def render_sprite(shimmer_col: int | None = None) -> str:
    """Return the sprite as Rich markup, with an optional shimmer highlight."""
    lines: list[str] = []
    for row_idx, row in enumerate(_SHAPE):
        base = _ROW_COLORS[min(row_idx, len(_ROW_COLORS) - 1)]
        parts: list[str] = []
        run_color: str | None = None
        run_chars: list[str] = []

        for col_idx, ch in enumerate(row):
            if ch == " ":
                # Flush any open colour run, then emit a literal space
                if run_chars:
                    parts.append(f"[{run_color}]{''.join(run_chars)}[/]")
                    run_chars.clear()
                    run_color = None
                parts.append(" ")
            else:
                color = (
                    _SHIMMER_COLOR
                    if shimmer_col is not None
                    and shimmer_col <= col_idx < shimmer_col + _SHIMMER_BAND
                    else base
                )
                if color == run_color:
                    run_chars.append(ch)
                else:
                    if run_chars:
                        parts.append(f"[{run_color}]{''.join(run_chars)}[/]")
                    run_color = color
                    run_chars = [ch]

        if run_chars:
            parts.append(f"[{run_color}]{''.join(run_chars)}[/]")

        lines.append("".join(parts))
    return "\n".join(lines)


def get_welcome() -> str:
    """Full static welcome panel (sprite + wordmark + hints)."""
    return _build_welcome()


def _build_welcome(shimmer_col: int | None = None) -> str:
    """Build the welcome panel with an optional shimmer frame."""
    return "\n".join(
        [
            "",
            render_sprite(shimmer_col=shimmer_col),
            "",
            f"         {WORDMARK}",
            f"          {BADGE}",
            "",
            f"  {TAGLINE}",
            f"  {DISCLAIMER}",
            "  [dim]v0.1.0[/]",
            "",
            "  [dim]↑↓ Navigate  │  Enter Expand  │  / Search[/]",
            "  [dim]Tab Switch panels  │  y Copy path  │  Y ABFSS  │  ^Y URL  │  ? Help[/]",
            "",
        ]
    )


class OneLakeSprite(Static):
    """Animated unofficial OneLake-inspired splash art."""

    DEFAULT_CSS = """
    OneLakeSprite {
        width: auto;
        height: auto;
    }
    """

    _START = -_SHIMMER_BAND - 2
    _END = _WIDTH + 2

    def __init__(self, animate: bool = True, **kwargs) -> None:
        super().__init__("", markup=True, **kwargs)
        self._do_animate = animate
        self._frame = self._START

    def on_mount(self) -> None:
        self._show_frame()
        if self._do_animate:
            total = self._END - self._START
            self.set_interval(0.10, self._tick, repeat=total)

    def _tick(self) -> None:
        self._frame += 1
        self._show_frame()

    def _show_frame(self) -> None:
        col = self._frame if 0 <= self._frame < _WIDTH else None
        self.update(_build_welcome(shimmer_col=col))
