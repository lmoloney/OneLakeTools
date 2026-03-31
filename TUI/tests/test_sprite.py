from io import StringIO

from rich.console import Console

from onelake_tui.sprite import get_welcome


def test_welcome_panel_marks_tui_as_unofficial() -> None:
    welcome = get_welcome()

    # Render the Rich renderable to plain text for assertion
    buf = StringIO()
    console = Console(file=buf, force_terminal=False, width=200)
    console.print(welcome)
    text = buf.getvalue()

    assert "Not affiliated with Microsoft" in text
