from onelake_tui.sprite import get_welcome


def test_welcome_panel_marks_tui_as_unofficial() -> None:
    welcome = get_welcome()

    assert "UNOFFICIAL TUI" in welcome
    assert "Not affiliated with Microsoft" in welcome
