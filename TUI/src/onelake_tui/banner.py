"""ASCII art banner for the OneLake TUI.

Delegates to the sprite module for the block-art logo. This module remains
for backward compatibility.
"""

from onelake_tui.sprite import get_welcome


def get_banner() -> str:
    """Return the Rich-markup banner for the detail panel."""
    return get_welcome()
