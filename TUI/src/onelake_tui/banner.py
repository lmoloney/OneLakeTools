"""ASCII art banner for the unofficial OneLake TUI.

Delegates to the sprite module for the block-art logo. This module remains
for backward compatibility.
"""

from onelake_tui.sprite import get_welcome


def get_banner():
    """Return the Rich-renderable banner for the detail panel."""
    return get_welcome()
