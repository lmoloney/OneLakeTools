"""Item list widget — flat list of Fabric items in the selected workspace."""

from __future__ import annotations

import logging
import time

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import Label, OptionList
from textual.widgets.option_list import Option

from onelake_client import OneLakeClient
from onelake_client.models import Item

logger = logging.getLogger("onelake_tui.item_list")

_CACHE_TTL = 300  # 5 minutes

_ITEM_ICONS = {
    "Lakehouse": "🏠",
    "Warehouse": "🏭",
    "Notebook": "📓",
    "Report": "📊",
    "SQLEndpoint": "🔌",
    "SemanticModel": "📐",
    "DataPipeline": "🔄",
    "Eventstream": "⚡",
    "KQLDatabase": "📡",
    "KQLQueryset": "🔍",
}

_ITEM_TAGS: dict[str, tuple[str, str]] = {
    "Lakehouse": "LH",
    "Warehouse": "WH",
    "Notebook": "NB",
    "Report": "RPT",
    "SemanticModel": "SM",
    "SQLEndpoint": "SQL",
    "DataPipeline": "PL",
    "Eventstream": "ES",
    "KQLDatabase": "KQL",
    "KQLQueryset": "KQS",
    "SparkJobDefinition": "SPK",
    "MLModel": "ML",
    "MLExperiment": "EXP",
    "Environment": "ENV",
    "Datamart": "DM",
    "MirroredDatabase": "MDB",
    "MirroredWarehouse": "MWH",
}


class ItemList(Vertical):
    """Flat list of Fabric items for the currently selected workspace."""

    class ItemSelected(Message):
        """Posted when an item is highlighted."""

        def __init__(self, workspace_id: str, workspace_name: str, item: Item) -> None:
            super().__init__()
            self.workspace_id = workspace_id
            self.workspace_name = workspace_name
            self.item = item

    DEFAULT_CSS = """
    ItemList {
        height: 1fr;
    }
    ItemList #item-header {
        height: 1;
        padding: 0 1;
        background: $primary-background;
        color: $text-muted;
        text-style: bold;
    }
    ItemList OptionList {
        height: 1fr;
        background: $surface-darken-1;
        border-right: tall $primary-background;
        padding: 0 1;
    }
    """

    def __init__(self, client: OneLakeClient, **kwargs) -> None:
        super().__init__(**kwargs)
        self.client = client
        self._items: list[Item] = []
        self._workspace_id: str = ""
        self._workspace_name: str = ""
        self._item_cache: dict[str, tuple[list[Item], float]] = {}  # ws_id → (items, fetched_at)

    def compose(self) -> ComposeResult:
        yield Label("Items", id="item-header")
        yield OptionList(id="item-option-list")

    @work(exclusive=True, group="load_items")
    async def load_items(
        self, workspace_id: str, workspace_name: str, *, force: bool = False
    ) -> None:
        """Fetch and display items for a workspace."""
        self._workspace_id = workspace_id
        self._workspace_name = workspace_name
        option_list = self.query_one("#item-option-list", OptionList)
        option_list.clear_options()
        self.query_one("#item-header", Label).update(f"📦 {workspace_name}")

        # Use cached data if still fresh
        cached = self._item_cache.get(workspace_id)
        if not force and cached is not None:
            items, fetched_at = cached
            if (time.monotonic() - fetched_at) < _CACHE_TTL:
                self._items = items
                self._render_items()
                logger.debug("Using cached items for %s (%d items)", workspace_name, len(items))
                return

        try:
            items = await self.client.fabric.list_items(workspace_id)
            self._items = sorted(items, key=lambda i: (i.type, i.display_name.casefold()))
            self._item_cache[workspace_id] = (self._items, time.monotonic())
            self._render_items()
        except Exception as e:
            option_list.add_option(Option(f"❌ {e}", disabled=True))
            logger.exception("Failed to load items for %s", workspace_name)

    def _render_items(self) -> None:
        """Populate the OptionList from the current items list."""
        option_list = self.query_one("#item-option-list", OptionList)
        option_list.clear_options()
        for item in self._items:
            icon = _ITEM_ICONS.get(item.type, "📄")
            tag = _ITEM_TAGS.get(item.type, "")
            label = f"{icon} {tag} {item.display_name}" if tag else f"{icon} {item.display_name}"
            option_list.add_option(Option(label, id=item.id))
        if not self._items:
            option_list.add_option(Option("(empty workspace)", disabled=True))

    @on(OptionList.OptionHighlighted, "#item-option-list")
    def _on_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        """Post ItemSelected when an item is highlighted."""
        if event.option is None or event.option.id is None:
            return
        item = self._item_by_id(event.option.id)
        if item:
            self.post_message(self.ItemSelected(self._workspace_id, self._workspace_name, item))

    def _item_by_id(self, item_id: str) -> Item | None:
        for item in self._items:
            if item.id == item_id:
                return item
        return None

    def clear_items(self) -> None:
        """Clear the items list and cache."""
        self._items.clear()
        self._item_cache.clear()
        self.query_one("#item-option-list", OptionList).clear_options()
        self.query_one("#item-header", Label).update("Items")
