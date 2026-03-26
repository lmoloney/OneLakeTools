"""Item list widget — flat list of Fabric items in the selected workspace."""

from __future__ import annotations

import logging

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.message import Message
from textual.widgets import Label, OptionList
from textual.widgets.option_list import Option

from onelake_client import OneLakeClient
from onelake_client.models import Item

logger = logging.getLogger("onelake_tui.item_list")

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

    def compose(self) -> ComposeResult:
        yield Label("Items", id="item-header")
        yield OptionList(id="item-option-list")

    @work(exclusive=True, group="load_items")
    async def load_items(self, workspace_id: str, workspace_name: str) -> None:
        """Fetch and display items for a workspace."""
        self._workspace_id = workspace_id
        self._workspace_name = workspace_name
        option_list = self.query_one("#item-option-list", OptionList)
        option_list.clear_options()
        self.query_one("#item-header", Label).update(f"📦 {workspace_name}")

        try:
            items = await self.client.fabric.list_items(workspace_id)
            self._items = sorted(items, key=lambda i: (i.type, i.display_name.casefold()))
            for item in self._items:
                icon = _ITEM_ICONS.get(item.type, "📄")
                tag = _ITEM_TAGS.get(item.type, "")
                if tag:
                    label = f"{icon} {tag} {item.display_name}"
                else:
                    label = f"{icon} {item.display_name}"
                option_list.add_option(Option(label, id=item.id))

            if not self._items:
                option_list.add_option(Option("(empty workspace)", disabled=True))
        except Exception as e:
            option_list.add_option(Option(f"❌ {e}", disabled=True))
            logger.exception("Failed to load items for %s", workspace_name)

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
        """Clear the items list."""
        self._items.clear()
        self.query_one("#item-option-list", OptionList).clear_options()
        self.query_one("#item-header", Label).update("Items")
