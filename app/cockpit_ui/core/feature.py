from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .context import AppContext

_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Feature:
    """One page of the cockpit.

    Every package under ``cockpit_ui.features`` exports exactly one ``FEATURE = Feature(...)``.
    The registry discovers it automatically, so adding or removing a feature never touches core code.
    """

    id: str
    title: str
    render: Callable[["AppContext"], None]
    icon: str = ":material/insights:"
    owner: str = ""
    plan_refs: tuple[str, ...] = ()
    order: int = 100
    requires: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self) -> None:
        if not _ID_PATTERN.match(self.id):
            raise ValueError(f"Feature id {self.id!r} must be lower_snake_case (it is used as the page URL).")
