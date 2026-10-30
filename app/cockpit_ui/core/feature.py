from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .context import AppContext

_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

STATUSES = ("live", "preview", "planned")
DEFAULT_GROUP = "Insights"


@dataclass(frozen=True)
class Feature:
    """One page of the cockpit.

    Every package under ``cockpit_ui.features`` exports exactly one ``FEATURE = Feature(...)``.
    The registry discovers it automatically, so adding or removing a feature never touches core code.

    ``status`` drives how the shell treats the page:

    - ``live``: rendered once its ``requires`` tables exist.
    - ``preview``: rendered the same way, flagged as preview in the header.
    - ``planned``: ``render`` is never called. The shell shows a "what is coming" panel that lists
      ``requires`` as the gold tables the page will read and ``highlights`` as what it will show.
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
    status: str = "live"
    plan_id: str = ""
    group: str = DEFAULT_GROUP
    highlights: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _ID_PATTERN.match(self.id):
            raise ValueError(f"Feature id {self.id!r} must be lower_snake_case (it is used as the page URL).")
        if self.status not in STATUSES:
            raise ValueError(f"Feature {self.id!r}: status must be one of {STATUSES}, got {self.status!r}.")
        if not self.group.strip():
            raise ValueError(f"Feature {self.id!r}: group must not be empty.")

    @property
    def is_planned(self) -> bool:
        return self.status == "planned"

    @property
    def plan_label(self) -> str:
        """``plan_id`` when set, otherwise the plan refs joined (for example ``"F1 · F8"``)."""
        return self.plan_id or " · ".join(self.plan_refs)
