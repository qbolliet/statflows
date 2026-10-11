"""Client test doubles for :class:`~statflows.core.factory.ClientFactory`.

Importable by dotted path (``"tests.utils.clients.RecordingClient"`` or
``"tests.utils.clients:RecordingClient"``). ``RecordingClient`` keeps the
arguments it was built with and supports the context-manager protocol, without
any network or file access.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    # Nom importé au seul typage : annotation non résoluble à l'exécution
    from collections import OrderedDict


class Mode(Enum):
    """Enum whose values differ from the names (value vs name matching)."""

    FAST = "fast-mode"
    SLOW = "slow-mode"


class RecordingClient:
    """Client double recording its constructor arguments."""

    # Nombre d'instances construites (aucune pendant la validation d'une fabrique)
    instances: ClassVar[int] = 0

    def __init__(
        self,
        mode: Mode = Mode.FAST,
        fallback: Mode | None = None,
        label: str | Mode | None = None,
        token: str | None = None,
        timeout: int = 10,
    ) -> None:
        self.kwargs: dict[str, Any] = {
            "mode": mode,
            "fallback": fallback,
            "label": label,
            "token": token,
            "timeout": timeout,
        }
        self.closed = False
        RecordingClient.instances += 1

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> RecordingClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class UnresolvableClient:
    """Client double whose annotations cannot be resolved at runtime."""

    def __init__(self, mode: Mode = Mode.FAST, extra: OrderedDict | None = None):
        self.mode = mode
        self.extra = extra


def make_recording_client(**kwargs: Any) -> RecordingClient:
    """Function target forwarding its keyword arguments to ``RecordingClient``."""
    return RecordingClient(**kwargs)


# Cible non appelable
NOT_CALLABLE = 42
