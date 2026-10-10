"""Non-regression contract for the extraction of ``statflows`` from ``trade-analysis``.

Layout:

- ``tests/unit/`` — unit tests, mirroring the package layout
  (``core/``, ``sources/``, ``storage/json/``, ``storage/ducklake/``). No
  external service: local I/O and pure logic.
- ``tests/integration/`` — end-to-end against simulated services: S3 through
  ``moto``, file-based DuckLake catalog. Fixtures live in
  ``tests/integration/conftest.py``; every test there is automatically marked
  ``integration``.
- ``tests/utils/`` — shared test functions and classes (import
  ``from tests.utils.<module> import ...``).

These tests freeze the behaviour of the migrated code (JSON registries, DuckLake
helpers, pure helpers of the orchestrator) and must pass identically on both
sides of the extraction. No production code is modified by the tests.

Hermeticity guarantees (``autouse`` fixtures defined below):

- Ambient credentials are removed for every test: ``COMTRADE_SUBSCRIPTION_KEY``,
  ``AWS_S3_ENDPOINT``, ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and
  ``AWS_SESSION_TOKEN`` are unset, so the outcome never depends on the developer's
  environment. A test needing them sets them explicitly (e.g. the ``aws_env``
  fixture of the integration suite); autouse fixtures run before the fixtures a
  test requests, so explicit values always win.
- Tests under ``tests/unit/`` cannot open the network: ``socket.socket.connect`` and
  ``socket.create_connection`` raise an explicit error. No third-party plugin is used.
"""

from __future__ import annotations

import socket
from pathlib import Path
from typing import NoReturn

import pytest

# Racine des tests unitaires (réseau interdit)
_UNIT_DIR = Path(__file__).parent / "unit"

# Variables d'environnement ambiantes lues par le code de production
AMBIENT_ENV_VARS = (
    "COMTRADE_SUBSCRIPTION_KEY",
    "AWS_S3_ENDPOINT",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


@pytest.fixture(autouse=True)
def _neutral_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove the ambient credentials variables for every test."""
    for name in AMBIENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _forbidden(address: object) -> NoReturn:
    """Raise the error signalling a forbidden network access."""
    raise RuntimeError(f"network access is forbidden in unit tests: {address}")


@pytest.fixture(autouse=True)
def _no_network_in_unit_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Forbid socket connections for the tests located under ``tests/unit/``."""
    # Appartenance déterminée par le chemin de l'item, comme le conftest d'intégration
    if _UNIT_DIR not in Path(str(request.node.fspath)).parents:
        return

    def connect(self: socket.socket, address: object) -> NoReturn:
        _forbidden(address)

    def create_connection(address: object, *args: object, **kwargs: object) -> NoReturn:
        _forbidden(address)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket, "create_connection", create_connection)
