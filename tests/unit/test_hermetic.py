"""Non-regression tests of the global hermeticity guards (see ``tests/conftest.py``)."""

from __future__ import annotations

import os
import socket

import pytest

from tests.conftest import AMBIENT_ENV_VARS


def test_socket_connect_is_forbidden() -> None:
    with (
        socket.socket() as sock,
        pytest.raises(RuntimeError, match="network access is forbidden"),
    ):
        sock.connect(("127.0.0.1", 9))


def test_create_connection_is_forbidden() -> None:
    with pytest.raises(
        RuntimeError, match=r"forbidden in unit tests: \('example\.com', 80\)"
    ):
        socket.create_connection(("example.com", 80))


@pytest.mark.parametrize("name", AMBIENT_ENV_VARS)
def test_ambient_variable_is_absent(name: str) -> None:
    # Reste vrai même si le processus pytest a reçu la variable (monkeypatch.delenv)
    assert name not in os.environ
