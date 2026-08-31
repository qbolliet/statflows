"""L'extra « ducklake » est optionnel, sauf pour l'écriture.

``statflows.storage.ducklake.tables`` s'importe sans ``dt_ducklake_manager`` ;
seul ``write_dataframe`` le requiert et lève, à défaut, une ``ImportError``
nommant l'extra. L'absence est simulée via ``sys.modules``.

Porté depuis ``trade-analysis/tests/test_optional_ducklake.py``.
"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture
def ducklake_manager_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rend ``dt_ducklake_manager`` non importable pour la durée du test."""
    monkeypatch.setitem(sys.modules, "dt_ducklake_manager", None)
    for name in [k for k in list(sys.modules) if k.startswith("dt_ducklake_manager.")]:
        monkeypatch.delitem(sys.modules, name)
    for name in [
        k
        for k in list(sys.modules)
        if k in ("statflows.core.download", "statflows.storage.ducklake.tables")
    ]:
        monkeypatch.delitem(sys.modules, name)


def test_import_statflows_and_tables_without_manager(
    ducklake_manager_absent: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in [
        k for k in list(sys.modules) if k == "statflows" or k.startswith("statflows.")
    ]:
        monkeypatch.delitem(sys.modules, name)

    module = importlib.import_module("statflows")
    assert hasattr(module, "EurostatClient")

    tables = importlib.import_module("statflows.storage.ducklake.tables")
    assert tables.FACT_TABLE == "fact_table"
    assert callable(tables.fact_table_exists)


def test_write_dataframe_raises_explicit_import_error(
    ducklake_manager_absent: None,
) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    # L'import est en tête de corps : l'échec précède tout usage de la connexion,
    # ``conn`` et ``data`` peuvent donc valoir None.
    with pytest.raises(ImportError, match=r"statflows\[ducklake\]") as excinfo:
        write_dataframe(None, None, ["id"], catalog_alias="db", schema="s1")

    assert "dt-ducklake-manager" in str(excinfo.value)
