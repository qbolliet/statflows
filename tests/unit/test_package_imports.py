"""Le socle du package s'importe sans aucun extra.

``statflows`` et ses sous-paquets ``core`` / ``sources`` / ``storage`` ne doivent
tirer ni ``boto3`` (extra « s3 ») ni ``duckdb`` / ``dt_ducklake_manager``
(extra « ducklake ») au chargement. Le job « bare-imports » de la CI installe le
package nu et exécute ``tests/unit`` ; en local (extras présents) ces tests se
contentent de vérifier que les imports aboutissent.
"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "statflows",
        "statflows.core",
        "statflows.sources",
        "statflows.storage",
    ],
)
def test_base_module_imports(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert isinstance(getattr(module, "__all__", None), list)


def test_toplevel_reexports_one_client_per_provider() -> None:
    import statflows

    for name in ("EurostatClient", "OECDClient", "ComtradeClient", "UNSDClient"):
        assert hasattr(statflows, name), name


def test_storage_json_importable_without_s3_extra() -> None:
    # Après le découplage : ``Loader`` / ``Saver`` s'importent sans ``boto3`` ;
    # la connexion S3 n'est tentée qu'en présence d'un argument ``bucket``.
    from statflows.storage.json import Loader, Saver

    assert callable(Loader) and callable(Saver)


def test_ducklake_subpackage_importable_without_the_extra() -> None:
    # ``tables`` importe ``dt_ducklake_manager`` paresseusement (corps de
    # ``write_dataframe``) : le module s'importe toujours.
    import statflows.storage.ducklake.tables as tables

    assert tables.FACT_TABLE == "fact_table"
    assert callable(tables.fact_table_exists)


def test_extra_bearing_modules_are_not_imported_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Purge du cache pour une importation vraiment fraîche ; ``monkeypatch``
    # restaure les modules d'origine au démontage.
    for name in [k for k in list(sys.modules) if k.startswith("statflows")]:
        monkeypatch.delitem(sys.modules, name)

    importlib.import_module("statflows")

    assert "statflows.core.download" not in sys.modules
    assert "statflows.storage.ducklake.tables" not in sys.modules
