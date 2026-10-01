"""Tests unitaires — options de tamponnage et de connexion de ``SDMXDownloader``.

Comportement figé : validation des seuils, surcharge temporaire des options
DuckLake du connecteur (``ducklake_options``), exposition des diagnostics de
tamponnage dans les métriques du ``DownloadReport``, relais des nouveaux
paramètres par ``download_updates``. Aucun catalogue ni réseau : connecteur
factice. Requiert l'extra « ducklake » (import de l'orchestrateur).
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip(
    "dt_ducklake_manager",
    reason="requiert l'extra « ducklake » (dt_ducklake_manager absent)",
)

from statflows.core.download import (  # noqa: E402
    SDMXDownloader,
    download_updates,
    iter_registry_entries,
)
from statflows.core.reports import DownloadReport  # noqa: E402


class _Connector:
    """Connecteur factice : mémorise sa configuration au moment de ``connect()``."""

    catalog_alias = "db"

    def __init__(
        self, ducklake_options: Any = None, inlining: int | None = None
    ) -> None:
        self.ducklake_options = ducklake_options
        self.data_inlining_row_limit = inlining
        self.seen: list[dict[str, Any]] = []

    def connect(self) -> str:
        self.seen.append(
            {
                "options": self.ducklake_options,
                "inlining": self.data_inlining_row_limit,
            }
        )
        return "connection"


class _Client:
    structure_registry = None


def _downloader(tmp_path: Path, connector: _Connector, **kwargs: Any) -> SDMXDownloader:
    return SDMXDownloader(
        _Client(),
        connector,
        tmp_path / "structures.json",
        tmp_path / "last_download.json",
        **kwargs,
    )


# ──────────────────────────────────────────────────────────────────────
# Validation des seuils
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs",
    [
        {"registry_flush_every": 0},
        {"registry_flush_seconds": 0},
        {"write_batch_rows": 0},
        {"write_batch_queries": -1},
    ],
)
def test_invalid_thresholds_raise(tmp_path: Path, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _downloader(tmp_path, _Connector(), **kwargs)


def test_defaults_are_unbuffered(tmp_path: Path) -> None:
    downloader = _downloader(tmp_path, _Connector())

    assert downloader._registry_flush_every == 1
    assert downloader._registry_flush_seconds is None
    assert downloader._shard_key is None
    assert downloader._batching is False
    assert downloader._update_options is None
    assert downloader._build_options is None


# ──────────────────────────────────────────────────────────────────────
# Options DuckLake : surcharge le temps de connect(), puis restauration
# ──────────────────────────────────────────────────────────────────────


def test_no_ducklake_options_leaves_connector_untouched(tmp_path: Path) -> None:
    connector = _Connector(ducklake_options={"a": 1}, inlining=5)

    _downloader(tmp_path, connector)._connect()

    assert connector.seen == [{"options": {"a": 1}, "inlining": 5}]


def test_ducklake_options_overridden_for_connect_only(tmp_path: Path) -> None:
    connector = _Connector(ducklake_options={"parquet_compression": "zstd"}, inlining=5)
    downloader = _downloader(
        tmp_path,
        connector,
        ducklake_options={"DATA_INLINING_ROW_LIMIT": 0, "target_file_size": "50MB"},
    )

    downloader._connect()

    # Option d'ATTACH (insensible à la casse) et options post-ATTACH fusionnées
    assert connector.seen == [
        {
            "options": {"parquet_compression": "zstd", "target_file_size": "50MB"},
            "inlining": 0,
        }
    ]
    # Connecteur de l'appelant restauré
    assert connector.ducklake_options == {"parquet_compression": "zstd"}
    assert connector.data_inlining_row_limit == 5


def test_recommended_options_resolved_before_merge(tmp_path: Path) -> None:
    from dt_ducklake_manager.connection import RECOMMENDED_DUCKLAKE_OPTIONS

    connector = _Connector(ducklake_options="recommended")
    downloader = _downloader(
        tmp_path, connector, ducklake_options={"target_file_size": "1GB"}
    )

    downloader._connect()

    assert connector.seen[0]["options"] == {
        **RECOMMENDED_DUCKLAKE_OPTIONS,
        "target_file_size": "1GB",
    }
    assert connector.seen[0]["inlining"] is None
    assert connector.ducklake_options == "recommended"


def test_connector_restored_when_connect_fails(tmp_path: Path) -> None:
    class _Failing(_Connector):
        def connect(self) -> str:
            raise RuntimeError("attach failed")

    connector = _Failing(inlining=7)
    downloader = _downloader(
        tmp_path, connector, ducklake_options={"data_inlining_row_limit": 0}
    )

    with pytest.raises(RuntimeError):
        downloader._connect()
    assert connector.data_inlining_row_limit == 7


# ──────────────────────────────────────────────────────────────────────
# Diagnostics et API publique
# ──────────────────────────────────────────────────────────────────────


def test_report_metrics_include_buffering_diagnostics() -> None:
    report = DownloadReport(
        n_registry_flushes=3, n_write_batches=2, rows_pending_at_stop=0
    )

    metrics = report.to_metrics()

    assert metrics["download.n_registry_flushes"] == 3.0
    assert metrics["download.n_write_batches"] == 2.0
    assert metrics["download.rows_pending_at_stop"] == 0.0


def test_download_updates_exposes_every_downloader_option() -> None:
    downloader_params = set(inspect.signature(SDMXDownloader).parameters)
    wrapper_params = set(inspect.signature(download_updates).parameters)

    assert downloader_params <= wrapper_params


def test_registry_reader_reexported_from_download() -> None:
    import statflows

    assert iter_registry_entries is statflows.iter_registry_entries
