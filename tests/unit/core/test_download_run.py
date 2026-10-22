"""Unit tests — orchestration of ``SDMXDownloader`` (``run`` and ``download_updates``).

Frozen behaviour: normalisation of a single query, prioritisation (never
downloaded first, then oldest first), per-query failure isolation (reported,
registry date untouched, run goes on), graceful stop on ``max_runtime`` and on
``SIGTERM`` (inside and outside the interruptible fetch window, previous handler
restored, no handler outside the main thread), raising ``on_query_complete``
callback, per-query HTTP / rate-limit cost aggregated over a client and its
composed HTTP clients, sharded registry, failed registry flush retried at the
next one, time-based registry flush, write batches (row / query thresholds,
primary-key deduplication, empty batches), arguments forwarded to
``write_dataframe``, modification dates (unchanged writes leave them, legacy
entries fall back to their download date), structure registry export, ``connect`` of the S3
``Loader`` / ``Saver``, forwarding by ``download_updates``.

No catalog, no network, no S3: ``SDMXDownloader._connect`` returns a
``MagicMock`` and ``statflows.core.download.write_dataframe_with_report`` is
replaced by a recording spy; the JSON registries live in ``tmp_path``. ``SIGTERM`` is simulated
by calling ``_handle_sigterm`` directly (no real signal is ever sent). Requires
the "ducklake" extra (import of the orchestrator).
"""

from __future__ import annotations

import dataclasses
import inspect
import logging
import signal
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

pytest.importorskip(
    "dt_ducklake_manager",
    reason="requiert l'extra « ducklake » (dt_ducklake_manager absent)",
)

import statflows.core.download as download_module  # noqa: E402
from statflows.core.download import (  # noqa: E402
    SDMXDownloader,
    _SchemaBuffer,
    download_updates,
    iter_registry_entries,
)
from statflows.core.reports import (  # noqa: E402
    DownloadReport,
    FetchReport,
    HttpStats,
    RateLimitStats,
)
from statflows.core.structures import (  # noqa: E402
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)
from statflows.sources.eurostat.client import EurostatClient  # noqa: E402
from statflows.sources.eurostat.queries import EurostatQueryRequestV30  # noqa: E402
from statflows.storage.ducklake.tables import WriteResult  # noqa: E402
from statflows.storage.json import Loader, Saver  # noqa: E402
from tests.utils.fake_sdmx import (  # noqa: E402
    T0,
    FakeClient,
    FakeClock,
    FakeQuery,
    make_queries,
    query_frame,
)

# Logger de l'orchestrateur (cible de caplog)
LOGGER = "statflows.core.download"
# Connecteur minimal : le constructeur ne lit que ``catalog_alias``
CONNECTOR = SimpleNamespace(catalog_alias="db")
# Date ancienne d'un registre pré-rempli
OLD_DATE = "2020-01-01T00:00:00+00:00"
# Clé primaire attendue pour les données simulées
PRIMARY_KEYS = ["REF_AREA", "PRODUCT", "TIME_PERIOD"]


# ──────────────────────────────────────────────────────────────────────
# Doubles, fixtures et helpers
# ──────────────────────────────────────────────────────────────────────


class _WriteSpy:
    """Stand-in for ``write_dataframe_with_report``: records every call.

    Reports a creation on the first write of a schema and an upsert afterwards,
    or raises ``fail`` when set. ``rows_changed`` set to an integer attaches an
    operation report with that many inserted rows (``None``: no report, every
    query of the batch is then considered modified).
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fail: Exception | None = None
        self.rows_changed: int | None = None

    def __call__(
        self, conn: Any, data: pd.DataFrame, primary_keys: list[str], **kwargs: Any
    ) -> WriteResult:
        if self.fail is not None:
            raise self.fail
        created = all(c["kwargs"]["schema"] != kwargs["schema"] for c in self.calls)
        self.calls.append(
            {
                "conn": conn,
                "data": data.copy(),
                "primary_keys": list(primary_keys),
                "kwargs": kwargs,
            }
        )
        report = None
        if self.rows_changed is not None:
            report = SimpleNamespace(
                rows_inserted=self.rows_changed,
                rows_updated=0,
                snapshot_before=len(self.calls),
                snapshot_after=len(self.calls) + 1,
            )
        return WriteResult(created=created, report=report)  # type: ignore[arg-type]

    def reporters(self) -> list[set[str]]:
        """Reporters written by each call, in call order."""
        return [set(c["data"]["REF_AREA"]) for c in self.calls]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Fake clock substituted for ``statflows.core.download._now``."""
    fake = FakeClock()
    monkeypatch.setattr(download_module, "_now", fake)
    return fake


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Fake DuckLake connection returned by ``SDMXDownloader._connect``."""
    connection = MagicMock(name="conn")
    monkeypatch.setattr(SDMXDownloader, "_connect", lambda self: connection)
    return connection


@pytest.fixture
def writes(monkeypatch: pytest.MonkeyPatch) -> _WriteSpy:
    """Spy substituted for ``statflows.core.download.write_dataframe_with_report``."""
    spy = _WriteSpy()
    monkeypatch.setattr(download_module, "write_dataframe_with_report", spy)
    return spy


@pytest.fixture
def sigterm_guard():
    """Restore the process SIGTERM handler whatever the test does."""
    original = signal.getsignal(signal.SIGTERM)
    yield original
    signal.signal(signal.SIGTERM, original)


def _registry_path(tmp_path: Path) -> Path:
    return tmp_path / "last_download.json"


def _downloader(tmp_path: Path, client: Any, **kwargs: Any) -> SDMXDownloader:
    """Orchestrator on the fake connector, registries in ``tmp_path``."""
    kwargs.setdefault("max_runtime", None)
    return SDMXDownloader(
        client,
        CONNECTOR,  # type: ignore[arg-type]
        tmp_path / "structures.json",
        _registry_path(tmp_path),
        **kwargs,
    )


def _read_registry(tmp_path: Path) -> dict[str, Any]:
    """Entries of the single-file dates registry (``DOWNLOADS`` root)."""
    data = Loader().load(_registry_path(tmp_path), missing_ok=True) or {}
    return data.get("DOWNLOADS", {})


def _seed_registry(tmp_path: Path, dates: dict[str, str]) -> None:
    """Pre-fill the single-file registry with ``identity_key → last_download``."""
    entries = {
        key: {"agency": "FAKE", "dataflow": "DF", "params": {}, "last_download": date}
        for key, date in dates.items()
    }
    Saver().save(_registry_path(tmp_path), {"DOWNLOADS": entries}, indent=2)


def _at(minutes: int) -> str:
    """Reference date of the query processed at rank ``minutes``."""
    return (T0 + timedelta(minutes=minutes)).isoformat()


def _alpha(*indexes: int) -> list[FakeQuery]:
    """Queries of the ``DF_ALPHA`` dataflow with the given indexes."""
    return [FakeQuery(i, "DF_ALPHA") for i in indexes]


# ──────────────────────────────────────────────────────────────────────
# Normalisation et priorisation des requêtes
# ──────────────────────────────────────────────────────────────────────


def test_single_query_object_is_accepted(tmp_path: Path, clock, conn, writes) -> None:
    query = FakeQuery(0, "DF_ALPHA")

    # Objet requête isolé, non itérable
    report = _downloader(tmp_path, FakeClient(clock)).run(query)

    assert report.n_queries_planned == 1
    assert report.processed == 1
    assert list(_read_registry(tmp_path)) == [query.identity_key()]


def test_never_downloaded_first_then_oldest(
    tmp_path: Path, clock, conn, writes
) -> None:
    q0, q1, q2, q3 = make_queries(4)
    _seed_registry(
        tmp_path,
        {
            q0.identity_key(): "2020-06-01T00:00:00+00:00",
            q1.identity_key(): "2019-06-01T00:00:00+00:00",
            # Date illisible : traitée comme jamais téléchargée
            q2.identity_key(): "not-a-date",
        },
    )
    client = FakeClient(clock)

    report = _downloader(tmp_path, client).run([q0, q1, q2, q3])

    # Jamais téléchargées (ordre d'entrée conservé), puis la plus ancienne d'abord
    order = [q2, q3, q1, q0]
    assert client.calls == [q.identity_key() for q in order]
    assert [r.identity_key for r in report.queries] == client.calls
    assert [r.incremental for r in report.queries] == [False, False, True, True]
    # Nouvelle date de référence : instant capturé avant chaque fetch
    registry = _read_registry(tmp_path)
    for rank, query in enumerate(order):
        assert registry[query.identity_key()] == {
            "agency": query.agency,
            "dataflow": query.dataflow,
            "params": query.to_dict(),
            "last_download": _at(rank),
            "last_modified": _at(rank),
            "last_modified_snapshot": None,
        }


# ──────────────────────────────────────────────────────────────────────
# Échecs isolés par requête
# ──────────────────────────────────────────────────────────────────────


def test_failed_fetch_is_reported_and_run_goes_on(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    q0, q1, q2 = make_queries(3)
    _seed_registry(tmp_path, {q1.identity_key(): OLD_DATE})

    def on_fetch(position: int, query: FakeQuery) -> None:
        if query is q1:
            raise RuntimeError("provider down")

    client = FakeClient(clock, on_fetch=on_fetch)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = _downloader(tmp_path, client).run([q0, q1, q2])

    # Échec consigné comme une donnée du rapport, la requête est publiée
    assert report.errors == 1
    assert report.processed == 2
    failed = [r for r in report.queries if r.error_type is not None]
    assert [(r.identity_key, r.error_type, r.error_message) for r in failed] == [
        (q1.identity_key(), "RuntimeError", "provider down")
    ]
    assert len(report.queries) == 3
    assert f"Query {q1.identity_key()} failed" in caplog.text
    # Date non mise à jour (requête retentée au run suivant), les autres écrites
    registry = _read_registry(tmp_path)
    assert registry[q1.identity_key()]["last_download"] == OLD_DATE
    assert registry[q0.identity_key()]["last_download"] == _at(0)
    assert registry[q2.identity_key()]["last_download"] == _at(1)
    assert writes.reporters() == [{q0.reporter}, {q2.reporter}]


@dataclass
class _NoKeyClient(FakeClient):
    """Client whose data carries neither a dimension nor ``TIME_PERIOD``."""

    def fetch_updates(
        self, query: FakeQuery, since: datetime | None, n_observations: int
    ) -> pd.DataFrame:
        super().fetch_updates(query, since, n_observations)
        return pd.DataFrame({"LABEL": [query.reporter], "OBS_VALUE": [1.0]})

    def resolve_query_structure(self, query: FakeQuery) -> None:  # type: ignore[override]
        return None


@pytest.mark.parametrize("write_batch_queries", [None, 2])
def test_query_without_primary_key_fails_its_write(
    tmp_path: Path, clock, conn, writes, write_batch_queries: int | None
) -> None:
    queries = _alpha(0, 1)
    downloader = _downloader(
        tmp_path, _NoKeyClient(clock), write_batch_queries=write_batch_queries
    )

    report = downloader.run(queries)

    # Aucune écriture tentée, erreur imputée à chaque requête (seule ou en lot)
    assert writes.calls == []
    assert report.processed == 2
    assert report.errors == 2
    assert {r.error_type for r in report.queries} == {"ValueError"}
    assert all("No primary-key column" in r.error_message for r in report.queries)
    assert report.rows_written == 0
    assert report.rows_pending_at_stop == 0
    # Aucune entrée n'avance
    assert _read_registry(tmp_path) == {}


# ──────────────────────────────────────────────────────────────────────
# Arrêt anticipé : délai maximal et SIGTERM
# ──────────────────────────────────────────────────────────────────────


def test_max_runtime_stops_between_queries(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    queries = _alpha(0, 1, 2, 3, 4)
    client = FakeClient(clock)
    # L'horloge avance d'une minute par fetch : arrêt avant la 4e requête
    downloader = _downloader(tmp_path, client, max_runtime=timedelta(minutes=3))

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = downloader.run(queries)

    assert report.stopped_early is True
    assert report.processed == 3
    assert report.n_queries_remaining == 2
    assert len(client.calls) == 3
    assert "max runtime reached" in caplog.text
    assert set(_read_registry(tmp_path)) == {q.identity_key() for q in queries[:3]}
    conn.close.assert_called_once()


def test_sigterm_during_fetch_aborts_query_and_flushes_batch(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    queries = _alpha(0, 1, 2, 3, 4)
    holder: dict[str, SDMXDownloader] = {}

    def on_fetch(position: int, query: FakeQuery) -> None:
        # Arrêt du pod simulé pendant l'attente du fournisseur
        if position == 3:
            holder["downloader"]._handle_sigterm(signal.SIGTERM, None)

    downloader = _downloader(
        tmp_path, FakeClient(clock, on_fetch=on_fetch), write_batch_queries=10
    )
    holder["downloader"] = downloader

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = downloader.run(queries)

    assert report.stopped_early is True
    assert report.processed == 3
    assert report.n_queries_remaining == 2
    assert report.rows_pending_at_stop == 0
    assert "SIGTERM received" in caplog.text
    # Lot partiel écrit en un seul appel avant la fermeture
    assert writes.reporters() == [{q.reporter for q in queries[:3]}]
    assert report.n_write_batches == 1
    # Requête interrompue ni publiée ni enregistrée
    interrupted = queries[3].identity_key()
    assert interrupted not in {r.identity_key for r in report.queries}
    assert set(_read_registry(tmp_path)) == {q.identity_key() for q in queries[:3]}
    conn.close.assert_called_once()


def test_sigterm_outside_fetch_window_only_requests_stop(
    tmp_path: Path, clock, conn, writes
) -> None:
    queries = _alpha(0, 1, 2)
    holder: dict[str, SDMXDownloader] = {}

    def on_complete(query_report) -> None:
        # Hors de la fenêtre de fetch : drapeau seul, arrêt entre deux requêtes
        if query_report.identity_key == queries[1].identity_key():
            holder["downloader"]._handle_sigterm(signal.SIGTERM, None)

    client = FakeClient(clock)
    downloader = _downloader(tmp_path, client, on_query_complete=on_complete)
    holder["downloader"] = downloader

    report = downloader.run(queries)

    assert report.stopped_early is True
    assert report.processed == 2
    assert report.n_queries_remaining == 1
    assert report.errors == 0
    assert len(client.calls) == 2
    assert set(_read_registry(tmp_path)) == {q.identity_key() for q in queries[:2]}


def test_sigterm_handler_installed_during_run_and_restored(
    tmp_path: Path, clock, conn, writes, sigterm_guard
) -> None:
    def previous(signum: int, frame: Any) -> None:
        pass

    signal.signal(signal.SIGTERM, previous)
    seen: list[Any] = []
    client = FakeClient(
        clock,
        on_fetch=lambda position, query: seen.append(signal.getsignal(signal.SIGTERM)),
    )
    downloader = _downloader(tmp_path, client)

    downloader.run(_alpha(0))

    assert seen == [downloader._handle_sigterm]
    assert signal.getsignal(signal.SIGTERM) is previous


def test_previous_handler_restored_when_connect_fails(
    tmp_path: Path, clock, monkeypatch: pytest.MonkeyPatch, sigterm_guard
) -> None:
    def previous(signum: int, frame: Any) -> None:
        pass

    def failing_connect(self: SDMXDownloader) -> None:
        raise RuntimeError("attach failed")

    signal.signal(signal.SIGTERM, previous)
    monkeypatch.setattr(SDMXDownloader, "_connect", failing_connect)

    with pytest.raises(RuntimeError, match="attach failed"):
        _downloader(tmp_path, FakeClient(clock)).run(_alpha(0))

    assert signal.getsignal(signal.SIGTERM) is previous


def test_run_outside_main_thread_installs_no_handler(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    original = signal.getsignal(signal.SIGTERM)
    seen: list[Any] = []
    client = FakeClient(
        clock,
        on_fetch=lambda position, query: seen.append(signal.getsignal(signal.SIGTERM)),
    )
    downloader = _downloader(tmp_path, client)
    result: dict[str, DownloadReport] = {}

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        thread = threading.Thread(
            target=lambda: result.update(report=downloader.run(_alpha(0, 1)))
        )
        thread.start()
        thread.join()

    assert result["report"].processed == 2
    assert seen == [original, original]
    assert signal.getsignal(signal.SIGTERM) == original
    assert "no SIGTERM handler installed" in caplog.text


# ──────────────────────────────────────────────────────────────────────
# Observabilité : rappel et coût HTTP par requête
# ──────────────────────────────────────────────────────────────────────


def test_raising_on_query_complete_warns_and_run_goes_on(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    def on_complete(query_report) -> None:
        raise RuntimeError("tracker down")

    downloader = _downloader(tmp_path, FakeClient(clock), on_query_complete=on_complete)

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = downloader.run(_alpha(0, 1, 2))

    assert report.processed == 3
    assert report.errors == 0
    assert len(report.queries) == 3
    assert caplog.text.count("on_query_complete callback failed: tracker down") == 3


class _Limiter:
    """Rate limiter exposing cumulated counters through ``get_stats()``."""

    def __init__(self) -> None:
        self.stats = RateLimitStats(remaining_requests=10)

    def acquire(self, wait: float) -> None:
        self.stats.n_acquisitions += 1
        self.stats.total_wait_seconds += wait
        self.stats.max_wait_seconds = max(self.stats.max_wait_seconds, wait)
        self.stats.remaining_requests -= 1

    def get_stats(self) -> RateLimitStats:
        return dataclasses.replace(self.stats)


@dataclass
class _MeteredClient(FakeClient):
    """Client carrying its own HTTP counters plus a composed HTTP client's."""

    stats_: HttpStats = field(default_factory=HttpStats)
    session: SimpleNamespace = field(
        default_factory=lambda: SimpleNamespace(stats_=HttpStats())
    )
    rate_limiter: _Limiter = field(default_factory=_Limiter)
    last_fetch_report_: FetchReport | None = None

    def fetch_updates(
        self, query: FakeQuery, since: datetime | None, n_observations: int
    ) -> pd.DataFrame:
        df = super().fetch_updates(query, since, n_observations)
        # Une requête sur la session propre, deux sur la session composée
        self.stats_.record(200, 0.5, 100)
        self.session.stats_.record(200, 0.25, 50)
        self.session.stats_.record_failure(429, 0.25)
        self.rate_limiter.acquire(1.5)
        self.last_fetch_report_ = FetchReport(n_requests=2, rows_fetched=len(df))
        return df


def test_http_and_rate_limit_costs_aggregate_composed_clients(
    tmp_path: Path, clock, conn, writes
) -> None:
    report = _downloader(tmp_path, _MeteredClient(clock)).run(_alpha(0, 1))

    # Coût de chaque requête isolé (différence de compteurs cumulés), deux sources
    per_query = HttpStats(
        n_requests=3,
        n_failures=1,
        total_seconds=1.0,
        total_bytes=150,
        status_counts={"200": 2, "429": 1},
    )
    assert [r.http for r in report.queries] == [per_query, per_query]
    assert [r.rate_limit for r in report.queries] == [
        RateLimitStats(1, 1.5, 1.5, 9),
        RateLimitStats(1, 1.5, 1.5, 8),
    ]
    assert [r.fetch for r in report.queries] == [
        FetchReport(n_requests=2, rows_fetched=4)
    ] * 2


# ──────────────────────────────────────────────────────────────────────
# Registre des dates : fragments, échec de flush, seuil temporel
# ──────────────────────────────────────────────────────────────────────


def test_registry_shard_key_writes_sanitised_fragments(
    tmp_path: Path, clock, conn, writes
) -> None:
    alpha, beta = FakeQuery(0, "DF_ALPHA"), FakeQuery(1, "DF_BETA")
    absent = "FAKE:DF_OLD:R09999"
    _seed_registry(tmp_path, {alpha.identity_key(): OLD_DATE, absent: OLD_DATE})
    historical = _registry_path(tmp_path).read_bytes()
    downloader = _downloader(
        tmp_path, FakeClient(clock), registry_shard_key=lambda q: f"{q.dataflow}@x"
    )

    downloader.run([alpha, beta])

    # Un fragment par clé normalisée ; entrée absente du run → fragment par défaut
    fragments = tmp_path / "last_download"
    assert sorted(p.name for p in fragments.iterdir()) == [
        "DF_ALPHA_x.json",
        "DF_BETA_x.json",
        "_default.json",
    ]
    loader = Loader()
    assert set(loader.load(fragments / "DF_ALPHA_x.json")["DOWNLOADS"]) == {
        alpha.identity_key()
    }
    assert set(loader.load(fragments / "_default.json")["DOWNLOADS"]) == {absent}
    # Fichier historique laissé intact, lecture transparente des deux formats
    assert _registry_path(tmp_path).read_bytes() == historical
    entries = {
        e.identity_key: e for e in iter_registry_entries(_registry_path(tmp_path))
    }
    assert set(entries) == {alpha.identity_key(), beta.identity_key(), absent}
    assert entries[alpha.identity_key()].last_download.isoformat() == _at(1)


def test_failed_registry_flush_is_retried_at_next_flush(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    queries = _alpha(1, 2, 3)
    # Requêtes vides : seules les écritures du registre passent par le saver
    downloader = _downloader(tmp_path, FakeClient(clock, data_every=1000))
    original = downloader._saver.save
    attempts: list[list[str]] = []

    def flaky_save(filepath, obj=None, *args, **kwargs):
        attempts.append(sorted(obj["DOWNLOADS"]))
        if len(attempts) == 1:
            raise OSError("disk full")
        return original(filepath, obj, *args, **kwargs)

    downloader._saver.save = flaky_save  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = downloader.run(queries)

    keys = [q.identity_key() for q in queries]
    assert "Registry flush failed, retried at the next one: disk full" in caplog.text
    # L'entrée du flush en échec reste en mémoire et part au flush suivant
    assert attempts == [keys[:1], keys[:2], keys]
    assert report.n_registry_flushes == 2
    assert report.errors == 0
    assert sorted(_read_registry(tmp_path)) == keys


def test_registry_flush_seconds_threshold(
    tmp_path: Path, clock, conn, writes, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = {"t": 0.0}
    monkeypatch.setattr(
        download_module, "time", SimpleNamespace(monotonic=lambda: now["t"])
    )

    def on_fetch(position: int, query: FakeQuery) -> None:
        now["t"] += 6.0

    client = FakeClient(clock, data_every=1000, on_fetch=on_fetch)
    downloader = _downloader(
        tmp_path, client, registry_flush_every=100, registry_flush_seconds=10
    )
    saved: list[int] = []
    original = downloader._saver.save

    def spy(filepath, obj=None, *args, **kwargs):
        saved.append(len(obj["DOWNLOADS"]))
        return original(filepath, obj, *args, **kwargs)

    downloader._saver.save = spy  # type: ignore[method-assign]

    report = downloader.run(_alpha(1, 2, 3, 4))

    # 6 s par requête : seuil de 10 s franchi à la 2e puis à la 4e validation
    assert saved == [2, 4]
    assert report.n_registry_flushes == 2


# ──────────────────────────────────────────────────────────────────────
# Registre des dates : date de dernière modification
# ──────────────────────────────────────────────────────────────────────


def test_unchanged_write_advances_download_but_not_modification(
    tmp_path: Path, clock, conn, writes
) -> None:
    query = FakeQuery(0, "DF_ALPHA")
    key = query.identity_key()
    # Premier run : création du schéma, données modifiées
    writes.rows_changed = 4
    first = _downloader(tmp_path, FakeClient(clock)).run(query)
    entry = _read_registry(tmp_path)[key]

    assert first.n_modified == 1
    assert first.queries[0].data_modified is True
    assert entry["last_modified"] == entry["last_download"] == _at(0)
    assert entry["last_modified_snapshot"] == 2

    # Second run : upsert de valeurs inchangées (aucune ligne écrite)
    writes.rows_changed = 0
    second = _downloader(tmp_path, FakeClient(clock)).run(query)
    entry = _read_registry(tmp_path)[key]

    assert second.n_modified == 0
    assert second.queries[0].data_modified is False
    assert entry["last_download"] == _at(1)
    assert entry["last_modified"] == _at(0)
    assert entry["last_modified_snapshot"] == 2

    # Troisième run : valeurs modifiées, nouvelle date et nouveau snapshot
    writes.rows_changed = 1
    third = _downloader(tmp_path, FakeClient(clock)).run(query)
    entry = _read_registry(tmp_path)[key]

    assert third.n_modified == 1
    assert entry["last_modified"] == entry["last_download"] == _at(2)
    assert entry["last_modified_snapshot"] == 4


def test_empty_query_carries_over_legacy_download_date(
    tmp_path: Path, clock, conn, writes
) -> None:
    empty, fresh = FakeQuery(1, "DF_ALPHA"), FakeQuery(3, "DF_ALPHA")
    # Entrée héritée, sans champs de modification
    _seed_registry(tmp_path, {empty.identity_key(): OLD_DATE})

    # data_every=2 : requêtes d'indice impair vides
    report = _downloader(tmp_path, FakeClient(clock, data_every=2)).run([empty, fresh])
    registry = _read_registry(tmp_path)

    assert report.n_modified == 0
    assert [r.data_modified for r in report.queries] == [False, False]
    # Héritée : repli sur l'ancienne date de téléchargement
    assert registry[empty.identity_key()]["last_modified"] == OLD_DATE
    # Jamais écrite : aucune modification
    assert registry[fresh.identity_key()]["last_modified"] is None
    assert registry[fresh.identity_key()]["last_modified_snapshot"] is None


# ──────────────────────────────────────────────────────────────────────
# Écritures DuckLake : transmission des options, structures, lots
# ──────────────────────────────────────────────────────────────────────


def test_unbuffered_write_forwards_options(tmp_path: Path, clock, conn, writes) -> None:
    # Index pairs non vides ; deux écritures DF_ALPHA (création puis upsert)
    queries = [
        FakeQuery(0, "DF_ALPHA"),
        FakeQuery(1, "DF_ALPHA"),
        FakeQuery(2, "DF_BETA"),
        FakeQuery(4, "DF_ALPHA"),
    ]
    downloader = _downloader(
        tmp_path,
        FakeClient(clock, data_every=2),
        categorical_threshold=50,
        update_options={"allow_new_columns": True},
        build_options={"partition_by": ["REF_AREA"]},
        run_id="run-1",
    )

    report = downloader.run(queries)

    # Un appel par requête non vide, DataFrame transmis tel quel
    non_empty = [queries[0], queries[2], queries[3]]
    assert len(writes.calls) == 3
    for call, query in zip(writes.calls, non_empty, strict=True):
        assert call["conn"] is conn
        pd.testing.assert_frame_equal(call["data"], query_frame(query))
        assert call["primary_keys"] == PRIMARY_KEYS
        assert call["kwargs"] == {
            "catalog_alias": "db",
            "schema": query.dataflow,
            "categorical_threshold": 50,
            "label": query.dataflow,
            "update_options": {"allow_new_columns": True},
            "build_options": {"partition_by": ["REF_AREA"]},
            "run_id": "run-1",
            "commit_message": f"statflows {query.dataflow}",
        }
    assert report.n_write_batches == 3
    assert report.n_tables_created == 2
    assert report.rows_written == 12
    assert report.empty == 1
    by_key = {r.identity_key: r for r in report.queries}
    assert [
        (by_key[q.identity_key()].rows_written, by_key[q.identity_key()].table_created)
        for q in queries
    ] == [(4, True), (0, False), (4, True), (4, False)]
    assert by_key[queries[1].identity_key()].empty is True
    assert by_key[queries[2].identity_key()].schema == "DF_BETA"


def test_no_run_id_means_no_commit_message(tmp_path: Path, clock, conn, writes) -> None:
    _downloader(tmp_path, FakeClient(clock)).run(_alpha(0))

    assert writes.calls[0]["kwargs"]["run_id"] is None
    assert writes.calls[0]["kwargs"]["commit_message"] is None


def test_structure_registry_exported_then_reused(
    tmp_path: Path, clock, conn, writes
) -> None:
    queries = [FakeQuery(0, "DF_ALPHA"), FakeQuery(7, "DF_BETA")]
    structures_path = tmp_path / "structures.json"

    # 1. Structures inconnues : résolues puis exportées
    first = _downloader(tmp_path, FakeClient(clock)).run(queries)
    assert first.n_structures_fetched == 2
    exported = DataflowStructureRegistry()
    exported.load_from_dict(Loader().load(structures_path))
    assert exported.has("FAKE", "DF_ALPHA") and exported.has("FAKE", "DF_BETA")

    # 2. Registre rechargé : aucune structure nouvelle, aucun export
    second = _downloader(tmp_path, FakeClient(clock))
    saved: list[str] = []
    original = second._saver.save

    def spy(filepath, *args, **kwargs):
        saved.append(Path(filepath).name)
        return original(filepath, *args, **kwargs)

    second._saver.save = spy  # type: ignore[method-assign]
    assert second.run(queries).n_structures_fetched == 0
    assert "structures.json" not in saved

    # 3. Registre neuf demandé : structures de nouveau résolues
    third = _downloader(tmp_path, FakeClient(clock), fresh_registry=True)
    assert third.run(queries).n_structures_fetched == 2


def test_write_batch_queries_threshold(tmp_path: Path, clock, conn, writes) -> None:
    queries = _alpha(0, 1, 2, 3, 4)

    report = _downloader(tmp_path, FakeClient(clock), write_batch_queries=2).run(
        queries
    )

    # Lots de deux requêtes, reliquat écrit en fin de run
    assert writes.reporters() == [
        {"R00000", "R00001"},
        {"R00002", "R00003"},
        {"R00004"},
    ]
    assert [len(c["data"]) for c in writes.calls] == [8, 8, 4]
    assert report.n_write_batches == 3
    assert report.n_tables_created == 1
    assert report.rows_pending_at_stop == 0
    # Publication à la complétion du lot, création imputée au 1er item du 1er lot
    assert [r.identity_key for r in report.queries] == [
        q.identity_key() for q in queries
    ]
    assert [r.table_created for r in report.queries] == [
        True,
        False,
        False,
        False,
        False,
    ]
    assert len(_read_registry(tmp_path)) == 5


def test_write_batch_rows_threshold(tmp_path: Path, clock, conn, writes) -> None:
    # 4 lignes par requête, deux dataflows alternés : un lot par schéma
    queries = [FakeQuery(i, ("DF_ALPHA", "DF_BETA")[i % 2]) for i in range(7)]

    report = _downloader(tmp_path, FakeClient(clock), write_batch_rows=10).run(queries)

    # Écriture dès 12 lignes (≥ 10), reliquat DF_ALPHA écrit en fin de run
    assert [(c["kwargs"]["schema"], len(c["data"])) for c in writes.calls] == [
        ("DF_ALPHA", 12),
        ("DF_BETA", 12),
        ("DF_ALPHA", 4),
    ]
    assert writes.reporters() == [
        {"R00000", "R00002", "R00004"},
        {"R00001", "R00003", "R00005"},
        {"R00006"},
    ]
    assert report.n_write_batches == 3
    assert report.rows_pending_at_stop == 0


@dataclass
class _OverlappingClient(FakeClient):
    """Client whose rows of ``overlapping`` products all fall on ``R00000``.

    Values carry the query index, so the winner of a duplicated key is visible.
    """

    overlapping: tuple[str, ...] = ("P1", "P2")

    def fetch_updates(
        self, query: FakeQuery, since: datetime | None, n_observations: int
    ) -> pd.DataFrame:
        df = super().fetch_updates(query, since, n_observations)
        df.loc[df["PRODUCT"].isin(self.overlapping), "REF_AREA"] = "R00000"
        return df.assign(OBS_VALUE=float(query.index))


def _run_overlapping_batch(
    tmp_path: Path, clock: FakeClock, overlapping: tuple[str, ...] = ("P1", "P2")
) -> DownloadReport:
    """Run two overlapping queries of one dataflow in a single write batch."""
    client = _OverlappingClient(clock, overlapping=overlapping)
    downloader = _downloader(tmp_path, client, write_batch_queries=2)
    return downloader.run(_alpha(0, 1))


def test_batch_with_duplicate_primary_keys_keeps_last_and_warns(
    tmp_path: Path, clock, conn, writes, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        report = _run_overlapping_batch(tmp_path, clock)

    # Un seul appel, clé primaire unique, valeurs de la dernière requête
    assert len(writes.calls) == 1
    data = writes.calls[0]["data"]
    assert len(data) == 4
    assert not data.duplicated(subset=PRIMARY_KEYS).any()
    assert set(data["OBS_VALUE"]) == {1.0}
    assert "DF_ALPHA: 4 duplicated primary key(s) dropped" in caplog.text
    assert report.errors == 0
    assert len(_read_registry(tmp_path)) == 2


@pytest.mark.parametrize(
    ("overlapping", "rows_per_query"),
    [
        # Toutes les clés de la 1re requête supplantées par la 2e
        (("P1", "P2"), [0, 4]),
        # Seules les lignes P1 se recouvrent : 2 lignes de la 1re requête survivent
        (("P1",), [2, 4]),
    ],
)
def test_rows_written_counts_rows_after_deduplication(
    tmp_path: Path,
    clock,
    conn,
    writes,
    overlapping: tuple[str, ...],
    rows_per_query: list[int],
) -> None:
    report = _run_overlapping_batch(tmp_path, clock, overlapping)

    # Lignes réellement transmises à l'écriture, réparties entre les requêtes
    assert report.rows_written == len(writes.calls[0]["data"]) == sum(rows_per_query)
    assert [r.rows_written for r in report.queries] == rows_per_query


def test_empty_queries_in_batched_mode_write_nothing(
    tmp_path: Path, clock, conn, writes
) -> None:
    queries = _alpha(1, 2, 3)
    downloader = _downloader(
        tmp_path, FakeClient(clock, data_every=1000), write_batch_queries=2
    )

    report = downloader.run(queries)

    assert writes.calls == []
    assert report.n_write_batches == 0
    assert report.empty == 3
    assert report.rows_pending_at_stop == 0
    assert sorted(_read_registry(tmp_path)) == sorted(q.identity_key() for q in queries)


def test_flush_buffer_on_empty_or_unknown_batch_is_noop(
    tmp_path: Path, clock, conn, writes
) -> None:
    downloader = _downloader(tmp_path, FakeClient(clock))
    downloader._buffers["DF_ALPHA"] = _SchemaBuffer(dataflow="DF_ALPHA", structure=None)
    report = DownloadReport()

    # Lot vide puis schéma inconnu : aucun appel d'écriture
    downloader._flush_buffer(conn, "DF_ALPHA", report)
    downloader._flush_buffer(conn, "UNKNOWN", report)

    assert writes.calls == []
    assert downloader._buffers == {}
    assert report == DownloadReport()


# ──────────────────────────────────────────────────────────────────────
# Stockage S3 des registres
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("bucket", "storage_options", "connected"),
    [
        ("bkt", {"endpoint_url": "http://s3.local", "verify": False}, True),
        ("bkt", None, False),
        ("bkt", {}, False),
        (None, {"endpoint_url": "http://s3.local"}, False),
    ],
)
def test_storage_options_connect_loader_and_saver(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bucket: str | None,
    storage_options: dict[str, Any] | None,
    connected: bool,
) -> None:
    loader_cls, saver_cls = MagicMock(name="Loader"), MagicMock(name="Saver")
    # Registre absent : aucune entrée, aucun fragment
    loader_cls.return_value.load.return_value = None
    loader_cls.return_value.list_json.return_value = []
    monkeypatch.setattr(download_module, "Loader", loader_cls)
    monkeypatch.setattr(download_module, "Saver", saver_cls)

    _downloader(
        tmp_path,
        FakeClient(FakeClock()),
        bucket=bucket,
        storage_options=storage_options,
    )

    loader, saver = loader_cls.return_value, saver_cls.return_value
    if connected:
        loader.connect.assert_called_once_with(**storage_options)
        saver.connect.assert_called_once_with(**storage_options)
    else:
        loader.connect.assert_not_called()
        saver.connect.assert_not_called()
    # Registres lus sur le bucket demandé
    assert {c.kwargs["bucket"] for c in loader.load.call_args_list} == {bucket}


# ──────────────────────────────────────────────────────────────────────
# download_updates
# ──────────────────────────────────────────────────────────────────────


def test_download_updates_forwards_every_argument_then_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Any] = []
    sentinel = DownloadReport(processed=42)

    class _RecordingDownloader:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args, self.kwargs, self.runs = args, kwargs, []
            created.append(self)

        def run(self, queries: Any) -> DownloadReport:
            self.runs.append(queries)
            return sentinel

    monkeypatch.setattr(download_module, "SDMXDownloader", _RecordingDownloader)
    client, connector, queries = object(), object(), [object()]
    # Une valeur différente du défaut pour chaque option
    options: dict[str, Any] = {
        "n_observations": 3,
        "fresh_registry": True,
        "max_runtime": timedelta(minutes=5),
        "categorical_threshold": 7,
        "bucket": "bkt",
        "storage_options": {"verify": False},
        "on_query_complete": lambda report: None,
        "registry_flush_every": 4,
        "registry_flush_seconds": 2.5,
        "registry_shard_key": lambda query: "shard",
        "write_batch_rows": 100,
        "write_batch_queries": 10,
        "update_options": {"allow_new_columns": True},
        "build_options": {"partition_by": ["REF_AREA"]},
        "ducklake_options": {"data_inlining_row_limit": 0},
        "run_id": "run-42",
    }
    keyword_only = {
        name
        for name, param in inspect.signature(download_updates).parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    }
    assert set(options) == keyword_only

    result = download_updates(
        client, queries, connector, "structures.json", "last.json", **options
    )

    assert result is sentinel
    [downloader] = created
    assert downloader.args == (client, connector, "structures.json", "last.json")
    assert downloader.kwargs.keys() == options.keys()
    assert all(downloader.kwargs[name] is value for name, value in options.items())
    assert downloader.runs == [queries]


# ──────────────────────────────────────────────────────────────────────
# Codes de dimension ambigus (Namibie, code « NA »)
# ──────────────────────────────────────────────────────────────────────


def test_namibia_never_reaches_the_write_with_a_null_key(
    tmp_path: Path, conn, writes
) -> None:
    dimensions = ["freq", "reporter", "partner", "product", "flow", "indicators"]
    csv = (
        "DATAFLOW,LAST UPDATE,freq,reporter,partner,product,flow,indicators,"
        "TIME_PERIOD,OBS_VALUE,OBS_FLAG\n"
        "ESTAT:DS-045409(1.0),01/10/26,A,BE,NA,85423990,1,VALUE_IN_EUROS,2020,7,\n"
        "ESTAT:DS-045409(1.0),01/10/26,A,BE,NO,85423990,1,VALUE_IN_EUROS,2020,:,\n"
        "ESTAT:DS-045409(1.0),01/10/26,A,BE,NL,85423990,2,VALUE_IN_EUROS,2020,9,\n"
    )
    client = EurostatClient(auto_load_rate_limit=False)
    client._comext_client = MagicMock()
    client._comext_client.get.return_value = SimpleNamespace(content=csv.encode())
    downloader = _downloader(tmp_path, client)
    # Structure enregistrée dans le registre adopté par l'orchestrateur
    client.register_structure(
        DataflowStructure(
            "ESTAT",
            "DS-045409",
            len(dimensions),
            [DimensionInfo(name, i) for i, name in enumerate(dimensions)],
        )
    )
    query = EurostatQueryRequestV30(
        "DS-045409", dimensions={"reporter": "BE"}, on_duplicate="ignore"
    )

    report = downloader.run([query])

    assert report.errors == 0
    [call] = writes.calls
    assert call["primary_keys"] == [*dimensions, "TIME_PERIOD"]
    data = call["data"]
    # Aucune clé nulle n'atteint l'écriture, la Namibie est bien présente
    assert not data[call["primary_keys"]].isna().any().any()
    assert sorted(data["partner"]) == ["NA", "NL", "NO"]


# ──────────────────────────────────────────────────────────────────────
# Vérification de fraîcheur : un appel par dataflow et par run
# ──────────────────────────────────────────────────────────────────────

# Artefact dataflow Comext : données mises à jour avant le dernier téléchargement
_COMEXT_DATAFLOW_XML = (
    '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message"'
    ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common">'
    "<com:Annotation><com:AnnotationType>UPDATE_DATA</com:AnnotationType>"
    "<com:AnnotationTitle>2025-12-01T11:00:00+0100</com:AnnotationTitle>"
    "</com:Annotation></mes:Structure>"
)


class _TickingClock:
    """Shared UTC clock advancing one second on every reading."""

    def __init__(self) -> None:
        self.readings: list[datetime] = []

    def __call__(self) -> datetime:
        self.readings.append(T0 + timedelta(seconds=len(self.readings)))
        return self.readings[-1]


def _comext_client(monkeypatch: pytest.MonkeyPatch) -> EurostatClient:
    """Eurostat client whose Comext session answers every call with the artefact.

    The real ``APIClient`` is kept, so the HTTP counters read by the
    orchestrator are the genuine ones.
    """
    client = EurostatClient(auto_load_rate_limit=False)
    comext = client._get_comext_client()
    response = SimpleNamespace(
        status_code=200,
        content=_COMEXT_DATAFLOW_XML.encode(),
        text=_COMEXT_DATAFLOW_XML,
        raise_for_status=lambda: None,
    )
    monkeypatch.setattr(comext.session, "get", MagicMock(return_value=response))
    return client


def test_n_eurostat_queries_cost_one_last_update_call_per_run(
    tmp_path: Path, conn, writes, monkeypatch: pytest.MonkeyPatch
) -> None:
    import statflows.core.client as client_module

    clock = _TickingClock()
    monkeypatch.setattr(download_module, "_now", clock)
    monkeypatch.setattr(client_module, "_utc_now", clock)
    client = _comext_client(monkeypatch)
    queries = [
        EurostatQueryRequestV30("DS-045409", dimensions={"product": f"{p:02d}"})
        for p in range(50)
    ]
    # Toutes les requêtes déjà téléchargées après la dernière mise à jour
    _seed_registry(
        tmp_path, {q.identity_key(): "2025-12-15T00:00:00+00:00" for q in queries}
    )
    downloader = _downloader(tmp_path, client)

    report = downloader.run(queries)

    aggregates = report.aggregates()
    assert report.empty == 50 and writes.calls == []
    assert aggregates["n_http_requests"] == 1.0
    assert aggregates["n_freshness_checks"] == 1.0
    assert aggregates["n_freshness_cache_hits"] == 49.0
    assert aggregates["n_freshness_unresolved"] == 0.0
    assert [q.freshness.n_checks for q in report.queries] == [1] + [0] * 49
    assert report.to_metrics()["download.n_freshness_cache_hits"] == 49.0

    # Requêtes sautées sur la réponse en cache : date enregistrée ramenée à
    # l'instant de la vérification, antérieur à leur propre début
    checked_at = client._freshness_checked_at[("DS-045409", "+")]
    registry = _read_registry(tmp_path)
    recorded = [
        datetime.fromisoformat(registry[q.identity_key()]["last_download"])
        for q in queries
    ]
    assert recorded[0] <= checked_at
    assert recorded[1:] == [checked_at] * 49

    # Nouveau run sur le même client : le cache est vidé, une seule vérification
    second = _downloader(tmp_path, client).run(queries)
    assert second.aggregates()["n_freshness_checks"] == 1.0
    assert second.aggregates()["n_http_requests"] == 1.0


def test_horizon_is_ignored_when_later_than_the_query_start(
    tmp_path: Path, conn, writes, clock
) -> None:
    # Client sans vérification préalable exposant un horizon postérieur
    client = FakeClient(clock, data_every=2)
    client.last_fetch_as_of_ = T0 + timedelta(days=1)  # type: ignore[attr-defined]
    queries = make_queries(1)

    _downloader(tmp_path, client).run(queries)

    entry = _read_registry(tmp_path)[queries[0].identity_key()]
    assert entry["last_download"] == _at(0)


def test_callback_time_is_accumulated_in_the_report(
    tmp_path: Path, conn, writes, clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloader = _downloader(
        tmp_path, FakeClient(clock), on_query_complete=lambda report: None
    )
    # Horloge monotone simulée : deux rappels de 2,5 s puis 1 s
    readings = iter([0.0, 2.5, 10.0, 11.0])
    monkeypatch.setattr(download_module.time, "monotonic", lambda: next(readings))

    report = DownloadReport()
    downloader._publish(report, download_module.QueryReport())
    downloader._publish(report, download_module.QueryReport())

    assert report.callback_seconds == pytest.approx(3.5)
    assert len(report.queries) == 2
