"""Tests d'intégration — orchestrateur ``SDMXDownloader`` (PS-27, constat C-08).

Bout-en-bout sans réseau : faux client (:mod:`tests.utils.fake_sdmx`), 2 000
fausses requêtes, catalogue DuckLake sur fichier local (vrai
``DuckLakeConnector``) et registres JSON sur un bucket S3 simulé (``moto``).

Le test a) est un test de **caractérisation** : écrit et vérifié sur le code
antérieur au tamponnage, il fige le contenu final (tables et registre) du mode
par défaut, qui doit rester identique.

Volumétrie : une requête sur ``DATA_EVERY`` renvoie des données, les autres un
DataFrame vide. En mode par défaut chaque requête non vide coûte un upsert
DuckLake complet (transaction, audit, compaction) : la proportion est réduite
pour garder la suite rapide, sans réduire le nombre de requêtes (2 000).
"""

from __future__ import annotations

import logging
import signal
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("duckdb", reason="requiert l'extra « ducklake » (duckdb absent)")
pytest.importorskip(
    "dt_ducklake_manager",
    reason="requiert l'extra « ducklake » (dt_ducklake_manager absent)",
)

from dt_ducklake_manager import DuckLakeConnector  # noqa: E402

import statflows.core.download as download_module  # noqa: E402
from statflows import iter_registry_entries  # noqa: E402
from statflows.core.download import SDMXDownloader  # noqa: E402
from statflows.storage.json import Loader, Saver  # noqa: E402
from tests.utils.fake_sdmx import (  # noqa: E402
    DATAFLOWS,
    T0,
    FakeClient,
    FakeClock,
    expected_table,
    make_queries,
)

# Nombre de fausses requêtes et proportion de requêtes non vides
N_QUERIES = 2000
DATA_EVERY = 20
# Clés des registres sur le bucket simulé
REGISTRY_KEY = "registries/fake_last_download.json"
STRUCTURES_KEY = "registries/fake_structures.json"


# ──────────────────────────────────────────────────────────────────────
# Fixtures et helpers
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Horloge factice substituée à ``statflows.core.download._now``."""
    fake = FakeClock()
    monkeypatch.setattr(download_module, "_now", fake)
    return fake


@pytest.fixture
def lake(tmp_path: Path) -> dict[str, Path]:
    """Chemins d'un catalogue DuckLake fichier vierge."""
    data_path = tmp_path / "data"
    data_path.mkdir()
    return {"catalog": tmp_path / "catalog.ducklake", "data": data_path}


def make_connector(lake: dict[str, Path]) -> DuckLakeConnector:
    """Connecteur DuckLake sur le catalogue fichier du test."""
    return DuckLakeConnector(
        str(lake["catalog"]),
        str(lake["data"]),
        log_filename=str(lake["data"].parent / "dl.log"),
    )


def make_downloader(
    client: FakeClient, lake: dict[str, Path], bucket: str, **kwargs: Any
) -> SDMXDownloader:
    """Orchestrateur câblé sur le faux client, le catalogue et le bucket."""
    return SDMXDownloader(
        client,
        make_connector(lake),
        STRUCTURES_KEY,
        REGISTRY_KEY,
        bucket=bucket,
        max_runtime=None,
        **kwargs,
    )


def read_table(lake: dict[str, Path], dataflow: str) -> pd.DataFrame:
    """Contenu de la table de faits d'un dataflow, trié par clé."""
    conn = make_connector(lake).connect()
    try:
        df = conn.execute(
            "SELECT REF_AREA, PRODUCT, TIME_PERIOD, OBS_VALUE "
            f"FROM db.{dataflow}.fact_table"
        ).df()
    finally:
        conn.close()
    return df.sort_values(["REF_AREA", "PRODUCT", "TIME_PERIOD"]).reset_index(drop=True)


def count_snapshots(lake: dict[str, Path]) -> int:
    """Nombre de snapshots du catalogue."""
    conn = make_connector(lake).connect()
    try:
        return conn.execute("SELECT count(*) FROM db.snapshots()").fetchone()[0]
    finally:
        conn.close()


def read_registry(bucket: str) -> dict[str, Any]:
    """Entrées du registre des dates sous la racine ``DOWNLOADS``."""
    return (Loader().load(REGISTRY_KEY, bucket=bucket, missing_ok=True) or {}).get(
        "DOWNLOADS", {}
    )


def expected_registry(queries: list[Any]) -> dict[str, Any]:
    """Registre attendu : la requête de rang ``i`` est datée ``T0 + i minutes``."""
    return {
        q.identity_key(): {
            "agency": q.agency,
            "dataflow": q.dataflow,
            "params": q.to_dict(),
            "last_download": (T0 + timedelta(minutes=position)).isoformat(),
        }
        for position, q in enumerate(queries)
    }


def assert_final_content(
    lake: dict[str, Path], bucket: str, queries: list[Any]
) -> None:
    """Contenu final attendu : tables complètes et registre à jour."""
    for dataflow in DATAFLOWS:
        pd.testing.assert_frame_equal(
            read_table(lake, dataflow),
            expected_table(queries, dataflow, DATA_EVERY),
            check_dtype=False,
        )
    assert read_registry(bucket) == expected_registry(queries)


# ──────────────────────────────────────────────────────────────────────
# a) Caractérisation du mode par défaut
# ──────────────────────────────────────────────────────────────────────


def test_default_mode_final_content(s3_bucket: str, lake, clock) -> None:
    queries = make_queries(N_QUERIES)
    client = FakeClient(clock, data_every=DATA_EVERY)
    downloader = make_downloader(client, lake, s3_bucket)

    report = downloader.run(queries)

    assert report.processed == N_QUERIES
    assert report.errors == 0
    assert report.empty == N_QUERIES - N_QUERIES // DATA_EVERY
    assert report.rows_written == (N_QUERIES // DATA_EVERY) * 4
    assert_final_content(lake, s3_bucket, queries)


# ──────────────────────────────────────────────────────────────────────
# Espions et helpers des modes tamponnés
# ──────────────────────────────────────────────────────────────────────

# Date ancienne d'un registre pré-rempli
OLD_DATE = "2020-01-01T00:00:00+00:00"


def spy_saves(downloader: SDMXDownloader) -> list[str]:
    """Espionne ``downloader._saver.save`` ; renvoie la liste (vivante) des chemins écrits."""
    saved: list[str] = []
    original = downloader._saver.save

    def spy(filepath, *args, **kwargs):
        saved.append(Path(filepath).as_posix())
        return original(filepath, *args, **kwargs)

    downloader._saver.save = spy  # type: ignore[method-assign]
    return saved


def registry_saves(saved: list[str]) -> list[str]:
    """Écritures du registre des dates (fichier unique ou fragments)."""
    stem = REGISTRY_KEY[: -len(".json")]
    return [p for p in saved if p == REGISTRY_KEY or p.startswith(stem + "/")]


def seed_registry(
    bucket: str, queries: list[Any], last_download: str
) -> dict[str, Any]:
    """Pré-remplit le registre avec une même date ancienne pour toutes les requêtes."""
    entries = {
        q.identity_key(): {
            "agency": q.agency,
            "dataflow": q.dataflow,
            "params": q.to_dict(),
            "last_download": last_download,
        }
        for q in queries
    }
    Saver().save(REGISTRY_KEY, {"DOWNLOADS": entries}, bucket=bucket, indent=2)
    return entries


def non_empty_by_dataflow(queries: list[Any], stop: int) -> dict[str, int]:
    """Nombre de requêtes non vides par dataflow parmi les ``stop`` premières."""
    counts = {dataflow: 0 for dataflow in DATAFLOWS}
    for q in queries[:stop]:
        if q.index % DATA_EVERY == 0:
            counts[q.dataflow] += 1
    return counts


def assert_stopped_cleanly(
    lake: dict[str, Path], bucket: str, queries: list[Any], stop: int, report
) -> None:
    """Garanties d'un arrêt anticipé après ``stop`` requêtes traitées."""
    done = queries[:stop]
    assert report.stopped_early is True
    assert report.processed == stop
    assert report.n_queries_remaining == len(queries) - stop
    assert report.rows_pending_at_stop == 0
    assert report.errors == 0
    # Lot partiel écrit : toutes les requêtes non vides traitées sont en base
    for dataflow in DATAFLOWS:
        pd.testing.assert_frame_equal(
            read_table(lake, dataflow),
            expected_table(done, dataflow, DATA_EVERY),
            check_dtype=False,
        )
    # Entrées validées pour toutes les requêtes traitées, et elles seules
    assert read_registry(bucket) == expected_registry(done)


# ──────────────────────────────────────────────────────────────────────
# b) Mode tamponné : écritures du registre et snapshots bornés
# ──────────────────────────────────────────────────────────────────────


def test_buffered_mode_bounds_writes_and_matches_default(
    s3_bucket: str, lake, clock
) -> None:
    queries = make_queries(N_QUERIES)
    client = FakeClient(clock, data_every=DATA_EVERY)
    downloader = make_downloader(
        client, lake, s3_bucket, registry_flush_every=500, write_batch_queries=500
    )
    saved = spy_saves(downloader)

    report = downloader.run(queries)

    # ≤ 5 écritures du registre (2 000 validations / 500), toutes comptées
    assert len(registry_saves(saved)) <= 5
    assert report.n_registry_flushes == len(registry_saves(saved))
    # ≤ 5 snapshots créés (hors snapshot initial de création du catalogue)
    assert count_snapshots(lake) - 1 <= 5
    # Un lot par schéma (moins de 500 requêtes non vides par dataflow)
    assert report.n_write_batches == len(DATAFLOWS)
    assert report.rows_pending_at_stop == 0
    assert report.errors == 0
    # Contenu identique au mode par défaut
    assert_final_content(lake, s3_bucket, queries)
    # Diagnostics reflétés dans les métriques
    metrics = report.to_metrics()
    assert metrics["download.n_registry_flushes"] == report.n_registry_flushes
    assert metrics["download.n_write_batches"] == len(DATAFLOWS)
    assert metrics["download.rows_pending_at_stop"] == 0


# ──────────────────────────────────────────────────────────────────────
# c) Échec d'écriture d'un lot
# ──────────────────────────────────────────────────────────────────────


def test_failed_batch_does_not_advance_its_entries(
    s3_bucket: str, lake, clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    queries = make_queries(N_QUERIES)
    old = seed_registry(s3_bucket, queries, OLD_DATE)
    failed_reporters: list[str] = []
    real_write = download_module.write_dataframe
    calls = {"n": 0}

    def flaky_write(conn, data, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            failed_reporters.extend(sorted(set(data["REF_AREA"])))
            raise RuntimeError("simulated DuckLake failure")
        return real_write(conn, data, *args, **kwargs)

    monkeypatch.setattr(download_module, "write_dataframe", flaky_write)
    client = FakeClient(clock, data_every=DATA_EVERY)
    downloader = make_downloader(
        client, lake, s3_bucket, registry_flush_every=500, write_batch_queries=10
    )

    report = downloader.run(queries)

    # Lot en échec : 10 requêtes, aucune entrée avancée, erreur imputée à chacune
    assert len(failed_reporters) == 10
    assert report.errors == 10
    failed_keys = {q.identity_key() for q in queries if q.reporter in failed_reporters}
    failed_reports = [r for r in report.queries if r.error_type is not None]
    assert {r.identity_key for r in failed_reports} == failed_keys
    assert all(r.error_type == "RuntimeError" for r in failed_reports)
    registry = read_registry(s3_bucket)
    for key in failed_keys:
        assert registry[key] == old[key]
    # Toutes les autres entrées ont avancé ; le run est allé jusqu'au bout
    expected = expected_registry(queries)
    for key in set(expected) - failed_keys:
        assert registry[key] == expected[key]
    assert report.processed == N_QUERIES
    assert report.rows_pending_at_stop == 0


# ──────────────────────────────────────────────────────────────────────
# d) Arrêt sur deadline au milieu d'un lot
# ──────────────────────────────────────────────────────────────────────


def test_deadline_mid_batch_flushes_pending_batch(s3_bucket: str, lake, clock) -> None:
    queries = make_queries(N_QUERIES)
    stop = 1110
    # Précondition : au moins un lot partiel (non multiple de 10) à l'arrêt
    assert any(n % 10 for n in non_empty_by_dataflow(queries, stop).values())
    client = FakeClient(clock, data_every=DATA_EVERY)
    downloader = make_downloader(
        client, lake, s3_bucket, registry_flush_every=500, write_batch_queries=10
    )
    # L'horloge avance d'une minute par requête : arrêt après ``stop`` requêtes
    downloader._max_runtime = timedelta(minutes=stop)

    report = downloader.run(queries)

    assert_stopped_cleanly(lake, s3_bucket, queries, stop, report)


# ──────────────────────────────────────────────────────────────────────
# e) SIGTERM (appel direct du gestionnaire)
# ──────────────────────────────────────────────────────────────────────


def test_sigterm_during_fetch_flushes_pending_batch(
    s3_bucket: str, lake, clock
) -> None:
    queries = make_queries(N_QUERIES)
    stop = 1110
    assert any(n % 10 for n in non_empty_by_dataflow(queries, stop).values())
    original = signal.getsignal(signal.SIGTERM)
    seen: dict[str, Any] = {}
    holder: dict[str, SDMXDownloader] = {}

    def on_fetch(position: int, query) -> None:
        if position == 0:
            seen["handler"] = signal.getsignal(signal.SIGTERM)
        if position == stop:
            # Arrêt du pod simulé : le gestionnaire interrompt le fetch en cours
            holder["downloader"]._handle_sigterm(signal.SIGTERM, None)

    client = FakeClient(clock, data_every=DATA_EVERY, on_fetch=on_fetch)
    downloader = make_downloader(
        client, lake, s3_bucket, registry_flush_every=500, write_batch_queries=10
    )
    holder["downloader"] = downloader

    report = downloader.run(queries)

    # Gestionnaire installé pendant run(), restauré ensuite
    assert seen["handler"] == downloader._handle_sigterm
    assert signal.getsignal(signal.SIGTERM) == original
    # La requête interrompue n'est ni comptée ni enregistrée (retentée au run suivant)
    assert_stopped_cleanly(lake, s3_bucket, queries, stop, report)


def test_sigterm_outside_fetch_only_requests_stop(s3_bucket: str, lake, clock) -> None:
    queries = make_queries(200)
    holder: dict[str, SDMXDownloader] = {}
    published: list[str] = []

    def on_complete(query_report) -> None:
        published.append(query_report.identity_key)
        # Hors de la fenêtre de fetch : pas d'interruption, arrêt entre deux requêtes
        if len(published) == 50:
            holder["downloader"]._handle_sigterm(signal.SIGTERM, None)

    client = FakeClient(clock, data_every=DATA_EVERY)
    downloader = make_downloader(client, lake, s3_bucket, on_query_complete=on_complete)
    holder["downloader"] = downloader

    report = downloader.run(queries)

    assert report.stopped_early is True
    assert report.processed == 50
    assert report.errors == 0
    assert read_registry(s3_bucket) == expected_registry(queries[:50])


def test_no_signal_handler_outside_main_thread(
    s3_bucket: str, lake, clock, caplog: pytest.LogCaptureFixture
) -> None:
    queries = make_queries(20)
    original = signal.getsignal(signal.SIGTERM)
    seen: dict[str, Any] = {}

    def on_fetch(position: int, query) -> None:
        if position == 0:
            seen["handler"] = signal.getsignal(signal.SIGTERM)

    client = FakeClient(clock, data_every=DATA_EVERY, on_fetch=on_fetch)
    downloader = make_downloader(client, lake, s3_bucket)
    result: dict[str, Any] = {}

    with caplog.at_level(logging.WARNING, logger="statflows.core.download"):
        thread = threading.Thread(
            target=lambda: result.update(r=downloader.run(queries))
        )
        thread.start()
        thread.join()

    assert result["r"].processed == 20
    assert seen["handler"] == original
    assert "no SIGTERM handler installed" in caplog.text


# ──────────────────────────────────────────────────────────────────────
# f) Registre fragmenté : lecture, migration, réécriture ciblée
# ──────────────────────────────────────────────────────────────────────


def test_sharded_registry_migration_and_targeted_rewrites(
    s3_bucket: str, lake, clock
) -> None:
    queries = make_queries(N_QUERIES)
    stem = REGISTRY_KEY[: -len(".json")]
    buffered = {"registry_flush_every": 500, "write_batch_queries": 500}
    by_dataflow = {"registry_shard_key": lambda q: q.dataflow}

    # 1. Registre historique en fichier unique
    make_downloader(FakeClient(clock, DATA_EVERY), lake, s3_bucket, **buffered).run(
        queries
    )
    single = {
        e.identity_key: e for e in iter_registry_entries(REGISTRY_KEY, bucket=s3_bucket)
    }
    assert {k: e.to_raw() for k, e in single.items()} == expected_registry(queries)

    # 2. Migration : registre fragmenté par dataflow, run sur 100 requêtes DF_ALPHA
    alpha = [q for q in queries if q.dataflow == "DF_ALPHA"][:100]
    migrator = make_downloader(
        FakeClient(clock, DATA_EVERY), lake, s3_bucket, **buffered, **by_dataflow
    )
    saved = spy_saves(migrator)
    assert migrator.run(alpha).errors == 0

    fragments = Loader().list_json(stem, bucket=s3_bucket)
    assert fragments == [f"{stem}/DF_ALPHA.json", f"{stem}/_default.json"]
    assert set(registry_saves(saved)) == set(fragments)
    # Toutes les entrées ont migré dans les fragments ; seules celles du run ont avancé
    fragment_keys: set = set()
    for fragment in fragments:
        fragment_keys |= set(Loader().load(fragment, bucket=s3_bucket)["DOWNLOADS"])
    assert fragment_keys == set(single)
    migrated = {
        e.identity_key: e for e in iter_registry_entries(REGISTRY_KEY, bucket=s3_bucket)
    }
    assert set(migrated) == set(single)
    run_keys = {q.identity_key() for q in alpha}
    for key, entry in migrated.items():
        if key in run_keys:
            assert entry.last_download > single[key].last_download
        else:
            assert entry == single[key]
    # Le fichier historique est laissé intact (périmé, sans effet à la lecture)
    assert read_registry(s3_bucket) == expected_registry(queries)

    # 3. Run suivant sur DF_ALPHA : seul son fragment est réécrit
    rerun = make_downloader(
        FakeClient(clock, DATA_EVERY), lake, s3_bucket, **buffered, **by_dataflow
    )
    saved = spy_saves(rerun)
    rerun.run(alpha[:10])
    assert registry_saves(saved) == [f"{stem}/DF_ALPHA.json"]
