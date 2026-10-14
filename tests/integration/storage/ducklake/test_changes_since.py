"""Integration test — ``changes_since`` and ``write_dataframe_with_report``.

Frozen behaviour: the operation report of a write tells an effective write from
an upsert of unchanged values (which still commits a snapshot); ``changes_since``
counts the inserted, updated and deleted rows since a watermark snapshot, reports
the smallest ``TIME_PERIOD`` touched, ignores metadata-only snapshots, and fails
explicitly on a watermark ahead of the catalog or whose snapshots were expired.

Requires the "ducklake" extra: the ``ducklake_conn`` fixture skips otherwise.
"""

from __future__ import annotations

import pandas as pd
import pytest

from statflows.storage.ducklake.tables import (
    FACT_TABLE,
    changes_since,
    write_dataframe_with_report,
)

# Clé primaire des données de test
PRIMARY_KEYS = ["geo", "TIME_PERIOD"]


def _frame(values: dict[tuple[str, str], float]) -> pd.DataFrame:
    """Observations ``(geo, TIME_PERIOD) → OBS_VALUE``."""
    return pd.DataFrame(
        [
            {"geo": geo, "TIME_PERIOD": period, "OBS_VALUE": value}
            for (geo, period), value in values.items()
        ]
    )


def _write(conn, alias: str, values: dict[tuple[str, str], float]):
    return write_dataframe_with_report(
        conn, _frame(values), PRIMARY_KEYS, catalog_alias=alias, schema="s1"
    )


def _current(conn, alias: str) -> int:
    return conn.execute(
        f"SELECT * FROM ducklake_current_snapshot('{alias}')"
    ).fetchone()[0]


BASE = {("FR", "2020"): 1.0, ("FR", "2021"): 2.0, ("DE", "2022"): 3.0}


def test_report_tells_effective_writes_from_unchanged_ones(ducklake_conn) -> None:
    conn, alias = ducklake_conn

    created = _write(conn, alias, BASE)
    unchanged = _write(conn, alias, BASE)
    revised = _write(conn, alias, {**BASE, ("FR", "2021"): 2.5})

    assert created.created is True
    assert created.rows_changed == 3
    # Upsert de valeurs identiques : snapshot créé, aucune ligne écrite
    assert unchanged.created is False
    assert unchanged.rows_changed == 0
    assert unchanged.report.snapshot_after > unchanged.report.snapshot_before
    assert revised.rows_changed == 1


def test_changes_since_counts_rows_and_earliest_period(ducklake_conn) -> None:
    conn, alias = ducklake_conn
    _write(conn, alias, BASE)
    watermark = _current(conn, alias)

    # Upsert inchangé (snapshot de simple horodatage) : aucun changement
    _write(conn, alias, BASE)
    assert not changes_since(conn, alias, "s1", watermark).modified

    # Révision de 2021, ajout de 2023, suppression de DE 2022
    _write(conn, alias, {**BASE, ("FR", "2021"): 2.5, ("FR", "2023"): 4.0})
    conn.execute(f"DELETE FROM {alias}.s1.{FACT_TABLE} WHERE geo = 'DE'")
    changes = changes_since(conn, alias, "s1", watermark)

    assert changes.since_snapshot == watermark
    assert changes.current_snapshot == _current(conn, alias)
    assert (changes.rows_inserted, changes.rows_updated, changes.rows_deleted) == (
        1,
        1,
        1,
    )
    assert changes.min_time_period == "2021"
    # Point de reprise à jour : plus rien à recalculer
    assert not changes_since(conn, alias, "s1", changes.current_snapshot).modified


def test_changes_since_none_counts_current_rows(ducklake_conn) -> None:
    conn, alias = ducklake_conn

    # Table absente : aucun changement
    assert not changes_since(conn, alias, "s1", None).modified

    _write(conn, alias, BASE)
    changes = changes_since(conn, alias, "s1", None)

    assert changes.rows_inserted == 3
    assert changes.min_time_period == "2020"
    assert changes.current_snapshot == _current(conn, alias)


def test_changes_since_rejects_unknown_watermarks(ducklake_conn) -> None:
    conn, alias = ducklake_conn
    _write(conn, alias, BASE)
    old = _current(conn, alias) - 1
    _write(conn, alias, {**BASE, ("FR", "2020"): 9.0})
    _write(conn, alias, {**BASE, ("FR", "2020"): 8.0})
    current = _current(conn, alias)

    # Point de reprise en avance sur le catalogue
    with pytest.raises(ValueError, match="ahead"):
        changes_since(conn, alias, "s1", current + 1)

    # Snapshots expirés : changements inconnus, recalcul complet requis
    conn.execute(
        f"CALL ducklake_expire_snapshots('{alias}', versions => [{old + 1}, {old + 2}])"
    )
    with pytest.raises(ValueError, match="no longer available"):
        changes_since(conn, alias, "s1", old)
