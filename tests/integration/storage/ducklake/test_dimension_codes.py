"""Integration test — ambiguous dimension codes through a real DuckLake write.

Frozen behaviour: a Comext-like response holding the partner ``"NA"``
(Namibia) is parsed, written and read back with ``partner == "NA"`` (never
null), product codes keep their leading zeros, and an identical second write
changes no row (idempotence).

Requires the "ducklake" extra: the ``ducklake_conn`` fixture skips otherwise.
"""

from __future__ import annotations

from statflows.core.client import AbstractSDMXClient
from statflows.core.parsing import resolve_key_columns
from statflows.core.structures import DataflowStructure, DimensionInfo
from statflows.storage.ducklake.tables import FACT_TABLE, write_dataframe_with_report

# Dimensions du jeu Comext DS-045409
DIMENSIONS = ["freq", "reporter", "partner", "product", "flow", "indicators"]
# Réponse SDMX-CSV avec la Namibie (NA), une observation manquante et un drapeau
CSV = (
    "DATAFLOW,LAST UPDATE,freq,reporter,partner,product,flow,indicators,"
    "TIME_PERIOD,OBS_VALUE,OBS_FLAG\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NA,01012100,1,VALUE_IN_EUROS,2020,7,\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NA,01012100,2,VALUE_IN_EUROS,2020,:,\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NO,01012100,1,VALUE_IN_EUROS,2020,8,c\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NL,01012100,1,VALUE_IN_EUROS,2020,9,\n"
)


def _write(conn, alias: str):
    df = AbstractSDMXClient._parse_csv_response(CSV)
    structure = DataflowStructure(
        "ESTAT",
        "DS-045409",
        len(DIMENSIONS),
        [DimensionInfo(name, i) for i, name in enumerate(DIMENSIONS)],
    )
    primary_keys = resolve_key_columns(structure, list(df.columns))
    return write_dataframe_with_report(
        conn, df, primary_keys, catalog_alias=alias, schema="s1"
    )


def test_namibia_round_trips_and_rewrite_is_idempotent(ducklake_conn) -> None:
    conn, alias = ducklake_conn

    created = _write(conn, alias)
    assert created.created is True

    rows = conn.execute(
        f"SELECT partner, product, flow, OBS_VALUE FROM {alias}.s1.{FACT_TABLE} "
        "ORDER BY partner, flow"
    ).fetchall()
    assert rows == [
        ("NA", "01012100", "1", 7),
        ("NA", "01012100", "2", None),
        ("NL", "01012100", "1", 9),
        ("NO", "01012100", "1", 8),
    ]
    n_null = conn.execute(
        f"SELECT count(*) FROM {alias}.s1.{FACT_TABLE} WHERE partner IS NULL"
    ).fetchone()[0]
    assert n_null == 0

    # Second passage identique : aucune ligne insérée ni modifiée
    rewritten = _write(conn, alias)
    assert rewritten.created is False
    assert rewritten.rows_changed == 0
