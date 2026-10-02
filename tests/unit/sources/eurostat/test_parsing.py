"""Tests — :mod:`statflows.sources.eurostat.parsing`.

Frozen behaviour: transparent gzip handling, TSV / JSON-stat data parsing,
tolerant ISO date parsing, and SDMX-ML structure / catalogue / codelist parsing
in both the 3.0 and 2.1 namespaces.
"""

from __future__ import annotations

import gzip
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import pytest

from statflows.sources.eurostat.parsing import (
    _code_parent,
    _codelist_id_from_urn,
    _parse_iso_datetime,
    decompress_response_bytes,
    parse_codelist_response,
    parse_dataconstraint_last_update,
    parse_dataflow_list_response,
    parse_json_response,
    parse_structure_response,
    parse_tsv_response,
)

NS3 = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common",
}
NS21 = {k: v.replace("v3_0", "v2_1") for k, v in NS3.items()}


def _records(df) -> list[dict]:
    """Records with missing values as ``None`` (pandas stores them as NaN)."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def _message(ns: dict[str, str], body: str, prepared: str | None = None) -> str:
    header = f"<mes:Header><mes:Prepared>{prepared}</mes:Prepared></mes:Header>"
    return (
        f'<mes:Structure xmlns:mes="{ns["mes"]}" xmlns:str="{ns["str"]}" '
        f'xmlns:com="{ns["com"]}">'
        f"{header if prepared else ''}{body}</mes:Structure>"
    )


# ──────────────────────────────────────────────────────────────────────
# Décompression
# ──────────────────────────────────────────────────────────────────────


def test_decompress_gzip_content() -> None:
    assert decompress_response_bytes(gzip.compress(b"payload")) == b"payload"


@pytest.mark.parametrize("content", [b"plain text", b"", b"\x1f"])
def test_decompress_leaves_other_content_untouched(content) -> None:
    assert decompress_response_bytes(content) == content


# ──────────────────────────────────────────────────────────────────────
# TSV
# ──────────────────────────────────────────────────────────────────────


def test_tsv_is_melted_to_long_format_and_flags_dropped() -> None:
    text = (
        "freq,geo\\TIME_PERIOD\t2020\t2021\tcomment\n"
        "A,FR\t1.5 e\t2\tfoo\n"
        "A,DE\t:\t3 p\tbar\n"
    )
    df = parse_tsv_response(text)
    assert list(df.columns) == ["DIM_0", "DIM_1", "TIME_PERIOD", "value"]
    # La colonne "comment" ne porte pas de chiffre : elle est écartée
    assert len(df) == 4
    # Une valeur suivie d'un flag n'est pas numérique : elle devient NaN
    fr = df[df["DIM_1"] == "FR"].set_index("TIME_PERIOD")["value"]
    assert fr.isna()["2020"]
    assert fr["2021"] == 2.0


def test_tsv_invalid_input_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Failed to parse TSV response"):
        parse_tsv_response("")


# ──────────────────────────────────────────────────────────────────────
# JSON-stat
# ──────────────────────────────────────────────────────────────────────


def _jsonstat(value, geo_index=None) -> dict:
    return {
        "id": ["geo", "time"],
        "size": [2, 2],
        "dimension": {
            "geo": {"category": {"index": geo_index or {"FR": 0, "DE": 1}}},
            "time": {"category": {"index": ["2020", "2021"]}},
        },
        "value": value,
    }


def test_jsonstat_sparse_values() -> None:
    df = parse_json_response(_jsonstat({"0": 1.0, "3": 4.0}))
    assert df.to_dict("records") == [
        {"geo": "FR", "time": "2020", "value": 1.0},
        {"geo": "DE", "time": "2021", "value": 4.0},
    ]


def test_jsonstat_dense_values_skip_none() -> None:
    df = parse_json_response(_jsonstat([1.0, None, 3.0, None]))
    assert df["value"].tolist() == [1.0, 3.0]
    assert df["geo"].tolist() == ["FR", "DE"]


def test_jsonstat_category_index_out_of_range_gives_none_code() -> None:
    df = parse_json_response(_jsonstat({"0": 1.0, "2": 3.0}, {"FR": 0, "DE": 5}))
    assert _records(df)[1]["geo"] is None


def test_jsonstat_missing_category_index_gives_none_codes() -> None:
    data = _jsonstat({"0": 1.0})
    data["dimension"] = {}
    df = parse_json_response(data)
    assert df.iloc[0].tolist() == [None, None, 1.0]


@pytest.mark.parametrize(
    "data",
    [{}, {"id": ["a"], "size": []}, {"id": ["a", "b"], "size": [1]}],
)
def test_jsonstat_empty_or_malformed_returns_empty_frame(data) -> None:
    assert parse_json_response(data).empty


def test_jsonstat_unparsable_raises_value_error() -> None:
    data = _jsonstat({"0": 1.0})
    data["dimension"]["geo"]["category"]["index"] = {"FR": "x"}
    with pytest.raises(ValueError, match="Failed to parse JSON response"):
        parse_json_response(data)


# ──────────────────────────────────────────────────────────────────────
# Dates ISO
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2024-03-15", datetime(2024, 3, 15, tzinfo=UTC)),
        ("2024-03-15T10:00:00", datetime(2024, 3, 15, 10, tzinfo=UTC)),
        ("2024-03-15T10:00:00Z", datetime(2024, 3, 15, 10, tzinfo=UTC)),
        ("2024-03-15T12:00:00+02:00", datetime(2024, 3, 15, 10, tzinfo=UTC)),
        ("  updated on 2024-03-15 (data)  ", datetime(2024, 3, 15, tzinfo=UTC)),
    ],
)
def test_parse_iso_datetime_valid(text, expected) -> None:
    assert _parse_iso_datetime(text) == expected


@pytest.mark.parametrize("text", [None, "", "no date here", "2024-99-99"])
def test_parse_iso_datetime_invalid_returns_none(text) -> None:
    assert _parse_iso_datetime(text) is None


# ──────────────────────────────────────────────────────────────────────
# Dataconstraint
# ──────────────────────────────────────────────────────────────────────


def _annotation(ann_type: str | None, title=None, text=None) -> str:
    parts = []
    if ann_type is not None:
        parts.append(f"<com:AnnotationType>{ann_type}</com:AnnotationType>")
    if title is not None:
        parts.append(f"<com:AnnotationTitle>{title}</com:AnnotationTitle>")
    if text is not None:
        parts.append(f"<com:AnnotationText>{text}</com:AnnotationText>")
    return f"<com:Annotation>{''.join(parts)}</com:Annotation>"


def test_dataconstraint_prefers_data_update_over_other_updates() -> None:
    xml = _message(
        NS3,
        _annotation("UPDATE_STRUCTURE", "2024-01-01")
        + _annotation("UPDATE_DATA", "2024-02-02")
        + _annotation("OTHER", "2030-01-01"),
    )
    assert parse_dataconstraint_last_update(xml) == datetime(2024, 2, 2, tzinfo=UTC)


def test_dataconstraint_reads_date_from_text_and_sdmx21_namespace() -> None:
    xml = _message(NS21, _annotation("UPDATE_DATA", title="n/a", text="2024-05-06"))
    assert parse_dataconstraint_last_update(xml) == datetime(2024, 5, 6, tzinfo=UTC)


def test_dataconstraint_falls_back_to_prepared_header() -> None:
    xml = _message(
        NS3,
        _annotation("UPDATE_DATA", "no date") + _annotation(None),
        prepared="2024-07-08T09:00:00Z",
    )
    assert parse_dataconstraint_last_update(xml) == datetime(2024, 7, 8, 9, tzinfo=UTC)


def test_dataconstraint_prepared_fallback_in_sdmx21() -> None:
    xml = _message(NS21, "", prepared="2024-07-08")
    assert parse_dataconstraint_last_update(xml) == datetime(2024, 7, 8, tzinfo=UTC)


def test_dataconstraint_without_any_date_returns_none() -> None:
    assert parse_dataconstraint_last_update(_message(NS3, "", prepared="oops")) is None
    assert parse_dataconstraint_last_update(_message(NS3, "")) is None


def test_dataconstraint_invalid_xml_returns_none() -> None:
    assert parse_dataconstraint_last_update("<not xml") is None


# ──────────────────────────────────────────────────────────────────────
# Structure
# ──────────────────────────────────────────────────────────────────────

URN = "urn:sdmx:org.sdmx.infomodel.codelist.Codelist=ESTAT:CXT_NC(11.0)"


def test_codelist_id_from_urn() -> None:
    assert _codelist_id_from_urn(URN) == "CXT_NC"
    assert _codelist_id_from_urn("garbage") is None


@pytest.mark.parametrize("ns", [NS3, NS21], ids=["sdmx3", "sdmx21"])
def test_structure_dimensions_concepts_and_codelists(ns) -> None:
    body = f"""
    <str:Concept id="freq">
      <com:Name>Fréq</com:Name><com:Name>Frequency</com:Name>
    </str:Concept>
    <str:Concept id=""/>
    <str:DataStructure id="DSD">
      <str:DimensionList>
        <str:Dimension id="freq" position="0">
          <str:LocalRepresentation>
            <str:Enumeration>{URN}</str:Enumeration>
          </str:LocalRepresentation>
        </str:Dimension>
        <str:Dimension id="geo"/>
      </str:DimensionList>
    </str:DataStructure>"""
    structure = parse_structure_response(_message(ns, body), "NAMA_10")
    assert (structure.agency, structure.dataflow, structure.num_dimensions) == (
        "ESTAT",
        "NAMA_10",
        2,
    )
    freq, geo = structure.dimensions
    assert (freq.name, freq.position, freq.description, freq.codelist) == (
        "freq",
        0,
        "Frequency",
        "CXT_NC",
    )
    assert (geo.name, geo.position, geo.description, geo.codelist) == (
        "geo",
        1,
        None,
        None,
    )


def test_structure_without_dimension_list_is_empty() -> None:
    xml = _message(NS3, '<str:DataStructure id="DSD"/>')
    assert parse_structure_response(xml, "X").num_dimensions == 0


def test_structure_without_data_structure_raises() -> None:
    with pytest.raises(ValueError, match="DataStructure element not found"):
        parse_structure_response(_message(NS3, ""), "X")


def test_structure_invalid_xml_raises() -> None:
    with pytest.raises(ValueError, match="Failed to parse structure response"):
        parse_structure_response("<not xml", "X")


# ──────────────────────────────────────────────────────────────────────
# Catalogue de dataflows
# ──────────────────────────────────────────────────────────────────────

XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


@pytest.mark.parametrize("ns", [NS3, NS21], ids=["sdmx3", "sdmx21"])
def test_dataflow_list_prefers_english_name(ns) -> None:
    body = """
    <str:Dataflow id="A" agencyID="ESTAT" version="1.0">
      <com:Name xml:lang="fr">Alpha FR</com:Name>
      <com:Name xml:lang="en">Alpha</com:Name>
    </str:Dataflow>
    <str:Dataflow id="B" agencyID="ESTAT" version="2.0">
      <com:Name xml:lang="de">Beta</com:Name>
    </str:Dataflow>
    <str:Dataflow id="C" agencyID="ESTAT" version="3.0"/>"""
    df = parse_dataflow_list_response(_message(ns, body))
    assert _records(df) == [
        {"id": "A", "name": "Alpha", "version": "1.0", "agency": "ESTAT"},
        {"id": "B", "name": "Beta", "version": "2.0", "agency": "ESTAT"},
        {"id": "C", "name": None, "version": "3.0", "agency": "ESTAT"},
    ]


def test_dataflow_list_invalid_xml_raises() -> None:
    with pytest.raises(ValueError, match="Failed to parse dataflow catalogue"):
        parse_dataflow_list_response("<not xml")


# ──────────────────────────────────────────────────────────────────────
# Codelist
# ──────────────────────────────────────────────────────────────────────


def test_codelist_sdmx3_with_parent() -> None:
    body = """
    <str:Codelist id="CL">
      <str:Code id="EU"><com:Name xml:lang="en">Europe</com:Name></str:Code>
      <str:Code id="FR">
        <com:Name xml:lang="en">France</com:Name><str:Parent>EU</str:Parent>
      </str:Code>
    </str:Codelist>"""
    df = parse_codelist_response(_message(NS3, body), with_parent=True)
    assert _records(df) == [
        {"code": "EU", "name": "Europe", "parent": None},
        {"code": "FR", "name": "France", "parent": "EU"},
    ]


def test_codelist_sdmx21_parent_reference_and_no_parent_column_by_default() -> None:
    body = """
    <str:Codelist id="CL">
      <str:Code id="FR">
        <com:Name xml:lang="fr">France</com:Name>
        <str:Parent><Ref id="EU"/></str:Parent>
      </str:Code>
      <str:Code id="XX"><str:Parent><Ref/></str:Parent></str:Code>
    </str:Codelist>"""
    xml = _message(NS21, body)
    with_parent = parse_codelist_response(xml, with_parent=True)
    assert [r["parent"] for r in _records(with_parent)] == ["EU", None]
    assert list(parse_codelist_response(xml).columns) == ["code", "name"]


def test_codelist_empty_keeps_columns() -> None:
    df = parse_codelist_response(_message(NS3, ""), with_parent=True)
    assert df.empty
    assert list(df.columns) == ["code", "name", "parent"]


def test_codelist_invalid_xml_raises() -> None:
    with pytest.raises(ValueError, match="Failed to parse codelist response"):
        parse_codelist_response("<not xml")


def test_code_parent_without_parent_element() -> None:
    assert _code_parent(ET.fromstring("<Code/>"), NS3) is None
