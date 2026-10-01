"""Unit tests — codelists with labels (Eurostat, Comtrade, ``codelist_frame``).

Frozen behaviour: ``code`` / ``label`` / ``parent`` columns, resolution of a
dimension's codelist through the structure, and **no second network call** when
the codelist is cached — including when Comtrade has already downloaded it to
build the queries. No network: HTTP calls are mocked.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from statflows import ComtradeClient, EurostatClient
from statflows.core.factory import codelist_frame
from statflows.core.structures import DataflowStructure, DimensionInfo
from statflows.sources.eurostat.parsing import parse_codelist_response

# Codelist hiérarchique SDMX-ML 3.0 (parent en texte)
XML_V30 = """<?xml version="1.0" encoding="UTF-8"?>
<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common">
 <mes:Structures><str:Codelists><str:Codelist id="CL_PRODUCT">
  <str:Code id="01"><com:Name xml:lang="fr">Animaux</com:Name><com:Name xml:lang="en">Animals</com:Name></str:Code>
  <str:Code id="0101"><com:Name xml:lang="en">Horses</com:Name><str:Parent>01</str:Parent></str:Code>
 </str:Codelist></str:Codelists></mes:Structures>
</mes:Structure>"""

# Même codelist en SDMX-ML 2.1 (parent en élément Ref)
XML_V21 = """<?xml version="1.0" encoding="UTF-8"?>
<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
 <mes:Structures><str:Codelists><str:Codelist id="CL_PRODUCT">
  <str:Code id="01"><com:Name xml:lang="en">Animals</com:Name></str:Code>
  <str:Code id="0101"><com:Name xml:lang="en">Horses</com:Name><str:Parent><Ref id="01"/></str:Parent></str:Code>
 </str:Codelist></str:Codelists></mes:Structures>
</mes:Structure>"""

EXPECTED = pd.DataFrame(
    {"code": ["01", "0101"], "label": ["Animals", "Horses"], "parent": [None, "01"]}
)


# ──────────────────────────────────────────────────────────────────────
# Eurostat
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("xml", [XML_V30, XML_V21], ids=["sdmx30", "sdmx21"])
def test_parse_codelist_with_parent(xml: str) -> None:
    df = parse_codelist_response(xml, with_parent=True)

    pd.testing.assert_frame_equal(df.rename(columns={"name": "label"}), EXPECTED)


def test_parse_codelist_default_columns_unchanged() -> None:
    assert parse_codelist_response(XML_V30).columns.tolist() == ["code", "name"]


def test_eurostat_codelist_is_cached() -> None:
    client = EurostatClient(auto_load_rate_limit=False)
    with mock.patch.object(client, "get_structure", return_value=XML_V30) as get:
        first = codelist_frame(client, "CL_PRODUCT")
        first.loc[0, "label"] = "mutated"
        second = client.get_codelist("CL_PRODUCT")
        refreshed = client.get_codelist("CL_PRODUCT", refresh=True)

    # Un seul appel réseau hors rafraîchissement explicite ; cache non altéré
    assert get.call_count == 2
    pd.testing.assert_frame_equal(second, EXPECTED)
    pd.testing.assert_frame_equal(refreshed, EXPECTED)


def test_codelist_frame_resolves_dimension_codelist() -> None:
    client = EurostatClient(auto_load_rate_limit=False)
    structure = DataflowStructure(
        agency="ESTAT",
        dataflow="DS-045409",
        num_dimensions=2,
        dimensions=[
            DimensionInfo("reporter", 0, codelist="CXT_FREE_ISO"),
            DimensionInfo("product", 1, codelist="CL_PRODUCT"),
        ],
    )
    with mock.patch.object(client, "get_structure", return_value=XML_V30) as get:
        df = codelist_frame(client, "PRODUCT", structure)

    assert get.call_args.args[1] == "CL_PRODUCT"
    pd.testing.assert_frame_equal(df, EXPECTED)
    with pytest.raises(KeyError):
        codelist_frame(client, "partner", structure)


def test_codelist_frame_rejects_dimension_without_codelist() -> None:
    structure = DataflowStructure("A", "D", 1, [DimensionInfo("freq", 0)])

    with pytest.raises(ValueError, match="declares no codelist"):
        codelist_frame(EurostatClient(auto_load_rate_limit=False), "freq", structure)


def test_codelist_frame_requires_get_codelist() -> None:
    with pytest.raises(TypeError, match="get_codelist"):
        codelist_frame(object(), "CL_GEO")


# ──────────────────────────────────────────────────────────────────────
# Comtrade
# ──────────────────────────────────────────────────────────────────────

_INDEX = {
    "results": [
        {"category": "reporter", "fileuri": "https://x/reporter.json"},
        {"category": "cmd:HS", "fileuri": "https://x/hs.json"},
    ]
}
_FILES = {
    "https://x/reporter.json": {
        "results": [
            {
                "id": 251,
                "text": "France",
                "reporterCode": 251,
                "reporterDesc": "France",
                "reporterCodeIsoAlpha3": "FRA",
                "entryExpiredDate": None,
                "isGroup": False,
            },
            {
                "id": 280,
                "text": "Fmr Fed. Rep. of Germany",
                "reporterCode": 280,
                "reporterDesc": "Fmr Fed. Rep. of Germany",
                "reporterCodeIsoAlpha3": "DEU",
                "entryExpiredDate": "1990-12-31",
                "isGroup": False,
            },
        ]
    },
    "https://x/hs.json": {
        "results": [
            {"id": "TOTAL", "text": "All Commodities", "parent": None},
            {"id": "01", "text": "01 - Animals; live", "parent": "TOTAL"},
        ]
    },
}


def _fake_get_json(endpoint, *args, **kwargs):
    return _INDEX if endpoint == ComtradeClient.REFERENCES_URL else _FILES[endpoint]


def test_comtrade_codelists_share_the_query_building_cache() -> None:
    client = ComtradeClient(auto_load_rate_limit=False)
    with mock.patch.object(client, "_get_json", side_effect=_fake_get_json) as get:
        # Construction des requêtes : énumération des déclarants
        assert client._extract_codes("reporter") == [251]
        reporters = codelist_frame(client, "reporter")
        products = codelist_frame(client, "cmd:HS")
        codelist_frame(client, "reporter")

    # Index + fichier déclarants + fichier produits : 3 appels, aucun doublon
    assert get.call_count == 3
    pd.testing.assert_frame_equal(
        reporters, pd.DataFrame({"code": ["251"], "label": ["France"]})
    )
    pd.testing.assert_frame_equal(
        products,
        pd.DataFrame(
            {
                "code": ["TOTAL", "01"],
                "label": ["All Commodities", "01 - Animals; live"],
                "parent": [None, "TOTAL"],
            }
        ),
    )


def test_comtrade_codelist_keep_metadata_and_dimension_resolution() -> None:
    client = ComtradeClient(auto_load_rate_limit=False)
    structure = client.resolve_query_structure(
        mock.Mock(agency="COMTRADE", dataflow="C_A_HS")
    )
    with mock.patch.object(client, "_get_json", side_effect=_fake_get_json):
        df = codelist_frame(client, "reporterISO", structure, keep_metadata=True)

    assert df.columns[:2].tolist() == ["code", "label"]
    assert df["reporterCodeIsoAlpha3"].tolist() == ["FRA"]
