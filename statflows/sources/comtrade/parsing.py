"""UN Comtrade parsing helpers.

Pure functions converting Comtrade reference/availability responses and the
parameter declarations into the package's shared structures. Kept free of any
HTTP or client state so they can be unit-tested in isolation, mirroring
``eurostat.parsing`` and ``oecd.parsing``.
"""

# Importation des modules
# Modules de base
import json
import logging
from pathlib import Path
from typing import Any

# Modules externes
import pandas as pd

# Modules du package
from ...core.structures import DataflowStructure

# Initialisation du logger
logger = logging.getLogger(__name__)


# Chargement des paramètres
with open(
    Path(__file__).parents[2] / "parameters" / "comtrade.json", encoding="utf-8"
) as f:
    PARAMETERS: dict[str, Any] = json.load(f)


# Fonction de construction d'une structure de dataflow depuis les paramètres
def build_structure_from_parameters(
    agency: str,
    dataflow: str,
) -> DataflowStructure | None:
    """Build a :class:`DataflowStructure` for a Comtrade dataflow.

    UN Comtrade exposes no structure endpoint, so the dataflow dimensions are
    declared in the module-level :data:`PARAMETERS` (``parameters/comtrade.json``).
    A ``STRUCTURES`` entry matching the requested dataflow is returned as-is;
    otherwise the canonical tariffline entry is cloned and re-stamped with the
    requested ``dataflow`` (every ``typeCode_freqCode_clCode`` dataflow shares
    the same dimension layout).

    Args:
        agency: Maintaining agency (``"COMTRADE"``).
        dataflow: Logical dataflow identifier (e.g. ``"C_A_HS"``).

    Returns:
        A :class:`DataflowStructure` whose dimensions are the tariffline
        identifier columns.

    Raises:
        ValueError: If no ``STRUCTURES`` entry is declared at all.

    Examples:
        >>> structure = build_structure_from_parameters("COMTRADE", "C_A_HS")
        >>> structure.get_position("reporterISO")
        3
    """
    # Liste des structures
    structures: list[dict[str, Any]] = PARAMETERS.get("STRUCTURES", [])

    # Recherche d'une entrée STRUCTURES correspondant exactement au dataflow
    for entry in structures:
        if entry.get("agency") == agency and entry.get("dataflow") == dataflow:
            return DataflowStructure.from_dict(entry)

    return None


# Fonction d'extraction des codes valides d'un jeu de métadonnées
def extract_codes(df, category: str) -> list[Any]:
    """Extract the valid codes of a reference category from its metadata.

    Args:
        df: Metadata DataFrame returned by ``ComtradeClient.get_metadata``.
        category: Reference category. One of ``"flow"``, ``"reporter"``,
            ``"partner"`` or ``"cmd:HS"``.

    Returns:
        List of valid codes for the category (expired entries are dropped for
        reporters and partners).

    Raises:
        ValueError: If ``category`` is not supported.

    Examples:
        >>> extract_codes(flow_metadata, "flow")  # doctest: +SKIP
        ['M', 'X', ...]
    """
    # Flux et nomenclature : codes dans la colonne "id"
    if category in ("flow", "cmd:HS"):
        return df["id"].tolist()
    # Reporters : codes des pays non expirés
    if category == "reporter":
        return df.loc[df["entryExpiredDate"].isna(), "reporterCode"].tolist()
    # Partenaires : codes des pays non expirés
    if category == "partner":
        return df.loc[df["entryExpiredDate"].isna(), "PartnerCode"].tolist()
    # Catégorie non supportée
    raise ValueError(
        f"Unsupported category '{category}'. "
        "Expected one of 'flow', 'reporter', 'partner', 'cmd:HS'."
    )


# Colonnes portant le code et le libellé dans les métadonnées de référence
# (par défaut : « id » et « text », communs à toutes les catégories)
_CODE_COLUMNS = {"reporter": "reporterCode", "partner": "PartnerCode"}
_LABEL_COLUMNS = {"reporter": "reporterDesc", "partner": "PartnerDesc"}


# Fonction de mise en forme d'une codelist avec libellés
def build_codelist(
    df: pd.DataFrame, category: str, keep_metadata: bool = False
) -> pd.DataFrame:
    """Turn reference metadata into a ``(code, label[, parent])`` codelist.

    Only the valid codes of :func:`extract_codes` are kept (expired reporters
    and partners are dropped), in the order of the metadata.

    Args:
        df: Metadata DataFrame returned by ``ComtradeClient.get_metadata``.
        category: ``"flow"``, ``"reporter"``, ``"partner"`` or ``"cmd:HS"``.
        keep_metadata: Append the remaining metadata columns (ISO codes,
            ``isGroup``…) after the standard ones.

    Returns:
        DataFrame with ``code`` (text), ``label`` and, when the metadata
        carries one (``cmd:HS``), ``parent`` (text, ``None`` at the root).

    Raises:
        ValueError: If ``category`` is not supported.

    Examples:
        >>> meta = pd.DataFrame({"id": ["01", "0101"], "text": ["Animals", "Horses"],
        ...                      "parent": ["TOTAL", "01"]})
        >>> build_codelist(meta, "cmd:HS")["parent"].tolist()
        ['TOTAL', '01']
    """
    # Codes valides et colonnes de code / libellé de la catégorie
    valid = {str(code) for code in extract_codes(df, category)}
    code_column = _CODE_COLUMNS.get(category, "id")
    label_column: str | None = _LABEL_COLUMNS.get(category, "text")
    if label_column not in df.columns:
        label_column = "text" if "text" in df.columns else None
    rows = df[df[code_column].astype(str).isin(valid)].reset_index(drop=True)

    # Colonnes normalisées
    codelist = pd.DataFrame({"code": rows[code_column].astype(str)})
    codelist["label"] = rows[label_column] if label_column is not None else None
    if "parent" in rows.columns:
        codelist["parent"] = rows["parent"].map(
            lambda value: None if pd.isna(value) else str(value)
        )

    # Métadonnées restantes, sur demande
    if keep_metadata:
        extra = [c for c in rows.columns if c not in codelist.columns]
        codelist = pd.concat([codelist, rows[extra]], axis=1)
    return codelist


# Fonction d'extraction de la date de dernière publication d'une disponibilité
def parse_availability_last_released(
    availability,
) -> dict[str, str | None]:
    """Map each period to its ``lastReleased`` date from an availability frame.

    Reads the DataFrame returned by ``getDaTariffline`` and produces a
    ``{period: lastReleased}`` mapping used by the download script to decide
    whether a (reporter, period) couple must be refreshed. The availability
    has one row per (reporter, period) dataset, so each period is mapped to
    the most recent ``lastReleased`` across its reporters: a republication by
    any reporter is thus never masked by an older one.

    Args:
        availability: DataFrame returned by
            ``ComtradeClient.get_tariffline_data_availability`` (expects ``period``
            and ``lastReleased`` columns).

    Returns:
        Mapping of period (as ``str``) to its most recent last-released date
        (``str``, ``None`` when no reporter has a valid date). Empty when the
        input is empty or lacks the columns.

    Examples:
        >>> import pandas as pd
        >>> parse_availability_last_released(pd.DataFrame({
        ...     "period": [2022, 2022, 2021],
        ...     "lastReleased": ["2026-02-06T10:18:40.23", "2025-08-14T08:57:07.4733333", None],
        ... }))
        {'2022': '2026-02-06T10:18:40.23', '2021': None}
    """
    # Court-circuit si le jeu de données est vide ou incomplet
    if (
        availability is None
        or availability.empty
        or "period" not in availability.columns
        or "lastReleased" not in availability.columns
    ):
        return {}

    # Construction du dictionnaire période → date de publication la plus récente
    # (plusieurs reporters par période : comparaison sur les dates parsées, les
    # chaînes brutes n'ayant pas toutes la même précision)
    latest: dict[str, Any | None] = {}
    for period, released in zip(availability["period"], availability["lastReleased"]):
        period = str(period)
        parsed = pd.to_datetime(released, errors="coerce")
        # Date absente ou invalide : la période est conservée sans date
        if pd.isna(parsed):
            latest.setdefault(period, None)
            continue
        current = latest.get(period)
        if current is None or parsed > current[0]:
            latest[period] = (parsed, str(released))

    # Renvoi de la chaîne d'origine de la date la plus récente
    return {
        period: None if value is None else value[1] for period, value in latest.items()
    }
