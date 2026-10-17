"""Provider-agnostic helpers for parsing tabular API responses.

Dimension codes must survive parsing verbatim: pandas' default missing-value
markers (``"NA"``, ``"N/A"``, ``"NULL"``, ``"nan"``…) collide with real codes —
``"NA"`` is the ISO code of Namibia — and numeric inference strips the leading
zeros of product codes such as ``"01012100"``. These helpers read every column
as text, restrict the missing-value markers to those that mean "no
observation", and convert only the measure columns back to numbers.
"""

# Importation des modules
from collections.abc import Iterable, Sequence
from io import StringIO
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .structures import DataflowStructure

# Seuls marqueurs légitimes d'une valeur manquante : champ vide, et « : »
# employé par Eurostat pour une observation non disponible. Aucun code de
# dimension ne prend ces valeurs, contrairement à « NA » (Namibie).
MISSING_VALUE_MARKERS: tuple[str, ...] = ("", ":")

# Colonnes de mesure, comparées sans tenir compte de la casse
MEASURE_COLUMNS: frozenset[str] = frozenset({"obs_value", "obsvalue", "value"})

# Dimension temporelle, toujours partie de la clé d'une observation
TIME_COLUMN = "TIME_PERIOD"


# Fonction de lecture d'un CSV dont les colonnes portent des codes de dimension
def read_codes_csv(text: str, sep: str = ",") -> pd.DataFrame:
    """Read a delimited API response, keeping dimension codes verbatim.

    Every column is read as text with pandas' default missing-value markers
    disabled: only an empty field or ``":"`` is treated as missing. The
    measure columns (``OBS_VALUE``, ``value``) are then converted to numbers
    (``errors="coerce"``), and ``TIME_PERIOD`` is converted to numbers only
    when every period is numeric (annual periods such as ``2020``), as the
    default pandas inference did.

    Args:
        text: Raw delimited text.
        sep: Field separator.

    Returns:
        Parsed DataFrame; non-measure columns have the pandas string dtype.

    Raises:
        pandas.errors.EmptyDataError: If ``text`` holds no column.

    Examples:
        >>> df = read_codes_csv("partner,product,OBS_VALUE\\nNA,01012100,:\\n")
        >>> df.loc[0, "partner"], df.loc[0, "product"]
        ('NA', '01012100')
        >>> bool(df["OBS_VALUE"].isna().all())
        True
    """
    # Lecture en texte, sans les marqueurs de manquant par défaut de pandas
    df = pd.read_csv(
        StringIO(text),
        sep=sep,
        dtype=str,
        keep_default_na=False,
        na_values=list(MISSING_VALUE_MARKERS),
    )
    # Parcours des colonnes
    for column in df.columns:
        # Mesures : seule conversion numérique imposée
        if str(column).lower() in MEASURE_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        # Périodes annuelles : type numérique conservé, comme l'inférence de pandas
        elif column == TIME_COLUMN:
            try:
                df[column] = pd.to_numeric(df[column])
            except (TypeError, ValueError):
                pass
    return df


# Fonction de détermination des colonnes de clé d'un jeu de données
def resolve_key_columns(
    structure: "DataflowStructure | None",
    df_columns: Sequence[str],
) -> list[str]:
    """Derive the columns identifying an observation.

    The key is the set of dataflow dimensions intersected with the columns
    actually present, matched case-insensitively (the DataFrame's casing is
    kept). Label dimensions (``attribute_of``) are left out: they may be
    null. ``TIME_PERIOD`` is always appended when present, even if it is not
    listed among the structure dimensions.

    Args:
        structure: Resolved dataflow structure (may be ``None``).
        df_columns: Columns of the DataFrame.

    Returns:
        Ordered, de-duplicated list of key column names.

    Examples:
        >>> resolve_key_columns(None, ["geo", "TIME_PERIOD", "OBS_VALUE"])
        ['TIME_PERIOD']
    """
    # Dimensions de la structure, hors libellés
    dimension_names = (
        [dim.name for dim in structure.dimensions if not dim.attribute_of]
        if structure is not None
        else []
    )
    # Index insensible à la casse : nom en minuscules → nom réel de la colonne
    cols_lower = {str(c).lower(): str(c) for c in df_columns}
    keys: list[str] = []
    for name in [*dimension_names, TIME_COLUMN]:
        actual = cols_lower.get(name.lower())
        if actual is not None and actual not in keys:
            keys.append(actual)
    return keys


# Fonction de vérification de l'absence de nul dans les colonnes de clé
def check_no_null_keys(
    df: pd.DataFrame,
    key_columns: Iterable[str],
    label: str | None = None,
) -> None:
    """Raise if a key column holds a null value.

    A null dimension code makes the observation unidentifiable: the DuckLake
    upsert would reject the whole write batch with a generic message, or the
    row would be stored with a wrong key. Failing here names the culprit.
    Null measures (missing observations) are not checked.

    Args:
        df: Parsed DataFrame.
        key_columns: Columns identifying an observation (dimensions and
            ``TIME_PERIOD``); columns absent from ``df`` are ignored.
        label: Optional context (e.g. dataflow) prefixed to the message.

    Raises:
        ValueError: If a key column contains a null value; the message names
            the column, the number of affected rows and the first of them.

    Examples:
        >>> check_no_null_keys(pd.DataFrame({"geo": ["FR"]}), ["geo"])
        >>> check_no_null_keys(pd.DataFrame({"geo": [None]}), ["geo"])  # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        ValueError: Key column 'geo' has 1 null value(s) ...
    """
    # Parcours des colonnes de clés
    for column in key_columns:
        # Cas où la colonne n'est pas dans le jeu de données
        if column not in df.columns:
            continue
        # Comptage des valeurs manquantes
        nulls = df[column].isna()
        n_nulls = int(nulls.sum())
        if n_nulls:
            # Extrait des premières lignes fautives pour le diagnostic
            examples = df.loc[nulls].head(5).to_dict("records")
            prefix = f"{label}: " if label else ""
            raise ValueError(
                f"{prefix}Key column '{column}' has {n_nulls} null value(s) "
                f"out of {len(df)} row(s); first rows: {examples}"
            )
