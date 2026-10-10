"""Construction helpers for provider clients and query objects.

Shared factory helpers used by the download orchestration entry points (CLI
script, Kedro nodes, notebooks). Kept in :mod:`statflows.core` so the relative
imports of the provider clients and query DTOs resolve correctly and the logic
is reusable rather than duplicated in each script.

:class:`ClientFactory` describes *how* to build a client (target and options)
as a small picklable object, so that a client can be created in another
process, a pipeline node or from a YAML/JSON configuration;
:func:`build_client` is the one-shot shortcut for the built-in providers.
"""

# Importation des modules
# Modules de base
import importlib
import inspect
import logging
import re
from collections.abc import Iterable, Mapping, Sequence
from enum import Enum
from types import MappingProxyType, ModuleType, NoneType, UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Literal,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)
# Module de manipulation de données
import pandas as pd

from ..sources.eurostat.queries import EurostatQueryRequestV30
from ..sources.oecd.queries import OECDQueryRequest
from .queries import SDMXQueryRequest
from .structures import DataflowStructure

# Classes de clients importées au seul typage des surcharges de build_client
if TYPE_CHECKING:
    from ..sources.comtrade.client import ComtradeClient
    from ..sources.eurostat.client import EurostatClient
    from ..sources.oecd.client import OECDClient
    from ..sources.unsd.client import UNSDClient

# Initialisation du logger
logger = logging.getLogger(__name__)


# Table de correspondance provider → classe de requête par défaut
_QUERY_CLASSES: dict[str, type[SDMXQueryRequest]] = {
    "eurostat": EurostatQueryRequestV30,
    "oecd": OECDQueryRequest,
}

# Table de correspondance alias de fournisseur → chemin pointé de la classe client
# (import paresseux : aucun client n'est importé au chargement du module)
CLIENT_PATHS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "eurostat": "statflows.sources.eurostat.client.EurostatClient",
        "oecd": "statflows.sources.oecd.client.OECDClient",
        "comtrade": "statflows.sources.comtrade.client.ComtradeClient",
        "unsd": "statflows.sources.unsd.client.UNSDClient",
    }
)


# Fonction auxiliaire de lecture d'un attribut, éventuellement imbriqué, d'un module
def _get_attribute(module: ModuleType, attributes: Sequence[str], path: str) -> Any:
    """Walk a chain of attributes from a module.

    Args:
        module: Imported module.
        attributes: Attribute names, outermost first.
        path: Full dotted path, for the error message.

    Returns:
        The attribute found at the end of the chain.

    Raises:
        ImportError: If an attribute of the chain is missing.
    """
    # Parcours de la chaîne d'attributs
    obj: Any = module
    for attribute in attributes:
        # Extraction de l'attribut
        try:
            obj = getattr(obj, attribute)
        except AttributeError as exc:
            raise ImportError(
                f"Cannot import '{path}': module '{module.__name__}' has no "
                f"attribute '{'.'.join(attributes)}'"
            ) from exc
    return obj


# Fonction auxiliaire d'importation d'un objet à partir de son chemin pointé
def _import_object(path: str) -> Any:
    """Import an object from its dotted path.

    Two notations are accepted: ``"package.module.Name"``, where the longest
    importable prefix is taken as the module, and ``"package.module:Name"``,
    where the colon separates the module from the attribute path (which may
    itself be dotted, e.g. ``"package.module:Outer.Inner"``).

    Args:
        path: Dotted path of the object.

    Returns:
        The imported object.

    Raises:
        ValueError: If the path is malformed.
        ImportError: If the module cannot be found or lacks the attribute. An
            import error raised *inside* an existing module (e.g. a missing
            third-party dependency) propagates unchanged.

    Examples:
        >>> _import_object("collections.abc:Mapping").__name__
        'Mapping'
        >>> _import_object("collections.OrderedDict").__name__
        'OrderedDict'
    """
    # Séparation explicite du module et de l'attribut (notation "a.b:C")
    if ":" in path:
        # Extraction du module et de l'attribut
        module_name, _, attribute_path = path.partition(":")
        attributes = attribute_path.split(".")
        # Cas d'erreur
        if not module_name or ":" in attribute_path or not all(attributes):
            raise ValueError(
                f"Malformed dotted path '{path}': expected 'package.module:Name'"
            )
        # Importation du module
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            # Absence du module désigné (et non d'une dépendance qu'il importe)
            if exc.name and (module_name + ".").startswith(exc.name + "."):
                raise ImportError(
                    f"Cannot import '{path}': module '{exc.name}' not found"
                ) from exc
            raise
        return _get_attribute(module, attributes, path)

    # Contrôle de la forme du chemin (notation "a.b.C")
    parts = path.split(".")
    if len(parts) < 2 or not all(parts):
        raise ValueError(
            f"Malformed dotted path '{path}': expected 'package.module.Name'"
        )

    # Recherche du plus long préfixe importable comme module
    for index in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:index])
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            # Remontée d'un niveau seulement si c'est ce préfixe qui est absent
            if exc.name and (module_name + ".").startswith(exc.name + "."):
                continue
            raise
        return _get_attribute(module, parts[index:], path)
    raise ImportError(f"Cannot import '{path}': module '{parts[0]}' not found")


# Fonction auxiliaire d'extraction de la classe d'énumération d'une annotation
def _enum_of(annotation: Any) -> type[Enum] | None:
    """Return the enum class an annotation reduces to.

    Only ``E`` and ``E | None`` (or ``Optional[E]``) qualify: in a wider union
    such as ``str | E`` a plain string is a legitimate value and must not be
    coerced.

    Args:
        annotation: Resolved parameter annotation.

    Returns:
        The :class:`~enum.Enum` subclass, or ``None``.

    Examples:
        >>> from statflows.core.sdmx import SDMXVersion
        >>> _enum_of(SDMXVersion | None)
        <enum 'SDMXVersion'>
        >>> _enum_of(str | SDMXVersion) is None
        True
    """
    # Déballage des unions (X | None, Optional[X])
    if get_origin(annotation) in (Union, UnionType):
        members = [arg for arg in get_args(annotation) if arg is not NoneType]
        if len(members) != 1:
            return None
        annotation = members[0]

    # Restriction aux classes d'énumération
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation
    return None


# Fonction auxiliaire de coercition d'une chaîne en membre d'énumération
def _coerce_enum(value: Any, enum_cls: type[Enum], option: str, path: str) -> Any:
    """Coerce a string into a member of ``enum_cls``.

    The string is matched against the member values first, then against the
    member names, case-insensitively. Members, ``None`` and other non-string
    values are returned unchanged.

    Args:
        value: Option value.
        enum_cls: Enum expected by the parameter.
        option: Option name, for the error message.
        path: Dotted path of the target, for the error message.

    Returns:
        The matching member, or ``value`` unchanged.

    Raises:
        ValueError: If the string matches neither a value nor a name.

    Examples:
        >>> from statflows.core.sdmx import SDMXVersion
        >>> _coerce_enum("V2_1", SDMXVersion, "api_version", "pkg.Client")
        <SDMXVersion.V2_1: 'v2_1'>
    """
    # Valeurs non textuelles (membres, None, objets) laissées telles quelles
    if not isinstance(value, str) or isinstance(value, enum_cls):
        return value

    # Correspondance par valeur
    try:
        return enum_cls(value)
    except ValueError:
        pass

    # Correspondance par nom, insensible à la casse
    by_name = {member.name.lower(): member for member in enum_cls}
    member = by_name.get(value.lower())
    if member is not None:
        return member
    raise ValueError(
        f"Invalid value {value!r} for option '{option}' of '{path}': expected one "
        f"of the {enum_cls.__name__} values {[m.value for m in enum_cls]} "
        f"or names {[m.name for m in enum_cls]}"
    )


# Fonction auxiliaire de lecture des annotations résolues des paramètres d'une cible
def _parameter_hints(target: Any) -> dict[str, Any]:
    """Return the resolved parameter annotations of a callable.

    Args:
        target: Class (its ``__init__`` is inspected) or other callable.

    Returns:
        Mapping parameter name → annotation; empty when the annotations
        cannot be resolved (e.g. names imported only for type checking).
    """
    # Annotations du constructeur pour une classe, de l'appelable sinon
    source = getattr(target, "__init__") if isinstance(target, type) else target
    try:
        return get_type_hints(source)
    except Exception:
        # Annotations non résolubles : aucune coercition pour cette cible
        logger.debug(f"Unresolvable annotations for {target!r}: no enum coercion")
        return {}


# Classe de fabrique picklable de clients
class ClientFactory:
    """Picklable recipe building a client on demand.

    A factory names its *target* — a provider alias of ``CLIENT_PATHS``
    (``"eurostat"``, ``"oecd"``, ``"comtrade"``, ``"unsd"``) or the dotted path
    of any callable (``"package.module.Name"`` or ``"package.module:Name"``,
    e.g. a test double) — together with the keyword arguments to pass to it.
    Calling the factory returns a **new** client on every call; the factory
    itself holds no client, HTTP session or rate limiter.

    Everything that can be checked without building a client is checked at
    construction, so that a misconfiguration surfaces where the factory is
    declared rather than at the first call, often in another process:

    - the target is imported and must be callable;
    - options and credentials are bound to the target signature
      (:meth:`inspect.Signature.bind`): an unknown or missing argument
      raises :class:`TypeError`;
    - a string option whose parameter is annotated with an enum (``E`` or
      ``E | None``) is coerced into a member, by value first, then by
      case-insensitive name (``"v3"`` or ``"V3"`` → ``SDMXVersion.V3``),
      since a YAML/JSON configuration cannot express an enum.

    Checks that belong to the client itself (e.g. an API version the provider
    does not support) still happen when the factory is called.

    ``credentials`` (API keys, tokens…) are merged with ``options`` when the
    client is built but are never displayed nor logged: the ``repr`` only
    shows their count. They are not coerced.

    A factory pickles as its target string, options and credentials, and is
    validated again when unpickled. Its options must therefore be picklable
    themselves: an option holding a lock (e.g. a shared
    :class:`~statflows.core.rate_limiter.RateLimiter`) makes the factory
    unpicklable, and pickling fails with the usual error.

    Factories compare equal when they resolve to the same callable with equal
    options and credentials; being mutable, they are not hashable.

    Args:
        target: Provider alias, or dotted path of the callable building the
            client.
        options: Keyword arguments passed to the target, shown in the ``repr``.
        credentials: Secret keyword arguments passed to the target, hidden
            from the ``repr``.

    Attributes:
        target: The target as given.
        path: Dotted path of the target (resolved from the alias if needed).
        options: Options after enum coercion.

    Raises:
        ValueError: If the target is neither a known alias nor a dotted path,
            if an option and a credential share a key, or if a string option
            matches no member of its enum.
        ImportError: If the dotted path cannot be imported.
        TypeError: If the target is not callable or does not accept the
            given arguments.

    Examples:
        >>> factory = ClientFactory("eurostat", options={"api_version": "v2_1"})
        >>> factory
        ClientFactory('eurostat', options={'api_version': <SDMXVersion.V2_1: 'v2_1'>})
        >>> with factory() as client:
        ...     client.api_version
        <SDMXVersion.V2_1: 'v2_1'>
        >>> ClientFactory("comtrade", credentials={"subscription_key": "my-key"})
        ClientFactory('comtrade', options={}, credentials=<1 hidden>)
        >>> import pickle
        >>> pickle.loads(pickle.dumps(factory)) == factory
        True
        >>> ClientFactory("unsd", options={"timeot": 30})
        Traceback (most recent call last):
            ...
        TypeError: Invalid arguments for 'statflows.sources.unsd.client.UNSDClient': got an unexpected keyword argument 'timeot'
    """

    # Désactivation du hachage (fabrique mutable comparée par valeur)
    __hash__ = None  # type: ignore[assignment]

    # Initialisation
    def __init__(
        self,
        target: str,
        options: Mapping[str, Any] | None = None,
        credentials: Mapping[str, Any] | None = None,
    ) -> None:
        # Résolution de l'alias de fournisseur en chemin pointé
        if target in CLIENT_PATHS:
            path = CLIENT_PATHS[target]
        elif "." in target or ":" in target:
            path = target
        else:
            raise ValueError(
                f"Unknown client target '{target}': expected a provider alias "
                f"({', '.join(sorted(CLIENT_PATHS))}) or a dotted path "
                "'package.module.Name'"
            )

        # Importation de la cible et contrôle de son caractère appelable
        obj = _import_object(path)
        if not callable(obj):
            raise TypeError(
                f"Client target '{path}' is not callable: {type(obj).__name__}"
            )

        # Copie défensive des options et des credentials
        resolved_options = dict(options or {})
        resolved_credentials = dict(credentials or {})

        # Détection des clés fournies à la fois en option et en credential
        conflicts = sorted(resolved_options.keys() & resolved_credentials.keys())
        if conflicts:
            raise ValueError(f"Keys given both as options and credentials: {conflicts}")

        # Lecture de la signature de la cible (absente pour certains appelables natifs)
        try:
            signature: inspect.Signature | None = inspect.signature(obj)
        except (TypeError, ValueError):
            signature = None

        if signature is not None:
            # Liaison des arguments à la signature (option inconnue ou manquante)
            try:
                signature.bind(**resolved_options, **resolved_credentials)
            except TypeError as exc:
                raise TypeError(f"Invalid arguments for '{path}': {exc}") from exc

            # Coercition des options textuelles attendues sous forme d'Enum
            hints = _parameter_hints(obj)
            for name, value in resolved_options.items():
                parameter = signature.parameters.get(name)
                if parameter is None or parameter.kind is parameter.VAR_KEYWORD:
                    continue
                enum_cls = _enum_of(hints.get(name))
                if enum_cls is not None:
                    resolved_options[name] = _coerce_enum(value, enum_cls, name, path)

        # Mémorisation de l'état (chaînes et dictionnaires) et de la cible importée
        self.target = target
        self.path = path
        self.options = resolved_options
        self._credentials = resolved_credentials
        self._callable = obj

    # Méthode de construction d'un nouveau client
    def __call__(self) -> Any:
        """Build a new client.

        Returns:
            A new client instance on every call.
        """
        # Construction d'un nouveau client à chaque appel
        return self._callable(**self.options, **self._credentials)

    # Méthode de représentation (credentials masquées)
    def __repr__(self) -> str:
        # Affichage de la cible et des options, du seul nombre de credentials
        text = f"{type(self).__name__}({self.target!r}, options={self.options!r}"
        if self._credentials:
            text += f", credentials=<{len(self._credentials)} hidden>"
        return text + ")"

    # Méthode de comparaison (cible résolue, options et credentials)
    def __eq__(self, other: object) -> bool:
        # Comparaison limitée aux fabriques
        if not isinstance(other, ClientFactory):
            return NotImplemented
        return (
            self._callable is other._callable
            and self.options == other.options
            and self._credentials == other._credentials
        )

    # Méthode de sérialisation (pickle)
    def __reduce__(self) -> tuple[Any, ...]:
        # Sérialisation en chaînes et dictionnaires, revalidés au chargement
        return (type(self), (self.target, self.options, self._credentials))


# Surcharges de typage de build_client (type du client selon l'alias littéral)
@overload
def build_client(provider: Literal["eurostat"], **options: Any) -> "EurostatClient": ...
@overload
def build_client(provider: Literal["oecd"], **options: Any) -> "OECDClient": ...
@overload
def build_client(provider: Literal["comtrade"], **options: Any) -> "ComtradeClient": ...
@overload
def build_client(provider: Literal["unsd"], **options: Any) -> "UNSDClient": ...
@overload
def build_client(provider: str, **options: Any) -> Any: ...


# Fonction de construction du client provider
def build_client(provider: str, **options: Any) -> Any:
    """Instantiate the client of a provider.

    Shortcut for ``ClientFactory(provider, options)()`` restricted to the
    provider aliases. Use :class:`ClientFactory` to build a client from a
    dotted path, keep the recipe (e.g. to send it to another process) or pass
    credentials kept out of its ``repr``.

    Args:
        provider: ``"eurostat"``, ``"oecd"``, ``"comtrade"`` or ``"unsd"``.
        **options: Keyword arguments of the client constructor; a string is
            coerced into an enum member where the constructor expects an enum
            (e.g. ``api_version="v2_1"``).

    Returns:
        A new provider client instance (typed after the provider when it is
        given as a literal).

    Raises:
        ValueError: For an unknown provider or an invalid enum value.
        TypeError: If the client constructor does not accept an option.

    Examples:
        >>> client = build_client("unsd", timeout=30)
        >>> type(client).__name__
        'UNSDClient'
        >>> client.close()
    """
    # Restriction aux alias de fournisseurs connus
    if provider not in CLIENT_PATHS:
        raise ValueError(
            f"Unknown provider '{provider}'; known providers: "
            f"{', '.join(sorted(CLIENT_PATHS))}"
        )

    # Construction déléguée à la fabrique (validation et coercition)
    return ClientFactory(provider, options)()


# Fonction de construction des requêtes à partir d'une liste de spécifications
def build_queries(provider: str, specs: list[dict[str, Any]]) -> list[SDMXQueryRequest]:
    """Build provider query objects from a list of JSON specifications.

    Each specification is passed to the provider query DTO
    :meth:`~statflows.core.queries.SDMXQueryRequest.from_dict`,
    which ignores keys not matching a DTO field (e.g. a human-readable
    ``"description"``) and coerces a textual ``"format"`` value into the
    provider enum.

    Args:
        provider: ``"eurostat"`` or ``"oecd"``.
        specs: List of keyword-argument dicts for the provider query DTO.

    Returns:
        List of provider query objects.

    Raises:
        ValueError: For an unknown provider.
    """
    # Sélection de la classe de requête du provider
    try:
        query_cls = _QUERY_CLASSES[provider]
    except KeyError:
        raise ValueError(f"Unknown provider '{provider}'")

    # Construction déléguée à from_dict (filtrage + coercition du format)
    return [query_cls.from_dict(spec) for spec in specs]


# Fonction de filtrage d'une liste de codes par inclusion/exclusion
def filter_codes(
    available: Iterable[str],
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    include_regex: str | None = None,
    exclude_regex: str | None = None,
) -> list[str]:
    """Filter a codelist by include/exclude lists and regular expressions.

    Selects the codes to keep when iterating a split dimension (e.g. the
    Eurostat ``reporter`` or ``product`` dimensions) to build the cartesian
    product of queries. The four criteria are applied in order and each is
    skipped when ``None``:

    1. ``include`` (list): keep only codes present in this list.
    2. ``include_regex``: keep only codes fully matching this pattern.
    3. ``exclude`` (list): drop these codes.
    4. ``exclude_regex``: drop codes fully matching this pattern.

    Args:
        available: Codes available in the dimension codelist.
        include: Allow-list of codes; ``None`` keeps every available code.
        exclude: Deny-list of codes; ``None`` removes nothing.
        include_regex: Pattern a code must fully match to be kept (``re.fullmatch``).
        exclude_regex: Pattern that drops a code when fully matched.

    Returns:
        Sorted list of selected codes (deterministic, duplicates removed).

    Examples:
        >>> filter_codes(
        ...     ["00", "27", "2710", "TOTAL", "2710XX", "271012"],
        ...     include_regex=r"^(\\d{2}|\\d{4}|\\d{6}|\\d{8})$",
        ...     exclude=["00"],
        ... )
        ['27', '2710', '271012']
    """
    # Déduplication des codes disponibles
    selected = {str(code) for code in available}

    # Avertissement si des codes inclus sont absents de la codelist
    # (diagnostic d'une codelist qui a évolué côté provider)
    if include is not None:
        include_set = {str(code) for code in include}
        missing = include_set - selected
        if missing:
            logger.warning(
                f"{len(missing)} included code(s) absent from the codelist: "
                f"{sorted(missing)}"
            )
        # Restriction à la liste d'inclusion
        selected &= include_set

    # Restriction au motif d'inclusion
    if include_regex is not None:
        pattern = re.compile(include_regex)
        selected = {code for code in selected if pattern.fullmatch(code)}

    # Retrait de la liste d'exclusion
    if exclude is not None:
        selected -= {str(code) for code in exclude}

    # Retrait du motif d'exclusion
    if exclude_regex is not None:
        pattern = re.compile(exclude_regex)
        selected = {code for code in selected if not pattern.fullmatch(code)}

    # Retour trié déterministe
    return sorted(selected)


# Fonction de récupération d'une codelist avec libellés, quel que soit le client
def codelist_frame(
    client: Any,
    dimension: str,
    structure: DataflowStructure | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Return a codelist with its labels, for Eurostat and Comtrade alike.

    Resolves the codelist of a dataflow dimension (``DimensionInfo.codelist``)
    and delegates to the client's cached ``get_codelist``: building the split
    queries and publishing reference tables (product and country labels) from
    the same client costs one network call per codelist, none once it is in
    cache.

    Args:
        client: A client exposing ``get_codelist`` (``EurostatClient``,
            ``ComtradeClient``).
        dimension: Dimension name looked up in ``structure`` (matched
            case-insensitively) or, without ``structure``, the codelist
            identifier itself (Eurostat ``"CL_GEO"`` / ``"CXT_FREE_ISO"``,
            Comtrade ``"reporter"`` / ``"cmd:HS"``…).
        structure: Dataflow structure carrying the dimension → codelist
            mapping (e.g. ``client.get_dataflow_structure(...)`` or
            ``client.resolve_query_structure(query)``).
        **kwargs: Forwarded to ``get_codelist`` (e.g. ``refresh=True``, or
            Comtrade's ``keep_metadata=True``).

    Returns:
        DataFrame with columns ``code``, ``label`` and, when the codelist is
        hierarchical, ``parent``.

    Raises:
        TypeError: If the client exposes no ``get_codelist``.
        KeyError: If ``dimension`` is not a dimension of ``structure``.
        ValueError: If the dimension declares no codelist.

    Examples:
        >>> structure = client.get_dataflow_structure("DS-045409")  # doctest: +SKIP
        >>> products = codelist_frame(client, "product", structure)  # doctest: +SKIP
        >>> countries = codelist_frame(ComtradeClient(), "reporter")  # doctest: +SKIP
    """
    # Client compatible : codelists avec libellés mises en cache
    get_codelist = getattr(client, "get_codelist", None)
    if not callable(get_codelist):
        raise TypeError(
            f"{type(client).__name__} exposes no get_codelist(): codelists with "
            "labels are available for EurostatClient and ComtradeClient"
        )

    # Sans structure : la dimension désigne directement la codelist
    if structure is None:
        return get_codelist(dimension, **kwargs)

    # Résolution de la codelist de la dimension (insensible à la casse)
    matches = [d for d in structure.dimensions if d.name.lower() == dimension.lower()]
    if not matches:
        raise KeyError(
            f"Dimension '{dimension}' not found in {structure.dataflow}: "
            f"{[d.name for d in structure.dimensions]}"
        )
    codelist_id = matches[0].codelist
    if not codelist_id:
        raise ValueError(
            f"Dimension '{dimension}' of {structure.dataflow} declares no codelist"
        )
    return get_codelist(codelist_id, **kwargs)
