# Référence d'architecture — pandas 3, couverture des tests, fabrique de clients

> Document de travail destiné aux sessions d'implémentation décrites dans
> `PROMPTS_IMPLEMENTATION.md`. Il fixe les décisions ; il **ne doit jamais être
> cité** dans le code, les commentaires ou les docstrings, qui restent
> autoporteurs. À supprimer (ou archiver hors du dépôt) une fois les trois chantiers
> fusionnés.

État des lieux mesuré le 2026-10-10 sur `main` (`ca0d316`), Python 3.13.4,
`uv sync --all-extras`.

---

## 0. État des lieux

### 0.1 Dépendances

| Élément | Valeur |
|---|---|
| Borne déclarée | `pandas>=2.3.3` (`pyproject.toml`) |
| Version verrouillée | `pandas==3.0.6`, `pandas-stubs==3.0.5.260914`, `numpy==2.5.2`, `pyarrow==25.0.1`, `duckdb==1.5.6`, `narwhals==2.25.0` |
| `dt-ducklake-manager` | `v0.4.0` (git), dépend de `duckdb`, `narwhals`, `pyarrow` — pas de pandas direct |

Le projet **tourne déjà sous pandas 3.0.6** (Dependabot, `d30c172`) : les 499 tests
passent, y compris avec `-W error::pandas.errors.Pandas4Warning
-W error::pandas.errors.ChainedAssignmentError -W error::FutureWarning`. Mypy
(`pandas-stubs` 3) et ruff sont verts. Le chantier pandas est donc un chantier de
**garantie** (borne déclarée, filet de sécurité, tests de caractérisation des
changements sémantiques), pas de réécriture.

Écart à corriger : la borne `>=2.3.3` promet une compatibilité pandas 2 que la CI ne
teste plus jamais.

À noter : `uv.lock` enregistre encore `statflows` en `0.1.0` alors que
`pyproject.toml` est en `0.1.1` (release-please ne met pas le lock à jour) ; toute
commande `uv run` / `uv lock` réécrit cette ligne. C'est attendu : le `uv lock` de P1
l'embarquera.

### 0.2 Couverture

| Périmètre | Couverture (lignes + branches) |
|---|---|
| Unitaires seuls (`-m "not integration"`, tous extras) | **89 %** |
| Unitaires + intégration | **97 %** |

Modules sous-couverts par les **tests unitaires** :

| Module | Unitaires | + intégration | Lignes manquantes (unitaires) |
|---|---|---|---|
| `core/download.py` | 40 % | 91 % | quasiment tout `SDMXDownloader.run` et suivants ; `download_updates` (1369-1391) jamais couvert |
| `storage/json/s3/loader.py` | 32 % | 71 % | 71-86 |
| `storage/json/s3/saver.py` | 32 % | 71 % | 78-93 |
| `storage/_connection.py` | 52 % | 68 % | 64, 109, 114-159 (branche `s3fs`, repli sur l'environnement) |
| `storage/json/loader.py` | 51 % | 96 % | 124-136, 188-206 (branches S3) |
| `storage/__init__.py` | 70 % | 70 % | 22-24 (`__getattr__` paresseux) |
| `core/queries.py` | 82 % | 82 % | 100-101, 118-119 (implémentations par défaut) |
| `sources/comtrade/parsing.py` | 87 % | 87 % | 68, 107-110, 155 |
| `sources/comtrade/queries.py` | 89 % | 89 % | 112, 127-147 (`identity_key` : **clé du registre de téléchargement**) |
| `sources/eurostat/queries.py` | 94 % | 94 % | 74, 87 |
| petits restes | — | — | `comtrade/client.py` 167, 1315-1316 ; `eurostat/client.py` 927 ; `unsd/client.py` 336-338 ; `core/registry.py` 298, 380, 399 ; `storage/json/saver.py` 129 |

### 0.3 Herméticité des tests

- Aucun test n'ouvre de socket (vérifié en bloquant `socket.socket.connect`).
- **Défaut constaté** : avec `COMTRADE_SUBSCRIPTION_KEY` défini dans
  l'environnement du développeur, `tests/unit/sources/comtrade/test_client.py::
  test_request_tariffline_public_preview` échoue. Cause : la fixture `client` passe
  `subscription_key=None`, que `ComtradeClient` interprète comme « lire la variable
  d'environnement ». Les variables `AWS_*` lues par `S3Connection._connect` posent
  le même risque.
- Rien n'empêche aujourd'hui un futur test d'appeler le réseau par inadvertance.

### 0.4 Construction des clients

- `statflows.core.factory.build_client(provider)` ne connaît que `"eurostat"` et
  `"oecd"` (2 fournisseurs sur 4), sans aucune option (impossible de passer un
  `timeout`, une clé Comtrade, `auto_load_rate_limit=False`…).
- Le projet consommateur `trade-analysis` maintient sa propre `ClientFactory`
  (`kedro_pipeline/io/clients.py`) qui désigne les classes `statflows` par chemin
  pointé : la brique générique gagnerait à vivre dans `statflows`.
- Signatures actuelles des constructeurs (toutes sans appel réseau) :
  - `EurostatClient(api_version=SDMXVersion.V3, base_url=None, timeout=90, structure_registry=None, auto_fetch_structure=True, rate_limiter=None, auto_load_rate_limit=True)`
  - `OECDClient(base_url=..., timeout=60, sdmx_version=SDMXVersion.V2, structure_registry=None, auto_fetch_structure=True, rate_limiter=None, auto_load_rate_limit=True)`
  - `ComtradeClient(base_url=..., timeout=120, subscription_key=None, proxy=None, structure_registry=None, rate_limiter=None, auto_load_rate_limit=True, max_retries=3, backoff_factor=0.5)`
  - `UNSDClient(base_url=..., timeout=120, max_retries=3, backoff_factor=0.5)`
- `SDMXVersion` est un `str, Enum` (valeurs `"v1"`, `"v2"`, `"v2_1"`, `"v3"`) : une
  chaîne brute `"v3"` passe le contrôle d'appartenance d'`EurostatClient` mais casse
  plus loin (`.name`). Une configuration YAML/JSON ne peut pas exprimer un enum :
  la coercition doit se faire à la construction.

---

## 1. Axe A — pandas ≥ 3.0

### 1.1 Politique de dépendances

- `pandas>=3.0` dans `[project].dependencies` ; `pandas-stubs>=3.0` dans le groupe
  `dev`. `uv lock` puis `uv sync --all-extras`.
- `requires-python` reste `>=3.13` (pandas 3 exige ≥ 3.11 : pas de conflit).
- Changement **cassant** pour les consommateurs encore sous pandas 2 : commit
  `chore(deps)!: require pandas >= 3.0` (le `!` fait apparaître l'entrée dans le
  CHANGELOG généré par release-please). Ne pas éditer `CHANGELOG.md` ni la version.

### 1.2 Filet de sécurité

Aujourd'hui `[tool.pytest.ini_options].filterwarnings` ignore **tous** les
`DeprecationWarning` / `FutureWarning`, ce qui masquerait une dépréciation pandas.
Cible (les entrées plus basses priment sur les plus hautes) :

```toml
filterwarnings = [
    "ignore::DeprecationWarning",
    "ignore::FutureWarning",
    # pandas : toute dépréciation ou alerte de changement de comportement est une erreur
    "error::pandas.errors.PandasChangeWarning",
    "error::pandas.errors.ChainedAssignmentError",
]
```

`PandasChangeWarning` est la base commune de `Pandas4Warning`, `Pandas5Warning`,
`PandasDeprecationWarning`, `PandasFutureWarning` (vérifié sur 3.0.6). Si une
référence de classe dans `filterwarnings` empêche la collecte sur l'installation
nue (job CI `bare-imports`), vérifier : pandas fait partie du socle, donc l'import
doit réussir.

### 1.3 Points d'audit (changements de comportement pandas 3)

| # | Changement pandas 3 | Emplacements | Risque | Action attendue |
|---|---|---|---|---|
| A1 | Dtype chaîne par défaut `str` (`StringDtype(na_value=nan)`) ; `Series.astype(str)` **conserve** les manquants (`nan`) au lieu de produire `"nan"` / `"None"` | `sources/eurostat/parsing.py:104` (`astype(str).str.strip()` puis `to_numeric`) ; `sources/comtrade/parsing.py:156,159` (codes de codelist) | Codes manquants qui devenaient la chaîne `"nan"` (et pouvaient matcher) deviennent `NaN` | Test de caractérisation : codelist Comtrade avec code manquant ; TSV Eurostat avec cellule vide et valeur flaguée (`"12.3 p"`, `":"`) |
| A2 | `groupby(...).sum()` sur colonnes `str` non-clés : concaténation | `sources/comtrade/client.py:1267` (agrégation des doublons sur la clé primaire) | Colonnes texte hors clé concaténées (`"kgkg"`), booléens additionnés — **défaut latent indépendant de pandas 3**, mais dont la sortie change de dtype | Test de caractérisation avec une colonne texte et une colonne booléenne hors clé. **Ne pas corriger dans ce chantier** : signaler dans le résumé (correctif à part) |
| A3 | Inférence de la résolution datetime (plus systématiquement `ns`) | `sources/comtrade/parsing.py:219` ; `sources/comtrade/client.py:1142` (`to_datetime(..., utc=True).to_pydatetime()`) | Comparaisons `since` / dates de publication, sérialisation du registre | Test : date de publication à la microseconde, aware UTC, comparée à un `since` |
| A4 | `read_csv` / `read_excel(dtype=str)` produisent `str` ; `"string"` = `StringDtype(na_value=pd.NA)` ; mélange `str` / `string` | `core/client.py:1057` ; `sources/eurostat/parsing.py:82` ; `sources/unsd/parsing.py:195-200, 334, 357, 450` | Dtype de sortie documenté (`string`) ; `pd.NA` vs `nan` ; `zfill` | Vérifier que les dtypes documentés dans les docstrings sont ceux réellement produits ; test sur une colonne de codes avec manquants |
| A5 | Copy-on-Write par défaut ; affectation chaînée sans effet | aucune affectation chaînée trouvée (`grep`) ; `ChainedAssignmentError` non levé par la suite | Faible | Garder le filtre `error::` de 1.2 |
| A6 | Suppression des API dépréciées (`applymap`, `fillna(method=)`, alias de fréquences…) | aucune occurrence | Nul | — |
| A7 | Écriture DuckLake de colonnes `str` (pyarrow) via `narwhals` / `dt-ducklake-manager` | `storage/ducklake/tables.py`, `core/download.py:985` (`pd.concat` + `drop_duplicates`) | Couvert par `tests/integration/storage/ducklake` et `tests/integration/core/test_download_buffering.py` | Vérifier que ces tests tournent (extension DuckDB `ducklake` disponible) et ne sont pas `skip` |

### 1.4 Vérification

1. Suite complète sur la version verrouillée (`uv run pytest`).
2. Plancher : `uv run --with "pandas==3.0.0" pytest -m "not integration"`.
3. Pouvoir discriminant des nouveaux tests de caractérisation :
   `uv run --with "pandas==2.3.3" pytest <nouveaux tests>` — ceux qui figent un
   comportement propre à pandas 3 **doivent** échouer sous 2.3.3 (à constater, pas
   à committer).
4. `uv run mypy statflows/`, `uv run ruff check .`, `uv run ruff format --check .`.

---

## 2. Axe B — couverture des tests unitaires, sans clé API

### 2.1 Principes

- **Hermétiques** : aucun accès réseau, aucune dépendance aux variables
  d'environnement de la machine (`COMTRADE_SUBSCRIPTION_KEY`, `AWS_*`).
- **Doubles existants à réutiliser** : `tests/utils/fake_sdmx.py` (`FakeClient`,
  `FakeQuery`, `FakeClock`, `make_queries`, `expected_table`),
  `tests/utils/registries.py`, `tests/utils/ducklake.py` ; dans les tests de
  clients, remplacement de `client.get` / `_get_json` par `unittest.mock`.
- **Pas de `moto` dans `tests/unit/`** : S3 simulé reste dans `tests/integration/`.
  Côté unitaire, on remplace `boto3.client` / `s3fs.S3FileSystem` par des mocks.
- Extras optionnels : un test qui requiert `duckdb`, `dt_ducklake_manager`,
  `boto3`, `s3fs` ou `openpyxl` commence par `pytest.importorskip(..., reason=...)`
  (le job CI `bare-imports` exécute `tests/unit` sans extras).
- On teste des **comportements observables** (rapport, registre écrit, appels au
  connecteur, exceptions, logs) ; on n'ajoute pas de test qui fige un défaut — un
  défaut découvert est signalé, pas « verrouillé ».
- Structure miroir du paquet (`tests/unit/<sous-paquet>/test_<module>.py`),
  docstring de module « Frozen behaviour: … » en anglais, commentaires en français,
  séparateurs `# ───` comme dans les fichiers existants.

### 2.2 Garde-fous globaux (fixtures `autouse` dans `tests/conftest.py`)

1. **Environnement neutre** : `monkeypatch.delenv(..., raising=False)` pour
   `COMTRADE_SUBSCRIPTION_KEY`, `AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`. La fixture `aws_env` de
   l'intégration les repose ensuite explicitement (une fixture `autouse` d'un
   conftest parent s'exécute avant les fixtures demandées par le test).
2. **Réseau interdit pour `tests/unit/`** : `socket.socket.connect` et
   `socket.create_connection` remplacés par une fonction levant une erreur
   explicite (« network access is forbidden in unit tests »). Sans nouvelle
   dépendance. Restreint aux items situés sous `tests/unit/` (DuckDB télécharge
   l'extension `ducklake` en C++, hors Python, côté intégration ; moto n'ouvre pas
   de socket).
3. Tests de non-régression des garde-fous eux-mêmes (un test qui vérifie qu'une
   connexion lève, un test qui vérifie que la variable Comtrade est absente).

### 2.3 Cibles par module

| Lot | Module | Cas à couvrir |
|---|---|---|
| B1 | `core/download.py` (unitaires, `importorskip("dt_ducklake_manager")`) | `SDMXDownloader` piloté par `FakeClient` avec **connecteur et écriture simulés** : `monkeypatch` de `SDMXDownloader._connect` (renvoie un `MagicMock`) et de `statflows.core.download.write_dataframe` (enregistre les appels). Cas : requête unique non itérable (`_as_query_list`) ; `_prioritize` (jamais téléchargées d'abord, puis plus anciennes) ; échec d'une requête (rapport en échec, date **non** mise à jour) ; `max_runtime` atteint (arrêt anticipé, requêtes restantes comptées) ; SIGTERM — **en appelant `_handle_sigterm` directement** depuis `FakeClient.on_fetch` (portable, aucun vrai signal envoyé) : pendant la fenêtre interruptible (`_GracefulStop`) et hors fenêtre (drapeau seul), restauration du gestionnaire précédent, `run()` hors du thread principal (aucun gestionnaire installé, avertissement) ; rappel `on_query_complete` qui lève (avertissement, run poursuivi) ; `registry_shard_key` ; échec de flush du registre retenté au flush suivant ; `_concat_batch` avec doublons de clé primaire (dernier gagnant, avertissement) ; `_flush_buffer` sur lot vide ; `_buffer_full` par lignes et par requêtes ; `storage_options` + `bucket` → `connect` appelé sur `Loader`/`Saver` ; agrégation des `HttpStats` portés par composition (`_http_stats_sources`, `_total_http_stats`) ; `download_updates` délègue à `SDMXDownloader.run` avec les mêmes arguments |
| B2 | `storage/_connection.py`, `storage/__init__.py`, `storage/json/loader.py`, `storage/json/s3/{loader,saver}.py`, `storage/json/saver.py:129` | `S3Connection` : paquet invalide ; branche `boto3` avec arguments explicites puis repli sur l'environnement (variables posées par `monkeypatch`) ; branche `s3fs` ; `ImportError` avec message d'extra quand `boto3`/`s3fs` manquent (`monkeypatch` de `sys.modules[...] = None`) ; `__getattr__` paresseux de `statflows.storage` (succès et `AttributeError`) ; chargeur/sauveur S3 : clé sans `.json`, connexion paresseuse, objet absent avec/sans `missing_ok`, JSON invalide ; `Loader.list_json` en mode S3 (`boto3` : plusieurs pages du paginateur, fichiers masqués et non-JSON filtrés ; `s3fs` : `glob`) |
| B3 | `core/queries.py`, `sources/comtrade/{queries,parsing,client}.py`, `sources/eurostat/{queries,client}.py`, `sources/unsd/client.py`, `core/registry.py` | `identity_key` / `get_dataflow_key` par défaut et surchargés — **valeurs exactes figées** (la clé indexe le registre de téléchargement : la changer relance des téléchargements complets) et stabilité vis-à-vis de l'ordre des listes / des champs `None` ; `build_structure_from_parameters` sur dataflow inconnu ; `extract_codes` catégorie `partner` et catégorie inconnue ; repli de la colonne de libellé ; registre de structures injecté dans `ComtradeClient` ; `_fetch_structure` Comtrade depuis les paramètres ; `EurostatClient._fetch_structure` délègue ; `UNSDClient.close` ; `iter_registry_entries` avec `bucket` + `storage_options` ; `DownloadRegistry.get` / `__len__` / `assign_shards` en mode non fragmenté |

### 2.4 Objectifs chiffrés

- Unitaires seuls (tous extras) : **≥ 95 %** global, aucun module `statflows/` sous
  85 %.
- Unitaires + intégration : **≥ 98 %**.
- Seuil CI : `--cov-fail-under=95` sur l'étape d'**intégration** du job `test` (celle
  qui fait `--cov-append` et produit le rapport final), pas sur l'étape unitaire.

---

## 3. Axe C — fabrique de clients

### 3.1 Décision

Utile, avec un périmètre volontairement étroit :

- complète `build_client` (4 fournisseurs, options) ;
- fournit un **objet picklable** décrivant « comment construire un client » —
  nécessaire dès qu'un client est créé dans un autre processus, un nœud Kedro, un
  job planifié, ou décrit dans une configuration YAML/JSON ;
- permet à `trade-analysis` de supprimer sa copie locale.

Hors périmètre : pool / cache de clients, singleton, partage de rate limiter entre
processus, modification de `SDMXDownloader` / `download_updates` (qui continuent de
recevoir un client **instancié** ; l'appelant écrit `with factory() as client:`).

### 3.2 Améliorations par rapport à la référence `trade-analysis`

| Référence | Cible `statflows` |
|---|---|
| Chemin pointé uniquement | Alias de fournisseur (`"eurostat"`, `"oecd"`, `"comtrade"`, `"unsd"`) **ou** chemin pointé (`"pkg.mod.Name"` / `"pkg.mod:Name"`) pour brancher un double de test |
| Erreurs découvertes au premier appel (souvent dans un autre processus) | Validation **à la construction** : import de la cible, vérification qu'elle est appelable, `inspect.signature(target).bind(**kwargs)` → `TypeError` immédiat sur option inconnue |
| Options brutes | Coercition des chaînes en `Enum` quand l'annotation du paramètre est un `Enum` (y compris `X \| None`) : par valeur, puis par nom insensible à la casse (`"v3"`, `"V3"` → `SDMXVersion.V3`) ; erreur explicite listant les valeurs admises |
| Secrets mêlés aux options, `repr` muet | `options` (affichées) et `credentials` (jamais affichées) séparées ; clés en conflit → `ValueError` ; `repr` = `ClientFactory('comtrade', options={'timeout': 60}, credentials=<1 hidden>)` |
| — | `__eq__` (cible résolue + options + credentials) ; non hachable |
| Typage `Any` | `build_client` surchargé (`typing.overload` + `Literal`) : `build_client("comtrade")` est typé `ComtradeClient` |

### 3.3 API cible

Module : `statflows/core/factory.py` (il héberge déjà `build_client`,
`build_queries`, `filter_codes`, `codelist_frame`).

```python
# Table alias → chemin pointé (import paresseux : aucun client importé au chargement)
CLIENT_PATHS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "eurostat": "statflows.sources.eurostat.client.EurostatClient",
        "oecd": "statflows.sources.oecd.client.OECDClient",
        "comtrade": "statflows.sources.comtrade.client.ComtradeClient",
        "unsd": "statflows.sources.unsd.client.UNSDClient",
    }
)


def _import_object(path: str) -> Any: ...  # privé ; "a.b.C" ou "a.b:C"


class ClientFactory:
    def __init__(
        self,
        target: str,  # alias ou chemin pointé
        options: Mapping[str, Any] | None = None,
        credentials: Mapping[str, Any] | None = None,
    ) -> None: ...

    target: str  # tel que fourni
    path: str  # chemin pointé résolu
    options: dict[str, Any]  # après coercition des enums

    def __call__(self) -> Any: ...  # nouveau client à chaque appel
    def __repr__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

    __hash__ = None


@overload
def build_client(provider: Literal["eurostat"], **options: Any) -> EurostatClient: ...
@overload
def build_client(provider: Literal["oecd"], **options: Any) -> OECDClient: ...
@overload
def build_client(provider: Literal["comtrade"], **options: Any) -> ComtradeClient: ...
@overload
def build_client(provider: Literal["unsd"], **options: Any) -> UNSDClient: ...
@overload
def build_client(provider: str, **options: Any) -> Any: ...
def build_client(provider: str, **options: Any) -> Any:
    """Raccourci : ClientFactory(provider, options)()."""
```

Règles :

- `build_client` n'accepte **que les alias** (message d'erreur inchangé :
  `Unknown provider '<x>'` complété de la liste des alias connus) ; seul
  `ClientFactory` accepte un chemin pointé. Rétrocompatible :
  `build_client("eurostat")` / `build_client("oecd")` se comportent comme avant.
- Les imports des classes de clients pour les `overload` se font sous
  `if TYPE_CHECKING:` (pas d'import supplémentaire à l'exécution).
- Picklable : l'état ne contient que des chaînes et des dictionnaires (les membres
  d'`Enum` se picklent). Si une option n'est pas picklable (ex. un `RateLimiter`
  portant un verrou), le pickling échoue normalement — documenté dans la
  docstring, pas contourné.
- `credentials` ne sont pas coercées ni validées par nom au-delà du `bind` ; elles
  ne sont jamais journalisées.
- Pas de lecture de variables d'environnement dans la fabrique : le repli
  `COMTRADE_SUBSCRIPTION_KEY` reste dans `ComtradeClient`.
- Exports : `ClientFactory` et `build_client` ajoutés à `statflows.__all__`
  (`statflows/__init__.py`, import en dernier depuis `.core.factory`). **Ne pas**
  les exporter depuis `statflows/core/__init__.py` : `core.factory` importe
  `sources.*`, ce qui créerait un cycle d'import.

### 3.4 Documentation

- `docs/api/core/ClientFactory.md` (`::: statflows.core.factory.ClientFactory`) et
  entrée dans la `nav` de `mkdocs.yml` à côté de `build_client`.
- Section « Factories » de `docs/providers.md` : exemple YAML → `ClientFactory`
  → `with factory() as client: download_updates(client, ...)`.
- README : mention dans la liste des briques `core` (garder les marqueurs
  `<!-- --8<-- [start:...] -->`).
- `uv run mkdocs build --strict` doit passer.

### 3.5 Tests (`tests/unit/core/test_factory.py`)

Alias → bonne classe (4 fournisseurs) ; chemin pointé `.` et `:` vers un double
défini dans `tests/utils/` ; cible inconnue / non appelable ; option inconnue →
`TypeError` à la construction ; coercition d'enum (valeur, nom, casse, `None`
conservé, valeur invalide) ; `credentials` transmises au constructeur et absentes
du `repr` ; conflit options/credentials ; chaque appel → nouvelle instance ;
aller-retour `pickle` puis appel ; égalité ; `build_client` avec options et
rétrocompatibilité ; aucune construction n'ouvre le réseau (garanti par le
garde-fou 2.2).

---

## 4. Ordre et dépendances entre prompts

```
P0 herméticité ──┬─> P1 pandas 3 ──────────────┐
                 ├─> P2 tests download.py ─────┤
                 ├─> P3 tests storage ─────────┼─> P6 vérification finale + seuil CI
                 ├─> P4 petits trous ──────────┤
                 └─> P5 fabrique de clients ───┘
```

P1 à P5 sont indépendants entre eux une fois P0 fusionné (fichiers disjoints, à
l'exception de `pyproject.toml` pour P1 et P6) ; ils peuvent tourner dans des
worktrees séparés. En séquentiel, l'ordre P0 → P1 → P2 → P3 → P4 → P5 → P6 est
recommandé.
