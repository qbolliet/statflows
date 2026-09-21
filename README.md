# statflows

[![CI](https://github.com/qbolliet/statflows/actions/workflows/ci.yml/badge.svg)](https://github.com/qbolliet/statflows/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/qbolliet/statflows/branch/main/graph/badge.svg)](https://codecov.io/gh/qbolliet/statflows)
[![Python](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

Ce package contient un ensemble de clients d'API statistiques (Eurostat, OCDE, COMTRADE, UNSD), et un orchestrateur de téléchargement incrémental permettant d'industrialiser le téléchargement et le suivi de la mise à jour de bases de données. Pour les fournisseurs de données utilisant SDMX, une structure logicielle commune est utilisée et est adaptée pour les autres fournisseurs de données.

## Ce que fait le package

- **`statflows.sources`** — un client par fournisseur : `EurostatClient`,
  `OECDClient` (SDMX 2.1 / 3.0), `ComtradeClient` et `UNSDClient`. Chaque client
  construit ses URL à partir de la structure du *dataflow*, applique un
  *rate limiting* propre au fournisseur, parse la réponse (CSV / SDMX-JSON /
  SDMX-ML) et renvoie un `pandas.DataFrame`.
- **`statflows.core`** — le socle partagé : client HTTP (`APIClient`),
  *rate limiters*, registre de structures de *dataflow*, rapports d'exécution
  structurés (`DownloadReport`, `QueryReport`), et les fabriques `build_client` /
  `build_queries` / `filter_codes`.
- **`statflows.core.download`** — l'orchestrateur `download_updates` /
  `SDMXDownloader` : pour une liste de requêtes, premier téléchargement **complet**
  des séries absentes puis téléchargement **incrémental** des séries déjà
  présentes, écriture dans un catalogue DuckLake (un schéma par *dataflow*), et
  tenue d'un registre JSON des dates de dernier téléchargement.
- **`statflows.storage`** — `S3Connection` (session `boto3` / `s3fs` partagée) et
  `statflows.storage.json` (`Loader` / `Saver` de registres JSON, en local ou sur
  S3). `statflows.storage.ducklake.tables` fournit le helper `write_dataframe`
  (création du schéma au premier appel, *upsert* par clé primaire ensuite).

## Installation

Le package s'installe depuis git (pas encore publié sur PyPI) :

```bash
# Socle seul : clients + parsing + requêtes ponctuelles
pip install "git+https://github.com/qbolliet/statflows"

# Avec le stockage S3 des registres JSON
pip install "statflows[s3] @ git+https://github.com/qbolliet/statflows"

# Avec l'écriture DuckLake (tire dt-ducklake-manager + duckdb)
pip install "statflows[ducklake] @ git+https://github.com/qbolliet/statflows"

# Tout
pip install "statflows[all] @ git+https://github.com/qbolliet/statflows"
```

| Extra        | Contenu                     | Requis pour                                                              |
|--------------|-----------------------------|-------------------------------------------------------------------------|
| *(base)*     | `requests`, `pandas`, `pyarrow`, `pyyaml` | clients, requêtes ponctuelles, parsing, registres JSON **en local** |
| `s3`         | `boto3`, `s3fs`             | lecture/écriture des registres JSON **sur un bucket** (`Loader`/`Saver` avec `bucket=...`, `download_updates` avec `bucket=...`) |
| `ducklake`   | `duckdb`, `dt-ducklake-manager` | `statflows.storage.ducklake.tables` (écriture), `statflows.core.download` |
| `all`        | union                       | —                                                                       |

Le socle (`import statflows`, `statflows.core`, `statflows.sources`,
`statflows.storage`) s'importe sans aucun extra. `statflows.storage.json`
(`Loader` / `Saver`) s'importe et fonctionne **en local** sans l'extra `s3` ; un
appel avec `bucket=...` sans `boto3` installé lève une `ImportError` explicite.
`statflows.core.download` et `statflows.storage.ducklake.tables` ne sont chargés
que par import explicite.

## Exemples

### 1. Une requête Eurostat ponctuelle (sans DuckLake)

```python
from statflows import EurostatClient

client = EurostatClient()  # SDMX 3.0 par défaut

df = client.get_data(
    "namq_10_gdp",
    dimensions={"geo": ["FR", "DE"], "na_item": "B1GQ", "unit": "CP_MEUR"},
    start_period="2020-Q1",
    last_n_observations=8,
)
print(df.head())
```

### 2. Un aller-retour `Loader` / `Saver` JSON sur S3

```python
from statflows.storage.json import Loader, Saver

# Identifiants lus depuis les variables d'environnement AWS_* (ou passés en
# kwargs : aws_access_key_id=..., aws_secret_access_key=..., endpoint_url=...).
registry = {
    "DOWNLOADS": {
        "eurostat:namq_10_gdp": {"last_download": "2026-08-31T00:00:00Z"},
    }
}

Saver().save(
    "registries/last_download.json",
    registry,
    bucket="my-bucket",
    indent=2,
)

# missing_ok=True : un premier run (objet absent) renvoie None au lieu de lever.
loaded = Loader().load(
    "registries/last_download.json",
    bucket="my-bucket",
    missing_ok=True,
) or {}

assert loaded == registry
```

### 3. Un `download_updates` complet vers un catalogue DuckLake local

```python
from datetime import timedelta

from dt_ducklake_manager import DuckLakeConnector

from statflows import EurostatClient
from statflows.core.factory import build_queries
from statflows.core.download import download_updates

# Catalogue DuckLake local : métadonnées dans catalog.ducklake, données Parquet
# sous data/ — aucun serveur PostgreSQL requis.
connector = DuckLakeConnector("catalog.ducklake", "data/")

queries = build_queries(
    "eurostat",
    [
        {"dataflow": "namq_10_gdp", "dimensions": {"geo": ["FR", "DE"], "na_item": "B1GQ"}},
        {"dataflow": "une_rt_q", "dimensions": {"geo": ["FR", "DE"], "sex": "T", "age": "TOTAL"}},
    ],
)

report = download_updates(
    client=EurostatClient(),
    queries=queries,
    connector=connector,
    structures_path="registries/eurostat_structures.json",
    last_download_path="registries/eurostat_last_download.json",
    n_observations=10,
    max_runtime=timedelta(hours=1),
)

print(report)  # DownloadReport : requêtes traitées, lignes écrites, erreurs, HTTP
```

Le deuxième appel avec les mêmes requêtes ne télécharge que les observations
nouvellement publiées (mode incrémental), d'après les dates enregistrées dans
`eurostat_last_download.json`.

## Développement

```bash
uv sync --all-extras
uv run pytest                        # toute la suite
uv run pytest tests/unit             # unitaires seuls (tournent sur une install nue)
uv run pytest -m "not integration"   # exclut les tests contre services simulés
uv run pytest --cov                  # avec le rapport de couverture (terminal)
uv run pytest --cov --cov-report=html   # rapport HTML dans htmlcov/
```

Arborescence des tests :

- `tests/unit/` — unitaires, calqués sur le package (`core/`, `sources/`,
  `storage/json/`, `storage/ducklake/`) : logique pure et I/O locale, aucun
  service externe.
- `tests/integration/` — bout-en-bout contre `moto` (S3) et un catalogue DuckLake
  sur fichier ; chaque test y est marqué `integration`.
- `tests/utils/` — fonctions et classes de test partagées.

Les tests DuckLake sont ignorés (`skip`) dès la collecte quand `duckdb` /
`dt_ducklake_manager` sont absents ; les tests S3 quand `moto` est absent.
