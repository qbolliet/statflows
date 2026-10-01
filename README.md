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
  structurés (`DownloadReport`, `QueryReport`), les fabriques `build_client` /
  `build_queries` / `filter_codes`, et `codelist_frame` (codelists avec
  libellés, Eurostat et Comtrade).
- **`statflows.core.download`** — l'orchestrateur `download_updates` /
  `SDMXDownloader` : pour une liste de requêtes, premier téléchargement **complet**
  des séries absentes puis téléchargement **incrémental** des séries déjà
  présentes, écriture dans un catalogue DuckLake (un schéma par *dataflow*), et
  tenue d'un registre JSON des dates de dernier téléchargement — en fichier
  unique ou fragmenté, lisible via `iter_registry_entries` — avec tamponnage
  optionnel du registre et des écritures (voir « Performance et volumétrie »).
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

## Performance et volumétrie

### Coût du mode par défaut

Par défaut, `download_updates` / `SDMXDownloader` reproduisent le comportement
historique : **chaque requête** non vide fait l'objet d'une transaction DuckLake
(upsert, audit, compaction post-écriture) et **le registre entier** est réécrit
après chaque requête. C'est sûr mais coûteux sur un rattrapage complet
(10⁴ à 10⁵ requêtes) : le registre pèse plusieurs dizaines de Mo et sa
réécriture répétée a un coût quadratique, et chaque requête crée des snapshots
et des fichiers Parquet.

Mesure sur le banc de test (`tests/integration/core/test_download_buffering.py` :
2 000 requêtes dont 100 non vides, 2 *dataflows*, registre sur S3 simulé) :

| Mode                                                   | Écritures du registre | Lots DuckLake | Snapshots |
|--------------------------------------------------------|----------------------:|--------------:|----------:|
| défaut                                                 | 2 000                 | 100           | 200       |
| `registry_flush_every=500`, `write_batch_queries=500`  | 4                     | 2             | 4         |

(Un upsert de `dt-ducklake-manager` crée deux snapshots : l'écriture de la table
de faits et l'horodatage `dataset_metadata.updated_at`.)

### Paramètres

| Paramètre                | Défaut  | Effet |
|--------------------------|---------|-------|
| `registry_flush_every`   | `1`     | Persiste le registre après ce nombre d'entrées validées. |
| `registry_flush_seconds` | `None`  | Persiste aussi le registre après ce délai (vérifié à chaque validation). |
| `registry_shard_key`     | `None`  | `query → str` : registre fragmenté, seuls les fragments modifiés sont réécrits. |
| `write_batch_rows`       | `None`  | Écrit le lot d'un schéma dès qu'il atteint ce nombre de lignes. |
| `write_batch_queries`    | `None`  | Écrit le lot d'un schéma dès qu'il atteint ce nombre de requêtes non vides. |
| `update_options`         | `None`  | Options passées à `DatabaseUpdater.update_database` (ex. `allow_new_columns`). |
| `build_options`          | `None`  | Options passées à `DuckLakeTablesBuilder.build_schema` (ex. `partition_by`). |
| `ducklake_options`       | `None`  | Options DuckLake appliquées le temps de la connexion (dont `data_inlining_row_limit`). |
| `run_id`                 | `None`  | Identifiant de run inscrit sur chaque snapshot écrit. |

**Invariant.** Une entrée du registre n'avance **jamais** avant que les données
de sa requête aient été écrites avec succès. Les lots sont constitués **par
schéma** (un par *dataflow*), concaténés puis dédoublonnés par clé primaire
(dernier gagnant) ; les entrées de leurs requêtes ne sont validées qu'après
l'écriture du lot. Si l'écriture d'un lot échoue, aucune de ses entrées
n'avance, l'erreur est comptée pour chacune de ses requêtes dans le
`DownloadReport` (elles seront retéléchargées au run suivant) et le run
continue. Une requête vide valide son entrée immédiatement.

**Arrêts.** Les lots en attente et le registre sont persistés à la fin du run,
à l'échéance de `max_runtime`, sur exception et sur `SIGTERM` (arrêt d'un pod
Kubernetes) : pendant `run()`, un gestionnaire de signal est installé (dans le
thread principal seulement ; ailleurs, un avertissement est journalisé) puis
restauré. Un `SIGTERM` interrompt la récupération en cours — jamais une
écriture DuckLake ni une persistance du registre —, et `run()` retourne son
rapport avec `stopped_early=True`. La durée de grâce du pod
(`terminationGracePeriodSeconds`) doit couvrir l'écriture d'un lot.

**Diagnostics.** `DownloadReport` expose `n_registry_flushes`,
`n_write_batches` et `rows_pending_at_stop` (toujours `0` attendu), repris par
`to_metrics()`. Avec tamponnage, le `QueryReport` d'une requête non vide est
publié (et `on_query_complete` appelé) lorsque son lot est écrit ou a échoué.

**Mémoire.** Les DataFrames d'un lot restent en mémoire jusqu'à son écriture :
l'empreinte est bornée par `write_batch_rows` × nombre de *dataflows* actifs.

### Registre fragmenté

Avec `registry_shard_key`, le registre `registries/x_last_download.json` devient
un répertoire `registries/x_last_download/<fragment>.json`. Un registre existant
en fichier unique est migré à la première persistance (les entrées des requêtes
absentes du run vont dans `_default.json`) ; le fichier historique est laissé en
place : à la lecture, les deux formats sont fusionnés et la date la plus récente
l'emporte. Une écriture ne coûte que les fragments touchés depuis la persistance
précédente : choisir une clé **corrélée à l'ordre de traitement** (par exemple
le déclarant, si la liste de requêtes l'itère en boucle externe), sans quoi
chaque persistance retouche tous les fragments.

Les étapes aval lisent le registre sans dépendre de son format :

```python
from statflows import iter_registry_entries

for entry in iter_registry_entries("registries/eurostat_last_download.json", bucket="my-bucket"):
    print(entry.identity_key, entry.dataflow, entry.last_download)  # RegistryEntry gelée
```

### Compaction et inlining

Aucune compaction n'est lancée après les écritures : `write_dataframe` passe
`compact_after_update=False` à `update_database` (dont le défaut est `True`), car
compacter après chaque lot est coûteux et mieux placé en fin de run ou dans une
maintenance planifiée. Pour la réactiver malgré tout :
`update_options={"compact_after_update": True}` lance, après le commit de chaque
upsert (donc une fois par lot), la compaction légère de `dt-ducklake-manager`
sur `<schéma>.fact_table` : `ducklake_merge_adjacent_files` puis
`ducklake_rewrite_data_files`. Elle n'expire aucun snapshot, ne supprime aucun
fichier et ne vide pas les données inlinées ; ses échecs sont journalisés sans
interrompre l'écriture.

`DuckLakeConnector` accepte l'option d'ATTACH `DATA_INLINING_ROW_LIMIT`
(argument `data_inlining_row_limit`) : `ducklake_options={"data_inlining_row_limit": N}`
la fixe le temps de la connexion du run, sans modifier durablement le connecteur
(les autres clés sont fusionnées dans ses options `set_option`). Les écritures
de moins de `N` lignes sont alors conservées dans le catalogue plutôt qu'en
fichiers Parquet ; elles y restent jusqu'à un vidage explicite
(`ducklake_flush_inlined_data`), que la compaction post-écriture n'effectue pas.
Sur le banc de test, l'extension DuckLake fournie avec DuckDB 1.5 inline déjà
les petites écritures par défaut ; avec des lots volumineux, l'inlining ne
concerne plus que les derniers lots partiels.

### Configuration recommandée pour 10⁵ requêtes

```python
from datetime import timedelta

report = download_updates(
    client=EurostatClient(),
    queries=queries,                       # ~100 000 requêtes, déclarant en boucle externe
    connector=connector,
    structures_path="registries/comext_structures.json",
    last_download_path="registries/comext_last_download.json",
    bucket="my-bucket",
    max_runtime=timedelta(hours=23),
    # Registre : au plus une écriture par 1 000 entrées ou par 5 minutes,
    # fragmentée par dataflow et déclarant
    registry_flush_every=1_000,
    registry_flush_seconds=300,
    registry_shard_key=lambda q: f"{q.dataflow}_{q.dimensions.get('reporter')}",
    # DuckLake : une transaction par lot de 500 requêtes ou 500 000 lignes
    write_batch_queries=500,
    write_batch_rows=500_000,
    run_id="comext-2026-10-01",
)
assert report.rows_pending_at_stop == 0
```

Sur 10⁵ requêtes, le registre passe de 10⁵ réécritures complètes à une
centaine de persistances limitées aux fragments touchés, et le catalogue d'une
transaction par requête non vide à une par tranche de 500 (plus un lot partiel
par *dataflow* en fin de run).

### Codelists avec libellés

`statflows.core.factory.codelist_frame(client, dimension, structure=None)`
renvoie un DataFrame `code`, `label` (et `parent` pour les nomenclatures
hiérarchiques) pour `EurostatClient` et `ComtradeClient`. Les codelists sont
mises en cache par client : celles déjà téléchargées pour construire les
requêtes ne coûtent aucun second appel réseau.

```python
from statflows.core.factory import codelist_frame

structure = client.get_dataflow_structure("DS-045409")
products = codelist_frame(client, "product", structure)          # Eurostat Comext
countries = codelist_frame(comtrade_client, "reporter", keep_metadata=True)  # ISO, isGroup…
```

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
