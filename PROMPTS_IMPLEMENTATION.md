# Prompts d'implémentation — pandas 3, couverture des tests, fabrique de clients

Sept prompts à exécuter chacun dans une **session Claude Code indépendante**. Ils
sont autoporteurs ; les décisions détaillées sont dans `ARCHITECTURE_REFERENCE.md`
(à la racine), que chaque prompt demande de lire. Ce document de référence ne doit
**jamais** être cité dans le code, les commentaires, les docstrings ni les messages
de commit.

## Synthèse

| # | Objet | Modèle | Effort | Plan mode | Dépend de |
|---|---|---|---|---|---|
| P0 | Tests hermétiques (pas de réseau, pas de clé dans l'environnement) | Sonnet 5.5 | medium | Non | — |
| P1 | Passage à `pandas>=3.0` + filet de sécurité + tests de caractérisation | Opus 5.5 | high | **Oui** | P0 |
| P2 | Tests unitaires de `core/download.py` (40 % → ≥ 90 %) | Opus 5.5 | high | **Oui** | P0 |
| P3 | Tests unitaires des chemins S3 de `storage/` sans identifiants AWS | Sonnet 5.5 | medium | Non | P0 |
| P4 | Petits trous de couverture (clés d'identité, parsing, registres) | Sonnet 5.5 | medium | Non | P0 |
| P5 | `ClientFactory` + `build_client` étendu aux 4 fournisseurs | Opus 5.5 | high | **Oui** | P0 |
| P6 | Vérification finale, seuil de couverture en CI, nettoyage | Sonnet 5.5 | medium | Non | P1-P5 |

Pourquoi ces choix :

- **Opus 5.5 + plan mode** là où il faut du jugement avant d'écrire : audit des
  changements sémantiques de pandas 3 (P1), orchestrateur de 1 400 lignes avec
  signaux, délais et lots d'écriture où un test mal conçu figerait un défaut (P2),
  nouvelle API publique (P5).
- **Sonnet 5.5 sans plan mode** là où les cibles sont listées ligne par ligne et le
  travail est surtout d'exécution (P0, P3, P4, P6).
- Pas de recommandation `max` / `xhigh` : aucun chantier ne le justifie ; `high`
  suffit pour P1, P2, P5.

## Avant de commencer

1. Créer une branche de travail (le dépôt est sur `main`) :
   `git switch -c qb-pandas3-tests-factory`.
2. **Commiter `ARCHITECTURE_REFERENCE.md` et `PROMPTS_IMPLEMENTATION.md` sur cette
   branche** (`docs: add implementation working notes`) : sinon ils seront absents
   des worktrees si des prompts sont lancés en parallèle (`claude --worktree` ou
   `isolation: worktree` ne copient pas les fichiers non suivis).
3. Ordre conseillé en séquentiel : P0 → P1 → P2 → P3 → P4 → P5 → P6. Après P0,
   P1 à P5 touchent des fichiers disjoints et peuvent tourner en parallèle dans des
   worktrees (seul `pyproject.toml` est partagé entre P1 et P6).
4. Après P6, lancer `/code-review high` sur la branche avant d'ouvrir la PR.

---

## P0 — Rendre la suite de tests hermétique

**Modèle :** Sonnet 5.5 · **Effort :** medium · **Plan mode :** Non

```text
Contexte : dépôt Python `statflows` (clients Eurostat / OECD / UN Comtrade / UNSD,
registres JSON, écriture DuckLake). Outils : uv, pytest, ruff, mypy. Conventions :
commentaires de code en français, docstrings Google en anglais, Conventional Commits.
Lis `CONTRIBUTING.md`, `tests/conftest.py`, `tests/integration/conftest.py` et la
section 0.3 + 2.2 de `ARCHITECTURE_REFERENCE.md` (document de travail : ne le cite
jamais dans le code, les commentaires, les docstrings ni le message de commit).

Branche : travaille sur la branche courante si ce n'est pas `main` ; sinon crée
`qb-pandas3-tests-factory`.

Problème constaté : avec `COMTRADE_SUBSCRIPTION_KEY` défini dans l'environnement,
`tests/unit/sources/comtrade/test_client.py::test_request_tariffline_public_preview`
échoue, car `ComtradeClient(subscription_key=None)` se rabat sur cette variable.
`S3Connection._connect` lit de même `AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`. Rien n'empêche non plus un test
unitaire d'ouvrir le réseau.

Tâche :
1. Dans `tests/conftest.py`, ajoute une fixture `autouse` qui retire ces cinq
   variables (`monkeypatch.delenv(..., raising=False)`) pour TOUS les tests. La
   fixture `aws_env` de `tests/integration/conftest.py` doit continuer de les
   reposer (vérifie l'ordre d'exécution des fixtures).
2. Ajoute une fixture `autouse` qui interdit le réseau aux seuls tests situés sous
   `tests/unit/` : remplace `socket.socket.connect` et `socket.create_connection`
   (via `monkeypatch`) par une fonction levant une erreur explicite ("network access
   is forbidden in unit tests: <adresse>"). Aucune nouvelle dépendance (pas de
   pytest-socket). Détermine l'appartenance à `tests/unit/` à partir du chemin de
   l'item, comme le fait `pytest_collection_modifyitems` dans le conftest
   d'intégration.
3. Ajoute des tests de non-régression des garde-fous (par ex.
   `tests/unit/test_hermetic.py`) : une connexion socket lève l'erreur attendue ;
   les variables listées sont absentes même si le processus pytest les a reçues.
4. Mets à jour la docstring de module de `tests/conftest.py` (en anglais) pour
   décrire ces garanties. Ne modifie aucun code de production.

Vérifications (toutes doivent passer) :
- `uv sync --all-extras`
- `uv run pytest` (suite complète)
- même commande avec `COMTRADE_SUBSCRIPTION_KEY=fake` et `AWS_ACCESS_KEY_ID=fake`
  exportés dans l'environnement (adapte la syntaxe au shell)
- `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy statflows/`

Termine par un résumé des changements, puis crée un commit (sans push) :
`test: make the test suite hermetic (no network, no ambient credentials)`.
```

---

## P1 — Passer à pandas ≥ 3.0 sans régression

**Modèle :** Opus 5.5 · **Effort :** high · **Plan mode :** Oui

```text
Contexte : dépôt Python `statflows` (clients de statistiques Eurostat / OECD /
UN Comtrade / UNSD, parsing vers pandas, écriture DuckLake via narwhals). Outils :
uv, pytest, ruff, mypy. Conventions : commentaires en français, docstrings Google en
anglais, Conventional Commits ; ne modifie ni la version de `pyproject.toml` ni
`CHANGELOG.md` (release-please). Lis `CONTRIBUTING.md`, `pyproject.toml` et la
section 1 de `ARCHITECTURE_REFERENCE.md` (document de travail : ne le cite jamais
dans le code, les commentaires, les docstrings ni le message de commit).

Branche : la branche courante (pas `main`).

Situation : `pyproject.toml` déclare `pandas>=2.3.3` mais `uv.lock` résout déjà
pandas 3.0.6 et toute la suite passe dessus. L'objectif est de GARANTIR pandas 3 :
borne déclarée, filet de sécurité contre les dépréciations, et tests de
caractérisation des changements sémantiques qui ne sont aujourd'hui couverts par
aucun test.

Tâche — commence en plan mode : relis chaque emplacement du tableau 1.3 du document
d'architecture, confirme ou infirme le risque en exécutant de petits scripts
(`uv run python -c ...`), et présente-moi le plan (tests à ajouter, changements de
code éventuels) avant d'écrire quoi que ce soit.

1. Dépendances : `pandas>=3.0` dans `[project].dependencies`, `pandas-stubs>=3.0`
   dans le groupe `dev`, puis `uv lock` et `uv sync --all-extras`. Mets à jour la
   mention des dépendances du socle dans le README si une version y figure.
2. Filet de sécurité : dans `[tool.pytest.ini_options].filterwarnings`, garde les
   `ignore::DeprecationWarning` / `ignore::FutureWarning` (bruit des dépendances
   tierces) mais ajoute APRÈS eux `error::pandas.errors.PandasChangeWarning` et
   `error::pandas.errors.ChainedAssignmentError` (dans pytest, la dernière entrée
   qui correspond l'emporte). Vérifie que la collecte fonctionne aussi sur une
   installation nue : `uv venv .venv-bare`, `uv pip install --python .venv-bare . pytest`,
   puis `python -m pytest tests/unit` avec l'interpréteur de `.venv-bare`
   (`Scripts/` sous Windows, `bin/` ailleurs) ; supprime `.venv-bare` ensuite.
3. Audit et tests de caractérisation (points A1 à A4 du document) :
   - A1 `astype(str)` conserve désormais les manquants : `sources/eurostat/parsing.py`
     (TSV avec cellule vide, `":"`, valeur flaguée comme `"12.3 p"`) et
     `sources/comtrade/parsing.py` (codelist avec code manquant).
   - A2 `sources/comtrade/client.py` : agrégation `groupby(...).sum()` des doublons
     sur la clé primaire avec une colonne texte et une colonne booléenne HORS clé.
     C'est un défaut latent (concaténation de texte) indépendant de pandas 3 : fige
     le comportement actuel uniquement s'il est souhaitable, sinon ne le fige pas et
     signale-le dans ton résumé final. Ne le corrige pas dans ce commit.
   - A3 dates de publication Comtrade (`parsing.py` ~l.219, `client.py` ~l.1142) :
     résolution et fuseau du `datetime` renvoyé, comparaison avec `since`.
   - A4 dtypes produits par la lecture UNSD (`dtype=str`, `astype("string")`,
     `pd.NA`) : ils doivent correspondre à ce que disent les docstrings ; corrige la
     docstring (pas le comportement) en cas d'écart.
   Les tests vont dans les fichiers existants de `tests/unit/sources/<fournisseur>/`.
   Si un écart révèle un vrai bug introduit par pandas 3, corrige-le avec un test qui
   échouait avant le correctif.
4. Vérifie que les tests d'intégration DuckLake (`tests/integration/storage/ducklake`,
   `tests/integration/core/test_download_buffering.py`) s'exécutent réellement et ne
   sont pas `skip` (ils couvrent l'écriture de colonnes `str` pyarrow).

Vérifications (toutes doivent passer, sauf l'étape 3 qui est un constat) :
1. `uv run pytest` (suite complète, nouveaux filtres actifs)
2. `uv run --with "pandas==3.0.0" pytest -m "not integration"` (plancher déclaré)
3. `uv run --with "pandas==2.3.3" pytest <nouveaux tests>` : les tests qui figent un
   comportement propre à pandas 3 doivent ÉCHOUER sous 2.3.3 ; liste ceux qui
   passent quand même (ils ne discriminent rien) et justifie-les ou renforce-les
4. `uv run mypy statflows/`, `uv run ruff check .`, `uv run ruff format --check .`

Termine par un résumé (risques confirmés / écartés, défauts signalés non corrigés),
puis crée un commit (sans push) dont le titre est
`chore(deps)!: require pandas >= 3.0` et dont le corps explique en deux ou trois
lignes que pandas 2 n'est plus supporté.
```

---

## P2 — Couvrir l'orchestrateur `core/download.py` par des tests unitaires

**Modèle :** Opus 5.5 · **Effort :** high · **Plan mode :** Oui

```text
Contexte : dépôt Python `statflows`. `statflows/core/download.py` (≈1 400 lignes)
contient `SDMXDownloader` / `download_updates`, l'orchestrateur incrémental
fournisseur → DuckLake (registre de dates, lots d'écriture, délai maximal, arrêt
propre sur SIGTERM, rapports). Sa couverture par les tests UNITAIRES est de 40 %
(91 % seulement grâce aux tests d'intégration sur catalogue DuckLake fichier) ;
`download_updates` n'est couvert par aucun test. Conventions : commentaires en
français, docstrings Google en anglais (docstring de module « Frozen behaviour:
… »), séparateurs `# ───` comme dans les tests existants, Conventional Commits.

Lis : `statflows/core/download.py`, `tests/utils/fake_sdmx.py` (FakeClient,
FakeQuery, FakeClock, make_queries), `tests/utils/registries.py`,
`tests/unit/core/test_download.py`, `tests/unit/core/test_download_options.py`,
`tests/integration/core/test_download_buffering.py` (pour NE PAS dupliquer ce qu'il
couvre déjà), et les sections 2.1 et 2.3 (lot B1) de `ARCHITECTURE_REFERENCE.md`
(document de travail : ne le cite jamais dans le code, les commentaires, les
docstrings ni le message de commit).

Branche : la branche courante (pas `main`).

Contraintes :
- Tests unitaires purs : pas de réseau (un garde-fou `autouse` l'interdit déjà sous
  `tests/unit/`), pas d'extension DuckDB, pas de S3. Le module importe `duckdb` et
  `dt_ducklake_manager` : commence chaque fichier par
  `pytest.importorskip("dt_ducklake_manager", reason=...)` comme les fichiers voisins.
- Simule la persistance : `monkeypatch` de `SDMXDownloader._connect` (renvoie un
  `MagicMock`) et de `statflows.core.download.write_dataframe` (enregistre les
  appels et renvoie True/False) ; connecteur = objet minimal exposant ce que le
  constructeur lit (vérifie dans le code). Registres JSON dans `tmp_path`.
- SIGTERM : appelle `_handle_sigterm` directement depuis `FakeClient(on_fetch=...)`,
  n'envoie jamais de vrai signal (la suite tourne aussi sous Windows).
- Teste des comportements observables (DownloadReport / QueryReport, contenu du
  registre écrit, appels à write_dataframe, logs via `caplog`) plutôt que des
  attributs privés, sauf pour les helpers privés déjà testés de cette façon.
- Si un test révèle un défaut, NE le fige PAS : marque-le
  `pytest.mark.xfail(strict=True, reason=...)` et décris-le dans le résumé final.
- Aucune modification du code de production.

Cas à couvrir (au minimum) : requête unique non itérable ; priorisation (jamais
téléchargées d'abord, puis plus anciennes) ; échec d'une requête (rapport en échec,
date non mise à jour, run poursuivi) ; `max_runtime` atteint ; SIGTERM pendant et
hors fenêtre interruptible, restauration du gestionnaire, `run()` hors thread
principal ; rappel `on_query_complete` qui lève ; `registry_shard_key` ; échec de
flush du registre retenté au flush suivant ; lot avec doublons de clé primaire
(dernier gagnant + avertissement) ; lot vide ; seuils `write_batch_rows` et
`write_batch_queries` ; `bucket` + `storage_options` → `connect` sur Loader/Saver
(mocks) ; agrégation des `HttpStats` d'un client et de ses clients HTTP composés ;
`download_updates` transmet tous ses arguments à `SDMXDownloader` puis appelle
`run`.

Commence en plan mode : présente la liste des tests (nom + comportement vérifié +
lignes visées) et la stratégie de simulation du connecteur avant d'écrire.

Objectif : `core/download.py` ≥ 90 % en tests unitaires seuls, mesuré par
`uv run pytest -m "not integration" --cov=statflows.core.download --cov-report=term-missing`.

Vérifications : `uv sync --all-extras`, `uv run pytest` (suite complète),
`uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy statflows/`.

Termine par un résumé (couverture avant/après, défauts éventuels marqués xfail),
puis crée un commit (sans push) :
`test(core): cover SDMXDownloader orchestration with unit tests`.
```

---

## P3 — Couvrir les chemins S3 de `storage/` sans identifiants AWS

**Modèle :** Sonnet 5.5 · **Effort :** medium · **Plan mode :** Non

```text
Contexte : dépôt Python `statflows`. Le sous-paquet `statflows/storage/` gère des
registres JSON en local ou sur S3 (`boto3` ou `s3fs`, extra « s3 »). Les chemins S3
ne sont couverts que par des tests d'intégration `moto` ; en tests unitaires :
`storage/_connection.py` 52 %, `storage/json/loader.py` 51 %,
`storage/json/s3/loader.py` et `saver.py` 32 %, `storage/__init__.py` 70 %.
Conventions : commentaires en français, docstrings Google en anglais (docstring de
module « Frozen behaviour: … »), Conventional Commits.

Lis : tout `statflows/storage/` (hors `ducklake/`), `tests/unit/storage/json/`,
`tests/integration/storage/json/test_s3.py` (pour ne pas le dupliquer), et les
sections 2.1 et 2.3 (lot B2) de `ARCHITECTURE_REFERENCE.md` (document de travail :
ne le cite jamais dans le code, les commentaires, les docstrings ni le message de
commit).

Branche : la branche courante (pas `main`).

Contraintes :
- Pas de `moto` dans `tests/unit/` : remplace `boto3.client` / `s3fs.S3FileSystem`
  par des `unittest.mock` (patch à l'endroit où ils sont importés, c'est-à-dire
  dans le corps de `S3Connection._connect`). Aucun identifiant réel ; les variables
  `AWS_*` sont retirées par une fixture `autouse` existante — pose celles dont un
  test a besoin avec `monkeypatch.setenv`.
- Les tests qui requièrent `boto3` ou `s3fs` commencent par `pytest.importorskip` :
  le job CI `bare-imports` exécute `tests/unit` sans extras. L'absence d'un extra se
  simule avec `monkeypatch.setitem(sys.modules, "boto3", None)`.
- Aucune modification du code de production. Un défaut découvert est signalé dans
  le résumé, pas figé (au besoin `xfail(strict=True)`).

Cas à couvrir : `S3Connection` — paquet invalide ; `boto3` avec arguments explicites
puis repli sur l'environnement ; variable manquante (KeyError actuel) ; branche
`s3fs` ; `ImportError` avec le message d'extra quand le paquet manque ;
`statflows.storage.__getattr__` (succès paresseux et AttributeError) ; chargeur et
sauveur S3 (clé sans `.json`, connexion paresseuse au premier appel, objet absent
avec et sans `missing_ok`, JSON invalide) ; `Loader.list_json` en S3 (paginateur
`boto3` à plusieurs pages, fichiers masqués et non JSON filtrés ; `s3fs.glob`) ;
`storage/json/saver.py` ligne ~129.

Objectif : chacun de ces modules ≥ 95 % en tests unitaires seuls :
`uv run pytest -m "not integration" --cov=statflows.storage --cov-report=term-missing`.

Vérifications :
- `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`,
  `uv run ruff format --check .`, `uv run mypy statflows/`
- installation nue : `uv venv .venv-bare`, `uv pip install --python .venv-bare . pytest`,
  puis `.venv-bare/Scripts/python -m pytest tests/unit` (Windows ; `bin/` ailleurs) ;
  supprime `.venv-bare` ensuite.

Termine par un résumé (couverture avant/après), puis crée un commit (sans push) :
`test(storage): cover S3 code paths without AWS credentials`.
```

---

## P4 — Combler les petits trous de couverture (clés d'identité, parsing, registres)

**Modèle :** Sonnet 5.5 · **Effort :** medium · **Plan mode :** Non

```text
Contexte : dépôt Python `statflows` (clients Eurostat / OECD / UN Comtrade / UNSD).
La couverture unitaire est bonne mais laisse des trous ciblés, dont un sensible :
`ComtradeQueryRequest.identity_key()` n'est testé nulle part, alors que cette clé
indexe le registre des dates de téléchargement (la modifier ferait re-télécharger
intégralement toutes les requêtes existantes). Conventions : commentaires en
français, docstrings Google en anglais, Conventional Commits.

Lis la section 2.3 (lot B3) de `ARCHITECTURE_REFERENCE.md` (document de travail :
ne le cite jamais dans le code, les commentaires, les docstrings ni le message de
commit) et les fichiers de tests existants correspondants.

Branche : la branche courante (pas `main`).

Lignes non couvertes à traiter (vérifie les numéros, ils peuvent avoir bougé) :
- `statflows/core/queries.py` 100-101, 118-119 : `get_dataflow_key` / `identity_key`
  par défaut (via une sous-classe concrète minimale ou un DTO existant qui ne les
  surcharge pas).
- `statflows/sources/comtrade/queries.py` 112, 127-147 : `get_dataflow_key` et
  `identity_key`. Fige les VALEURS EXACTES actuelles (chaînes littérales dans le
  test) pour 2-3 requêtes représentatives, et vérifie : champs `None` omis ;
  `format` sans effet sur la clé ; effet (ou non) de l'ordre des éléments d'une
  liste — constate le comportement de `core/sdmx.build_identity_key`, ne le change
  pas.
- `statflows/sources/eurostat/queries.py` 74, 87.
- `statflows/sources/comtrade/parsing.py` 68 (dataflow inconnu), 107-110 (catégorie
  `partner`, catégorie inconnue), 155 (repli de la colonne de libellé).
- `statflows/sources/comtrade/client.py` 167 (registre de structures injecté),
  1315-1316 (`_fetch_structure` depuis les paramètres).
- `statflows/sources/eurostat/client.py` 927 (`_fetch_structure` délègue à
  `get_dataflow_structure`, à vérifier par mock).
- `statflows/sources/unsd/client.py` 336-338 (`close`).
- `statflows/core/registry.py` 298 (`iter_registry_entries` avec `bucket` et
  `storage_options` : `connect` appelé — mock du Loader), 380, 399.
- Branches partielles listées par `--cov-report=term-missing` dans `core/client.py`,
  `core/sdmx.py`, `sources/oecd/*`, `sources/eurostat/parsing.py` : couvre celles qui
  correspondent à un comportement utile ; ignore les autres et dis lesquelles.

Contraintes : aucun réseau ni clé API (garde-fou `autouse` déjà en place) ; mocks
via `unittest.mock` comme dans les tests voisins ; aucune modification du code de
production ; un défaut découvert est signalé, pas figé.

Mesure : `uv run pytest -m "not integration" --cov --cov-report=term-missing`
avant et après.

Vérifications : `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`,
`uv run ruff format --check .`, `uv run mypy statflows/`.

Termine par un résumé (couverture avant/après, branches volontairement ignorées),
puis crée un commit (sans push) :
`test: cover query identity keys and remaining provider gaps`.
```

---

## P5 — Ajouter `ClientFactory` et étendre `build_client`

**Modèle :** Opus 5.5 · **Effort :** high · **Plan mode :** Oui

```text
Contexte : dépôt Python `statflows` (clients Eurostat / OECD / UN Comtrade / UNSD).
`statflows.core.factory.build_client(provider)` ne connaît que "eurostat" et "oecd"
et n'accepte aucune option. Un projet consommateur maintient sa propre fabrique
picklable (chemin pointé + options) pour construire les clients dans des nœuds de
pipeline et des sous-processus ; on veut une version générique et améliorée dans
`statflows`. Conventions : commentaires en français (style « # Verbe + objet » au-
dessus de chaque bloc, comme dans `core/factory.py`), docstrings Google en anglais
avec `Examples:` doctest, Conventional Commits. Une docstring ne doit pas faire
référence à des documents externes au dépôt publié.

Lis : `statflows/core/factory.py`, `statflows/__init__.py`,
`statflows/core/__init__.py`, les `__init__` des quatre clients
(`statflows/sources/*/client.py`), `statflows/core/sdmx.py` (`SDMXVersion`),
`SDMXQueryRequest.from_dict` dans `statflows/core/queries.py` (coercition d'enum
existante), `tests/unit/core/test_factory.py`, `tests/unit/test_package_imports.py`,
`docs/providers.md`, `mkdocs.yml`, `docs/api/core/build_client.md`, `CONTRIBUTING.md`
(règles de la doc API), et la section 3 de `ARCHITECTURE_REFERENCE.md` qui fixe
l'API cible (document de travail : ne le cite jamais dans le code, les commentaires,
les docstrings ni le message de commit).

Branche : la branche courante (pas `main`).

Commence en plan mode : confronte l'API de la section 3.3 au code réel (signatures
des constructeurs, annotations résolubles par `typing.get_type_hints`, risque de
cycle d'import) et présente-moi les écarts éventuels avant d'implémenter.

À implémenter dans `statflows/core/factory.py` :
1. Table `CLIENT_PATHS` alias → chemin pointé (lecture seule), import paresseux.
2. Helper privé d'import par chemin pointé (`"a.b.C"` ou `"a.b:C"`), erreurs
   explicites.
3. `ClientFactory(target, options=None, credentials=None)` :
   - `target` = alias connu ou chemin pointé ; résolution, import et vérification
     « appelable » dès la construction ;
   - validation des arguments à la construction (`inspect.signature(...).bind`) →
     `TypeError` immédiat sur option inconnue ;
   - coercition des chaînes en `Enum` quand l'annotation du paramètre est un `Enum`
     (y compris `X | None`) : par valeur, puis par nom insensible à la casse ; erreur
     listant les valeurs admises ;
   - `credentials` fusionnées aux options à l'appel, jamais affichées ; clé présente
     des deux côtés → `ValueError` ;
   - `__call__` renvoie un NOUVEAU client à chaque appel ;
   - `__repr__` affiche la cible et les options, et seulement le NOMBRE de
     credentials ; `__eq__` ; non hachable ;
   - picklable (état = chaînes et dictionnaires) ; documente qu'une option non
     picklable rend la fabrique non picklable.
4. `build_client(provider, **options)` : délègue à `ClientFactory`, accepte les 4
   alias (pas les chemins pointés), message `Unknown provider '<x>'` complété des
   alias connus ; surcharges `typing.overload` + `Literal` pour que
   `build_client("comtrade")` soit typé `ComtradeClient` (imports sous
   `TYPE_CHECKING`). Rétrocompatible avec les appels actuels.
5. Exporte `ClientFactory` et `build_client` depuis `statflows` (`__all__`), PAS
   depuis `statflows.core` (cycle d'import : `core.factory` importe `sources`).

Hors périmètre : ne modifie pas `SDMXDownloader` / `download_updates`, pas de cache
ni de pool de clients, pas de lecture de variables d'environnement dans la fabrique.

Tests dans `tests/unit/core/test_factory.py` (double de client défini dans
`tests/utils/`) : alias → bonne classe pour les 4 fournisseurs ; chemin pointé `.`
et `:` ; cible inconnue / non appelable ; option inconnue → TypeError à la
construction ; coercition d'enum (valeur, nom, casse, None conservé, invalide) ;
credentials transmises et absentes du repr ; conflit options/credentials ; nouvelle
instance à chaque appel ; aller-retour pickle puis appel ; égalité ; `build_client`
avec options et rétrocompatibilité ; mise à jour de `test_package_imports.py` si
`__all__` y est vérifié. Aucun réseau (garde-fou autouse déjà en place).

Documentation : `docs/api/core/ClientFactory.md` (`::: statflows.core.factory.ClientFactory`)
+ entrée `nav` dans `mkdocs.yml` ; section « Factories » de `docs/providers.md` avec
un exemple de configuration (YAML) → `ClientFactory` → `with factory() as client:` ;
mention dans le README (ne supprime pas les marqueurs `<!-- --8<-- ... -->`).

Vérifications : `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`,
`uv run ruff format --check .`, `uv run mypy statflows/`,
`uv run --group docs mkdocs build --strict`, et
`uv run pytest --doctest-modules statflows/core/factory.py` pour les exemples
ajoutés (pas `python -m doctest` : le module utilise des imports relatifs).

Termine par un résumé (API finale, écarts par rapport au plan), puis crée un commit
(sans push) : `feat(core): add ClientFactory and extend build_client to all providers`.
```

---

## P6 — Vérification finale, seuil de couverture en CI, nettoyage

**Modèle :** Sonnet 5.5 · **Effort :** medium · **Plan mode :** Non

```text
Contexte : dépôt Python `statflows`. La branche courante contient plusieurs commits :
tests hermétiques, passage à pandas >= 3.0, nouveaux tests unitaires
(`core/download.py`, `storage/`, petits trous), `ClientFactory`. Il faut tout
vérifier ensemble et verrouiller la couverture en CI. Conventions : Conventional
Commits ; ne modifie ni la version de `pyproject.toml` ni `CHANGELOG.md`. Lis
`.github/workflows/ci.yml`, `pyproject.toml` et la section 2.4 de
`ARCHITECTURE_REFERENCE.md` (document de travail : ne le cite jamais dans le code,
les commentaires, les docstrings ni le message de commit).

Branche : la branche courante (pas `main`). `git log main..HEAD --oneline` pour voir
les commits.

Tâche :
1. Exécute toute la chaîne et corrige ce qui casse (sans affaiblir un test) :
   `uv sync --all-extras` ; `uv run ruff check .` ; `uv run ruff format --check .` ;
   `uv run mypy statflows/` ; `uv run pytest -m "not integration" --cov --cov-report=term-missing` ;
   `uv run pytest -m integration --cov --cov-append --cov-report=term-missing` ;
   `uv run --group docs mkdocs build --strict` ; `uv build` puis
   `uvx twine check dist/*` ; installation nue (`uv venv .venv-bare`,
   `uv pip install --python .venv-bare . pytest`, import de `statflows`,
   `statflows.core`, `statflows.sources`, `statflows.storage`, puis
   `python -m pytest tests/unit` avec l'interpréteur de `.venv-bare` ; supprime
   `.venv-bare` et `dist/` ensuite).
2. Rapporte la couverture : unitaires seuls (cible ≥ 95 %, aucun module sous 85 %)
   et unitaires + intégration (cible ≥ 98 %). Si une cible n'est pas atteinte,
   n'écris pas de tests de remplissage : liste les modules en cause.
3. Dans `.github/workflows/ci.yml`, ajoute `--cov-fail-under=<seuil>` à l'étape
   d'INTÉGRATION du job `test` (celle qui fait `--cov-append` et produit le rapport
   final), pas à l'étape unitaire. Seuil : 95 si la couverture combinée mesurée
   est d'au moins 96 % ; sinon (couverture mesurée arrondie à l'entier inférieur)
   moins 1, en le signalant. Ajoute aussi
   `fail_under` dans `[tool.coverage.report]` uniquement si cela ne fait pas échouer
   l'étape unitaire (sinon, ne le mets pas et explique pourquoi).
4. Vérifie qu'aucun fichier du paquet, des tests ni de la doc publiée ne mentionne
   `ARCHITECTURE_REFERENCE.md` ou `PROMPTS_IMPLEMENTATION.md`
   (`git grep -n "ARCHITECTURE_REFERENCE\|PROMPTS_IMPLEMENTATION" -- statflows tests docs README.md mkdocs.yml`).
5. Ne supprime pas ces deux fichiers de travail toi-même : demande-moi si je veux
   un commit `chore: remove implementation working notes`.

Termine par un tableau récapitulatif (commande → résultat, couverture par module
sous 95 %), puis crée un commit (sans push) : `ci: enforce a minimum test coverage`.
Suggère ensuite de lancer `/code-review high` sur la branche.
```
