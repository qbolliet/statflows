# Contributing

## Prérequis

- [uv](https://docs.astral.sh/uv/) >= 0.5
- Python >= 3.13
- [pre-commit](https://pre-commit.com/)

## Installation de l'environnement

```bash
uv sync --all-extras      # groupe dev + extras s3 / ducklake
uv run pre-commit install # hooks pre-commit + commit-msg
```

## Commandes utiles

```bash
# Tests
uv run pytest                          # tous les tests
uv run pytest -m integration           # tests d'intégration uniquement
uv run pytest -m "not integration"     # tests unitaires uniquement

# Qualité de code
uv run ruff check .                    # linting
uv run ruff format .                   # formatage
uv run mypy statflows/                 # vérification des types

# Paquet
uv build && uvx twine check dist/*     # construction + validation des métadonnées
```

## Format des commits (Conventional Commits)

Chaque message doit suivre [Conventional Commits](https://www.conventionalcommits.org/),
vérifié par le hook `commit-msg` :

```
<type>[(<scope>)]: <description>
```

| Type       | Description                                 | Impact version |
|------------|---------------------------------------------|----------------|
| `feat`     | Nouvelle fonctionnalité                     | minor          |
| `fix`      | Correction de bug                           | patch          |
| `perf`     | Amélioration de performance                 | patch          |
| `refactor` | Refactoring sans changement de comportement | —              |
| `test`     | Ajout ou modification de tests              | —              |
| `docs`     | Documentation uniquement                    | —              |
| `style`    | Formatage                                   | —              |
| `chore`    | Maintenance (dépendances, CI...)            | —              |

`!` après le type ou `BREAKING CHANGE:` dans le footer → `major`
(tant que la version est `0.x`, release-please reste conforme à SemVer 0.x).

## Workflow de release

Entièrement automatisé par [release-please](https://github.com/googleapis/release-please) :

1. Merge de commits conventionnels sur `main`.
2. release-please ouvre/met à jour une **PR « release »** (version dans
   `pyproject.toml`, `CHANGELOG.md`).
3. Merge de cette PR → tag `vX.Y.Z` + release GitHub.
4. (Après activation, cf. [docs/PYPI_CHECKLIST.md](docs/PYPI_CHECKLIST.md)) la
   release GitHub déclenche `publish.yml` → PyPI.

> Ne modifiez pas à la main la version de `pyproject.toml`, le `CHANGELOG.md`
> ni `.release-please-manifest.json`.

## Données du paquet

Les fichiers `statflows/parameters/*.json` sont des **données d'exécution** lues
par les clients (rate limits, codes, etc.) : ils restent **dans** le paquet
`statflows/` pour être embarqués dans la wheel.
