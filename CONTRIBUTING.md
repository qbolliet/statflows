# Contributing

## Prerequisites

- [uv](https://docs.astral.sh/uv/) >= 0.5
- Python >= 3.13
- [pre-commit](https://pre-commit.com/)

## Setting up the environment

```bash
uv sync --all-extras      # dev group + s3 / ducklake extras
uv run pre-commit install # pre-commit + commit-msg hooks
```

## Useful commands

```bash
# Tests
uv run pytest                          # all tests
uv run pytest -m integration           # integration tests only
uv run pytest -m "not integration"     # unit tests only

# Code quality
uv run ruff check .                    # linting
uv run ruff format .                   # formatting
uv run mypy statflows/                 # type checking

# Package
uv build && uvx twine check dist/*     # build + metadata validation

# Documentation (MkDocs)
uv sync --group docs
uv run mkdocs serve                    # live preview on http://127.0.0.1:8000
uv run mkdocs build --strict           # build; fails on any warning
```

## Language conventions

- **Code comments** (`#`): in **French**.
- **Docstrings** (Google style), **README** and **documentation site**: in **English**.
- **CONTRIBUTING**: in **English**.

The API reference (`docs/api/`) is generated from the docstrings by
mkdocstrings: a newly added public symbol needs a one-line page
(`::: statflows.module.Symbol`) and an entry in the `nav` of `mkdocs.yml`.
The README is the single source of several pages of the site (through the
`<!-- --8<-- [start:...] -->` markers): do not remove them.

## Commit format (Conventional Commits)

Every message must follow [Conventional Commits](https://www.conventionalcommits.org/),
checked by the `commit-msg` hook:

```
<type>[(<scope>)]: <description>
```

| Type       | Description                          | Version impact |
|------------|--------------------------------------|----------------|
| `feat`     | New feature                          | minor          |
| `fix`      | Bug fix                              | patch          |
| `perf`     | Performance improvement              | patch          |
| `refactor` | Refactoring without behaviour change | —              |
| `test`     | Adding or changing tests             | —              |
| `docs`     | Documentation only                   | —              |
| `style`    | Formatting                           | —              |
| `chore`    | Maintenance (dependencies, CI...)    | —              |

A `!` after the type or `BREAKING CHANGE:` in the footer → `major`
(while the version is `0.x`, release-please stays compliant with SemVer 0.x).

## Release workflow

Fully automated by [release-please](https://github.com/googleapis/release-please):

1. Conventional commits are merged into `main`.
2. release-please opens / updates a **"release" PR** (version in
   `pyproject.toml`, `CHANGELOG.md`).
3. Merging that PR → `vX.Y.Z` tag + GitHub release.
4. (Once enabled, see [docs/PYPI_CHECKLIST.md](docs/PYPI_CHECKLIST.md)) the
   GitHub release triggers `publish.yml` → PyPI.

> Do not edit the `pyproject.toml` version, `CHANGELOG.md` or
> `.release-please-manifest.json` by hand.

## Package data

The `statflows/parameters/*.json` files are **runtime data** read by the clients
(rate limits, codes, etc.): they stay **inside** the `statflows/` package so that
they are bundled in the wheel.
