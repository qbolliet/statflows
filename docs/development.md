# Development

--8<-- "README.md:development"

## Documentation

The site is built with [MkDocs](https://www.mkdocs.org/) and the
[Material](https://squidfunnel.github.io/mkdocs-material/) theme; the API
reference is generated from the Google-style docstrings by
[mkdocstrings](https://mkdocstrings.github.io/). It is deployed to GitHub Pages
by the `docs.yml` workflow on every push to `main`.

```bash
uv sync --group docs
uv run mkdocs serve          # live preview on http://127.0.0.1:8000
uv run mkdocs build --strict # static build in site/ (fails on any warning)
```

## Language conventions

- **Code comments** are written in French.
- **Docstrings** (Google style) and the **README / documentation site** are
  written in English.

The API pages under `docs/api/` are one-liners (`::: statflows.core.client.APIClient`):
when a public symbol is added, create its page and list it in the `nav` of
`mkdocs.yml`.
