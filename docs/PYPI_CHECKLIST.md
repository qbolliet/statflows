# Checklist avant publication sur PyPI

L'infrastructure est en place (CI, release-please, workflow `publish.yml`
désactivé). Reste à faire pour publier :

## Bloquants

- [ ] **Dépendance `dt-ducklake-manager`** : elle n'est résolue que via
  `[tool.uv.sources]` (git, tag), ce qui n'est **pas publié** dans les
  métadonnées de la wheel. PyPI refuse/ne résoudra pas une dépendance git
  directe, et `pip install statflows[ducklake]` échouerait. → Publier d'abord
  `dt-ducklake-manager` sur PyPI, puis retirer l'entrée `[tool.uv.sources]`.
  (Alternative : retirer l'extra `ducklake`/`all` de la première publication.)
- [ ] **Nom `statflows` disponible sur PyPI** (vérifier sur https://pypi.org/project/statflows/,
  tester d'abord sur TestPyPI).
- [ ] **Trusted Publisher PyPI** : sur pypi.org → *Publishing* → ajouter
  le dépôt `qbolliet/statflows`, workflow `publish.yml`, environnement `pypi`.
- [ ] **Environnement GitHub `pypi`** (Settings → Environments), idéalement avec
  approbation manuelle requise.
- [ ] **Activer la publication** : variable de dépôt `PUBLISH_TO_PYPI=true`.
- [ ] **Secrets GitHub** : `RELEASE_PLEASE_TOKEN` (PAT, pour que la PR de
  release déclenche la CI) et `CODECOV_TOKEN`.
- [ ] **Branch protection sur `main`** : exiger les jobs CI (lint, test,
  bare-imports, build).

## Qualité à finir

- [ ] Test d'une répétition : publier d'abord sur **TestPyPI**
  (`uv build && uv publish --publish-url https://test.pypi.org/legacy/`),
  puis `uv pip install --index-url https://test.pypi.org/simple/ statflows`.
- [ ] Mettre à jour la section *Installation* du `README.md`
  (`pip install statflows`, extras) et retirer « pas encore publié sur PyPI ».
- [ ] Vérifier les classifiers / mots-clés / URLs dans `pyproject.toml`.
- [ ] Optionnel : documentation (mkdocs + GitHub Pages comme dt-ducklake-manager),
  matrice de tests multi-OS, `SECURITY.md`.
- [ ] Optionnel : `uv.lock` garde l'ancienne version du paquet après un bump
  release-please ; lancer `uv lock` dans la PR de release si besoin.

## Premier release

`release-please` démarre à `0.1.0` (manifest) et ne considère que les commits
postérieurs à `bootstrap-sha`. Merger la PR de release → tag `v0.1.0`/suivant,
puis la publication part automatiquement si `PUBLISH_TO_PYPI=true`.
