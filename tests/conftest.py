"""Contrat de non-régression de l'extraction de ``statflows`` depuis ``trade-analysis``.

Arborescence :

- ``tests/unit/`` — tests unitaires, calqués sur l'arborescence du package
  (``core/``, ``sources/``, ``storage/json/``, ``storage/ducklake/``). Aucun
  service externe : entrées/sorties locales et logique pure.
- ``tests/integration/`` — bout-en-bout contre des services simulés : S3 via
  ``moto``, catalogue DuckLake sur fichier. Fixtures dans
  ``tests/integration/conftest.py`` ; chaque test y est automatiquement marqué
  ``integration``.
- ``tests/utils/`` — fonctions et classes de test partagées (import
  ``from tests.utils.<module> import ...``).

Ces tests figent le comportement du code migré (registres JSON, helpers DuckLake,
helpers purs de l'orchestrateur) et doivent passer à l'identique de part et
d'autre de l'extraction. Aucun code de production n'est modifié par les tests.
"""
