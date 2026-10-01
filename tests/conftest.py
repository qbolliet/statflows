"""Non-regression contract for the extraction of ``statflows`` from ``trade-analysis``.

Layout:

- ``tests/unit/`` — unit tests, mirroring the package layout
  (``core/``, ``sources/``, ``storage/json/``, ``storage/ducklake/``). No
  external service: local I/O and pure logic.
- ``tests/integration/`` — end-to-end against simulated services: S3 through
  ``moto``, file-based DuckLake catalog. Fixtures live in
  ``tests/integration/conftest.py``; every test there is automatically marked
  ``integration``.
- ``tests/utils/`` — shared test functions and classes (import
  ``from tests.utils.<module> import ...``).

These tests freeze the behaviour of the migrated code (JSON registries, DuckLake
helpers, pure helpers of the orchestrator) and must pass identically on both
sides of the extraction. No production code is modified by the tests.
"""
