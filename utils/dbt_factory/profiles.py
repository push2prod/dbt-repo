"""Cosmos profile mappings this repo adds to the ones Cosmos ships.

Referenced by class path from infra/dbt_bundle.yml `profile.mapping.class`
(e.g. ``utils.dbt_factory.profiles.TrinoNoAuthProfileMapping``); the factory
imports it like a Cosmos one. Airflow finds `utils` on PYTHONPATH.
"""

from __future__ import annotations

from cosmos.profiles.trino.base import TrinoBaseProfileMapping


class TrinoNoAuthProfileMapping(TrinoBaseProfileMapping):
    """An Airflow Trino connection to a dbt-trino profile with ``method: none`` (no authentication).

    Cosmos 1.10 only maps the LDAP (password), JWT and certificate methods, and
    LDAP fails on a connection without a password ("cannot find value for secret
    field password"). This one needs no secret: from the connection it takes host,
    port and login (dbt's ``user``); ``database`` (the catalog) and ``schema``
    come from ``profile_args``, as for every Trino mapping. dbt-trino's
    ``http_scheme`` defaults to http; pass ``http_scheme: https`` in
    ``profile_args`` if the server has TLS.
    """

    dbt_profile_method: str = "none"
    # Inherited: required_fields = host, database, schema, port + user; no secret_fields.
