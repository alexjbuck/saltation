"""Idempotent creators for the ``_cambrian`` namespace and its three tables.

Every helper here is safe to call against an already-bootstrapped catalog —
they either route through ``*_if_not_exists`` or swallow the corresponding
"already exists" PyIceberg exceptions. The self-migration in
:mod:`cambrian.sidecar.schema` calls these in sequence.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
)

from cambrian.sidecar.schema import (
    EVENTS_SCHEMA,
    EVENTS_TABLE,
    TABLE_STATES_SCHEMA,
    TABLE_STATES_TABLE,
    VERSION_SCHEMA,
    VERSION_TABLE,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table

__all__ = [
    "ensure_events_table",
    "ensure_namespace",
    "ensure_table_states_table",
    "ensure_version_table",
]


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    """Create the sidecar namespace if it doesn't already exist."""
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        return


def _ensure_table(catalog: Catalog, namespace: str, table_name: str, schema: Schema) -> Table:
    identifier = (namespace, table_name)
    try:
        return catalog.create_table(identifier=identifier, schema=schema)
    except TableAlreadyExistsError:
        return catalog.load_table(identifier)


def ensure_events_table(catalog: Catalog, namespace: str) -> Table:
    """Create ``<namespace>.events`` if absent; return the live table."""
    return _ensure_table(catalog, namespace, EVENTS_TABLE, EVENTS_SCHEMA)


def ensure_table_states_table(catalog: Catalog, namespace: str) -> Table:
    """Create ``<namespace>.table_states`` if absent; return the live table."""
    return _ensure_table(catalog, namespace, TABLE_STATES_TABLE, TABLE_STATES_SCHEMA)


def ensure_version_table(catalog: Catalog, namespace: str) -> Table:
    """Create ``<namespace>.version`` if absent; return the live table.

    The first row (``version = 1``) is written by the v0→v1 self-migration,
    not by this helper — keeping the schema/data steps separate makes the
    self-migration code obvious.
    """
    return _ensure_table(catalog, namespace, VERSION_TABLE, VERSION_SCHEMA)
