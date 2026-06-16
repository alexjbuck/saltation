"""Sidecar version detection and forward-only self-migrations.

``ensure_current`` is the single entry point. It:

1. checks for the ``<sidecar_namespace>.version`` table;
2. reads the persisted version integer;
3. runs any pending self-migrations from ``SELF_MIGRATIONS`` forward;
4. returns the resulting :class:`SidecarState`.

If the sidecar has never been initialised, ``ensure_current`` itself decides
how to proceed: when ``allow_read_only=False`` (i.e. ``cambrian init``) the
v0→v1 migration is run; otherwise :class:`NotInitializedError` is raised so
read-only commands can print a clear hint.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from cambrian.errors import NotInitializedError, SidecarVersionAheadError
from cambrian.sidecar.bootstrap import (
    ensure_events_table,
    ensure_namespace,
    ensure_table_states_table,
    ensure_version_table,
)
from cambrian.sidecar.schema import (
    CAMBRIAN_SIDECAR_VERSION,
    SELF_MIGRATIONS,
    VERSION_TABLE,
)

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

__all__ = ["SidecarState", "ensure_current"]


@dataclass(frozen=True)
class SidecarState:
    """Resolved state after ``ensure_current`` finishes.

    ``is_version_ahead`` is only ever ``True`` when the caller passed
    ``allow_read_only=True`` and the persisted version exceeded what this
    binary understands — read-only commands surface that as a warning.
    """

    version: int
    is_version_ahead: bool
    sidecar_namespace: str


def _version_table_exists(catalog: Catalog, namespace: str) -> bool:
    return catalog.table_exists((namespace, VERSION_TABLE))


def _read_version(catalog: Catalog, namespace: str) -> int:
    """Return the single integer stored in ``<namespace>.version``.

    The table is append-only; in steady state it holds exactly one row. If a
    historical migration appended additional rows we take the max (highest
    version reached so far) — defensive against a future where we'd record
    every bump as a row instead of overwriting.
    """
    table = catalog.load_table((namespace, VERSION_TABLE))
    arrow = table.scan().to_arrow()
    if arrow.num_rows == 0:
        # Should never happen post-bootstrap, but treat empty as "needs v1".
        return 0
    return int(max(arrow.column("version").to_pylist()))


# PyIceberg requires the PyArrow input schema's nullability to match the
# Iceberg schema's ``required`` flag exactly, so we declare an explicit
# non-nullable schema rather than relying on pa.array's default (nullable).
_VERSION_PA_SCHEMA = pa.schema([pa.field("version", pa.int64(), nullable=False)])


def _write_version(catalog: Catalog, namespace: str, version: int) -> None:
    """Append a new version row. Caller is responsible for ordering."""
    table = catalog.load_table((namespace, VERSION_TABLE))
    arrow = pa.table(
        {"version": pa.array([version], type=pa.int64())},
        schema=_VERSION_PA_SCHEMA,
    )
    table.append(arrow)


def _create_initial_sidecar(catalog: Catalog, namespace: str) -> None:
    """v0 → v1 migration: namespace + three tables + ``version=1`` row.

    Exposed at module scope (rather than nested inside ``ensure_current``)
    so :mod:`cambrian.sidecar.schema` can reference it as the first entry of
    ``SELF_MIGRATIONS``.
    """
    ensure_namespace(catalog, namespace)
    ensure_events_table(catalog, namespace)
    ensure_table_states_table(catalog, namespace)
    ensure_version_table(catalog, namespace)
    _write_version(catalog, namespace, 1)


def ensure_current(catalog: Catalog, namespace: str, *, allow_read_only: bool) -> SidecarState:
    """Bring the sidecar to ``CAMBRIAN_SIDECAR_VERSION`` and return the state.

    Behaviour by initial state:

    - **No version table** + ``allow_read_only=True``: raise
      :class:`NotInitializedError`. Read-only commands surface the hint.
    - **No version table** + ``allow_read_only=False``: run all self-migrations
      from v0 forward (i.e. bootstrap).
    - **Version equals target**: no-op, return state.
    - **Version below target**: apply pending self-migrations in order.
    - **Version above target** + ``allow_read_only=True``: return state with
      ``is_version_ahead=True``; caller proceeds read-only.
    - **Version above target** + ``allow_read_only=False``: raise
      :class:`SidecarVersionAheadError`.
    """
    initialized = _version_table_exists(catalog, namespace)

    if not initialized:
        if allow_read_only:
            raise NotInitializedError()
        # Bootstrap from scratch by running every self-migration in order.
        # SELF_MIGRATIONS[0] handles namespace + table creation + version=1.
        for migration in SELF_MIGRATIONS:
            migration(catalog, namespace)
        return SidecarState(
            version=CAMBRIAN_SIDECAR_VERSION,
            is_version_ahead=False,
            sidecar_namespace=namespace,
        )

    current = _read_version(catalog, namespace)

    if current == CAMBRIAN_SIDECAR_VERSION:
        return SidecarState(
            version=current,
            is_version_ahead=False,
            sidecar_namespace=namespace,
        )

    if current > CAMBRIAN_SIDECAR_VERSION:
        if allow_read_only:
            return SidecarState(
                version=current,
                is_version_ahead=True,
                sidecar_namespace=namespace,
            )
        raise SidecarVersionAheadError(current, CAMBRIAN_SIDECAR_VERSION)

    # current < CAMBRIAN_SIDECAR_VERSION: run pending migrations forward.
    for migration in SELF_MIGRATIONS[current:]:
        migration(catalog, namespace)
        # Each migration is responsible for bumping the version row.
    final = _read_version(catalog, namespace)
    return SidecarState(
        version=final,
        is_version_ahead=False,
        sidecar_namespace=namespace,
    )
