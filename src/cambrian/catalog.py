"""PyIceberg catalog factory.

Thin passthrough wrapper around :func:`pyiceberg.catalog.load_catalog`. The
``[catalog]`` table in ``cambrian.toml`` is forwarded verbatim — we don't
rename, transform, or omit any keys. The catalog name we hand to PyIceberg
is hardcoded to ``"cambrian"`` because PyIceberg only uses that name for
its own config-file lookup (``~/.pyiceberg.yaml``/``PYICEBERG_CATALOG_*``),
which we bypass by passing properties directly.

Note: contrary to a "lazy constructor" assumption, PyIceberg's REST
``Catalog.__init__`` issues a ``GET /v1/config`` request eagerly. Callers
that want to validate config *without* a live catalog should stick to
:func:`cambrian.config.load_config`. ``cambrian config show`` and
``cambrian config check`` deliberately never call this function.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.catalog import load_catalog as _pyiceberg_load_catalog

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from cambrian.config import CambrianConfig

__all__ = ["CATALOG_NAME", "load_catalog"]

# Pyiceberg uses this name only for its config-file lookup, which we bypass.
CATALOG_NAME = "cambrian"


def load_catalog(cfg: CambrianConfig) -> Catalog:
    """Build a PyIceberg :class:`~pyiceberg.catalog.Catalog` from *cfg*.

    All fields in ``cfg.catalog`` (including extras like ``token``,
    ``credential``, ``warehouse``, …) are forwarded as kwargs to
    :func:`pyiceberg.catalog.load_catalog`.

    For REST catalogs this triggers an immediate ``GET /v1/config`` request;
    other backends (SQL, in-memory) construct without I/O. Treat any call
    here as "we expect the catalog to be reachable".
    """
    properties = cfg.catalog.model_dump()
    return _pyiceberg_load_catalog(CATALOG_NAME, **properties)
