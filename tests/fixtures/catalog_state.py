"""Compact, diffable snapshots of live catalog objects for effect assertions.

A snapshot flattens a table into short strings so an expected effect reads like
the SQL that caused it: ``{"cols": {"+": ["point.w:double"]}}``. Columns are
listed in schema order as dotted paths (``element``/``key``/``value`` for
collection children), each rendered ``path:type[!][#doc][=default]`` where
``!`` marks a required field and nested containers render as their kind only.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, NoSuchViewError
from pyiceberg.types import ListType, MapType, NestedField, StructType

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.typedef import Identifier

__all__ = ["ANY", "diff", "namespace_snapshot", "table_snapshot", "view_snapshot"]


class _Any:
    def __eq__(self, other: object) -> bool:
        return True

    def __repr__(self) -> str:
        return "ANY"


# Matches any value in an expected effect (e.g. the catalog-assigned old location).
ANY = _Any()

# Lakekeeper stamps these on every namespace; they say nothing about the SQL.
_NAMESPACE_NOISE = {"namespace_id", "location"}


def _field(path: str, f: NestedField) -> list[str]:
    t = f.field_type
    kind = "struct" if isinstance(t, StructType) else "list" if isinstance(t, ListType) else None
    kind = kind or ("map" if isinstance(t, MapType) else str(t))
    s = f"{path}:{kind}{'!' if f.required else ''}"
    if f.doc:
        s += f"#{f.doc}"
    if f.write_default is not None:
        s += f"={f.write_default}"
    out = [s]
    if isinstance(t, StructType):
        for child in t.fields:
            out += _field(f"{path}.{child.name}", child)
    elif isinstance(t, ListType):
        out += _field(f"{path}.element", t.element_field)
    elif isinstance(t, MapType):
        out += _field(f"{path}.key", t.key_field)
        out += _field(f"{path}.value", t.value_field)
    return out


def table_snapshot(
    catalog: Catalog, ident: Identifier, catalog_defaults: Mapping[str, object] | None = None
) -> dict[str, Any] | None:
    """Leaves out ``catalog_defaults``, properties the catalog stamps on new tables."""
    try:
        table = catalog.load_table(ident)
    except NoSuchTableError:
        return None
    schema = table.schema()
    cols: list[str] = []
    for f in schema.fields:
        cols += _field(f.name, f)
    snap = table.current_snapshot()
    return {
        "fv": table.format_version,
        "cols": cols,
        "ident": sorted(schema.identifier_field_names()),
        "spec": [
            f"{schema.find_column_name(p.source_id)}:{p.transform}:{p.name}"
            for p in table.spec().fields
        ],
        "sort": [
            f"{schema.find_column_name(s.source_id)}:{s.transform}:{s.direction.value}:"
            f"{s.null_order.value}"
            for s in table.sort_order().fields
        ],
        "props": {
            k: v for k, v in table.properties.items() if (catalog_defaults or {}).get(k) != v
        },
        "loc": table.location(),
        "rows": int(snap.summary["total-records"]) if snap and snap.summary else 0,
    }


def view_snapshot(catalog: Catalog, ident: Identifier) -> dict[str, Any] | None:
    try:
        view = catalog.load_view(ident)
    except NoSuchViewError:
        return None
    meta = view.metadata
    version = next(v for v in meta.versions if v.version_id == meta.current_version_id)
    schema = next(s for s in meta.schemas if s.schema_id == version.schema_id)
    cols: list[str] = []
    for f in schema.fields:
        cols += _field(f.name, f)
    return {
        "cols": cols,
        "sql": {r.root.dialect: r.root.sql for r in version.representations},
        "props": dict(meta.properties),
        "versions": len(meta.versions),
    }


def namespace_snapshot(catalog: Catalog, namespace: str) -> dict[str, Any] | None:
    try:
        props = catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        return None
    return {"props": {k: v for k, v in props.items() if k not in _NAMESPACE_NOISE}}


def _path(item: str) -> str:
    return item.split(":", 1)[0]


def diff(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, Any]:
    """Describe how *after* differs from *before*; ``{}`` when nothing changed.

    Lists diff as ``{"-": [...], "+": [...]}`` where each added item is suffixed
    with its position (``" first"`` or ``" after <path>"``), plus ``"order"``
    (the full new list) when the surviving items were reordered. Dicts diff per
    key as ``[before, after]`` (None = absent). Removal reports ``{"exists":
    False}``; creation reports ``{"exists": True, "state": after}``.
    """
    if before is None or after is None:
        if after is None:
            return {} if before is None else {"exists": False}
        return {"exists": True, "state": after}
    out: dict[str, Any] = {}
    for key, a in before.items():
        b = after[key]
        if a == b:
            continue
        if isinstance(a, list):
            out[key] = _list_diff(a, b)
        elif isinstance(a, dict):
            out[key] = {
                k: [a.get(k), b.get(k)] for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)
            }
        else:
            out[key] = [a, b]
    return out


def _list_diff(a: list[str], b: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    removed = [x for x in a if x not in b]
    added = [
        f"{x} after {_path(b[i - 1])}" if i else f"{x} first" for i, x in enumerate(b) if x not in a
    ]
    if removed or added:
        out = {"-": removed, "+": added}
    if [x for x in a if x in b] != [x for x in b if x in a]:
        out["order"] = b
    return out
