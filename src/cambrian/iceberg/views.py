"""Iceberg view writes the REST catalog supports but PyIceberg 0.12 doesn't expose.

PyIceberg can create, load, list and drop views; replacing a view (adding a
version), changing its properties and renaming it are plain REST calls with
no client method (#27). They go through the catalog's own HTTP session so auth,
headers and TLS settings are shared, and are isolated here like the raw
table updates in ``txn.py``. Request bodies follow the REST OpenAPI spec
(``CommitViewRequest``, ``RenameTableRequest``).
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import assign_fresh_schema_ids

from cambrian import __version__
from cambrian.errors import DispatchError

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Identifier
    from pyiceberg.view import View

__all__ = [
    "VIEW_SUMMARY",
    "rename_view",
    "replace_view",
    "require_rest",
    "update_view_properties",
]

VIEW_SUMMARY = {"engine-name": "cambrian", "engine-version": __version__}


def require_rest(catalog: Catalog) -> RestCatalog:
    if not isinstance(catalog, RestCatalog):
        raise DispatchError(
            f"Iceberg views need a REST catalog; [catalog] is {type(catalog).__name__}"
        )
    return catalog


def replace_view(
    catalog: RestCatalog,
    view: View,
    schema: Schema,
    representations: dict[str, str],
    default_namespace: Identifier,
    properties: dict[str, str],
) -> None:
    """Add a new current version (and schema) to an existing view in one commit."""
    meta = view.metadata
    # Field ids must be unique across nested types too; create_view does this
    # itself, the raw commit doesn't.
    fresh = assign_fresh_schema_ids(schema)
    updates: list[dict[str, Any]] = []
    # Java catalogs dedupe an identical add-schema, which leaves -1 ("the
    # schema added here") dangling, so an unchanged schema is referenced by id.
    existing = next((s for s in meta.schemas if s.as_struct() == fresh.as_struct()), None)
    if existing is None:
        schema_json = json.loads(fresh.model_dump_json(by_alias=True))
        schema_json["schema-id"] = max(s.schema_id for s in meta.schemas) + 1
        updates.append({"action": "add-schema", "schema": schema_json})
    version = {
        "version-id": max(v.version_id for v in meta.versions) + 1,
        "schema-id": -1 if existing is None else existing.schema_id,
        "timestamp-ms": int(time.time() * 1000),
        "summary": VIEW_SUMMARY,
        "representations": [
            {"type": "sql", "sql": sql, "dialect": dialect}
            for dialect, sql in representations.items()
        ],
        "default-namespace": list(default_namespace),
    }
    updates += [
        {"action": "add-view-version", "view-version": version},
        {"action": "set-current-view-version", "view-version-id": -1},
    ]
    if properties:
        updates.append({"action": "set-properties", "updates": properties})
    _commit(catalog, view, updates)


def update_view_properties(
    catalog: RestCatalog, view: View, *, set_: dict[str, str], remove: list[str]
) -> None:
    updates: list[dict[str, Any]] = []
    if set_:
        updates.append({"action": "set-properties", "updates": set_})
    if remove:
        updates.append({"action": "remove-properties", "removals": remove})
    if updates:
        _commit(catalog, view, updates)


def rename_view(catalog: RestCatalog, source: Identifier, target: Identifier) -> None:
    body = {
        "source": {"namespace": list(source[:-1]), "name": source[-1]},
        "destination": {"namespace": list(target[:-1]), "name": target[-1]},
    }
    response = catalog._session.post(catalog.url("views/rename"), data=json.dumps(body))
    response.raise_for_status()


def _commit(catalog: RestCatalog, view: View, updates: list[dict[str, Any]]) -> None:
    ident = tuple(view.name())
    body = {
        "identifier": {"namespace": list(ident[:-1]), "name": ident[-1]},
        "requirements": [{"type": "assert-view-uuid", "uuid": str(view.metadata.view_uuid)}],
        "updates": updates,
    }
    url = catalog.url(
        "namespaces/{namespace}/views/{view}",
        namespace=catalog._encode_namespace_path(ident[:-1]),
        view=ident[-1],
    )
    response = catalog._session.post(url, data=json.dumps(body))
    if not response.ok:
        raise DispatchError(
            f"view commit for {'.'.join(ident)} failed: HTTP {response.status_code} {response.text}"
        )
