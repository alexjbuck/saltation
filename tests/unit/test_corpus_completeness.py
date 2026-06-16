"""Completeness cross-check: corpus must cover the supported-feature surface.

``test_corpus_coverage`` proves every *corpus* entry parses + dispatches as its
contract says. This file proves the converse: every *supported feature* the
dispatcher accepts has at least one positive corpus entry exercising it. The
two together pin the corpus to the implementation — adding a new supported
construct (a new type-map key, a new transform, a new ALTER action node, a new
top-level statement branch) without a corpus entry FAILS here.

The supported-feature sets are introspected from ``dispatch.py`` wherever the
code exposes them as data (``_SQLGLOT_TYPE_TO_ICEBERG`` keys, the custom AST
node classes routed in ``_dispatch_alter_action``). Where the surface is only
expressed as control flow (the transform name branches, the top-level
``dispatch`` branches), we maintain an explicit feature set in this file but
guard it against drift: a sentinel assertion checks the introspectable subset
still matches, and the per-feature mapping must resolve to a live corpus id —
so a stale mapping or a stale feature list both surface as failures.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable

import pytest
from sqlglot import expressions as exp

from cambrian.sql import dispatch as dispatch_mod
from cambrian.sql.ast import (
    AddPartitionField,
    AlterColumnPosition,
    DropIdentifierFields,
    DropPartitionField,
    ReplacePartitionField,
    SetIdentifierFields,
    UnsetTblProperties,
    WriteDistribution,
    WriteOrderedBy,
)
from cambrian.sql.dispatch import _SQLGLOT_TYPE_TO_ICEBERG
from tests.fixtures.iceberg_corpus import positives


def _positive_ids() -> set[str]:
    return {e.id for e in positives()}


def _assert_covered(feature: str, id_substrings: tuple[str, ...]) -> None:
    """Fail unless some positive corpus id contains one of *id_substrings*."""
    ids = _positive_ids()
    if not any(any(sub in cid for cid in ids) for sub in id_substrings):
        pytest.fail(
            f"supported feature {feature!r} has no positive corpus entry "
            f"(looked for an id containing one of {id_substrings})"
        )


# ---------------------------------------------------------------------------
# Column types: every key in _SQLGLOT_TYPE_TO_ICEBERG + the 3 composites.
# ---------------------------------------------------------------------------

# Map each supported sqlglot DataType.Type to the corpus-id substrings that
# exercise it. Derived from the implementation's type map; the test below
# proves this mapping's keys equal the live key set, so a new type-map entry
# without an entry here fails immediately.
_PRIMITIVE_TYPE_COVERAGE: dict[exp.DataType.Type, tuple[str, ...]] = {
    exp.DataType.Type.BOOLEAN: ("ct_all_primitives",),
    exp.DataType.Type.TINYINT: ("ct_all_primitives",),
    exp.DataType.Type.SMALLINT: ("ct_all_primitives",),
    exp.DataType.Type.INT: ("ct_all_primitives", "add_column"),
    exp.DataType.Type.BIGINT: ("ct_simple", "ct_all_primitives"),
    exp.DataType.Type.FLOAT: ("ct_all_primitives",),
    exp.DataType.Type.DOUBLE: ("ct_all_primitives", "ct_struct", "alter_column_type"),
    exp.DataType.Type.VARCHAR: ("ct_simple", "ct_all_primitives"),
    exp.DataType.Type.TEXT: ("ct_simple", "ct_all_primitives"),
    exp.DataType.Type.CHAR: ("ct_simple", "ct_all_primitives"),
    exp.DataType.Type.BINARY: ("ct_all_primitives",),
    exp.DataType.Type.VARBINARY: ("ct_all_primitives",),
    exp.DataType.Type.DATE: ("ct_all_primitives",),
    exp.DataType.Type.TIMESTAMP: ("ct_all_primitives",),
    exp.DataType.Type.TIMESTAMPNTZ: ("ct_all_primitives",),
    exp.DataType.Type.TIMESTAMPTZ: ("ct_timestamptz_alias",),
    exp.DataType.Type.TIMESTAMPLTZ: ("ct_timestamptz_alias",),
}

# DECIMAL + the composites are handled by explicit branches in
# _iceberg_type_from_sqlglot, not via the type map.
_COMPOSITE_TYPE_COVERAGE: dict[str, tuple[str, ...]] = {
    "DECIMAL": ("ct_decimal", "ct_all_primitives"),
    "STRUCT": ("ct_struct", "ct_nested_combo"),
    "ARRAY": ("ct_array", "ct_nested_combo"),
    "MAP": ("ct_map", "ct_nested_combo"),
}


def test_primitive_type_map_keys_have_coverage() -> None:
    # The coverage mapping's keys must equal the live type-map keys: a new
    # supported primitive (or a removed one) breaks this before the per-key
    # corpus check below runs.
    assert set(_PRIMITIVE_TYPE_COVERAGE) == set(_SQLGLOT_TYPE_TO_ICEBERG), (
        "primitive type coverage map is out of sync with _SQLGLOT_TYPE_TO_ICEBERG; "
        "add the new/removed DataType.Type and a corpus entry that exercises it"
    )


@pytest.mark.parametrize("type_key", list(_SQLGLOT_TYPE_TO_ICEBERG), ids=lambda t: t.name)
def test_primitive_type_has_positive_entry(type_key: exp.DataType.Type) -> None:
    _assert_covered(f"column type {type_key.name}", _PRIMITIVE_TYPE_COVERAGE[type_key])


@pytest.mark.parametrize("composite", sorted(_COMPOSITE_TYPE_COVERAGE))
def test_composite_type_has_positive_entry(composite: str) -> None:
    _assert_covered(f"composite type {composite}", _COMPOSITE_TYPE_COVERAGE[composite])


# ---------------------------------------------------------------------------
# Partition / sort transforms accepted by _transform_from_func.
# ---------------------------------------------------------------------------

# The transform names the dispatcher accepts (identity, bucket, truncate,
# year(s), month(s), day(s), hour(s)). We assert this set still matches the
# string literals compared inside _transform_from_func so a newly-accepted
# transform name can't slip past without a corpus entry.
_TRANSFORM_COVERAGE: dict[str, tuple[str, ...]] = {
    "identity": ("ct_part_identity", "apf_identity"),
    "bucket": ("ct_part_bucket", "apf_bucket", "wob_transform"),
    "truncate": ("ct_part_truncate", "apf_truncate"),
    "year": ("ct_part_year", "apf_year"),
    "years": ("ct_part_years",),
    "month": ("ct_part_month",),
    "months": ("ct_part_months",),
    "day": ("ct_part_day", "apf_day"),
    "days": ("ct_part_days",),
    "hour": ("ct_part_hour", "apf_hour"),
    "hours": ("ct_part_hours",),
}


def _transform_names_in_source() -> set[str]:
    """Collect every string literal compared against a transform name.

    Walks the AST of ``_transform_from_func`` for ``name == "x"`` and
    ``name in (...)`` comparisons where ``name`` is the local holding the
    lowercased function name. This lets the test see the actual accepted
    names without hardcoding them a second time.
    """
    src = inspect.getsource(dispatch_mod._transform_from_func)
    tree = ast.parse(textwrap.dedent(src))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name):
            for comparator in node.comparators:
                if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                    names.add(comparator.value)
                elif isinstance(comparator, ast.Tuple):
                    for elt in comparator.elts:
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            names.add(elt.value)
    return names


def test_transform_coverage_matches_source() -> None:
    accepted = _transform_names_in_source()
    assert accepted == set(_TRANSFORM_COVERAGE), (
        "transform coverage map is out of sync with _transform_from_func; "
        f"source accepts {sorted(accepted)}, map has {sorted(_TRANSFORM_COVERAGE)}"
    )


@pytest.mark.parametrize("transform", sorted(_TRANSFORM_COVERAGE))
def test_transform_has_positive_entry(transform: str) -> None:
    _assert_covered(f"transform {transform}()", _TRANSFORM_COVERAGE[transform])


# ---------------------------------------------------------------------------
# Custom + stock AST action nodes routed in _dispatch_alter_action.
# ---------------------------------------------------------------------------

# Every node class with an ``isinstance(action, X)`` branch in
# _dispatch_alter_action, mapped to a corpus id substring. The
# test_alter_action_classes_match_source guard below proves this dict's keys
# equal the classes actually branched on, so a new ALTER action handler
# without a corpus entry fails.
_ALTER_ACTION_COVERAGE: dict[type[exp.Expression], tuple[str, ...]] = {
    exp.ColumnDef: ("add_column_single",),
    exp.Schema: ("add_columns_plural",),
    exp.Drop: ("drop_column_single", "drop_columns_plural"),
    exp.RenameColumn: ("rename_column",),
    exp.AlterRename: ("rename_table",),
    exp.AlterColumn: ("alter_column_type", "alter_column_comment"),
    AlterColumnPosition: ("alter_column_first", "alter_column_after"),
    exp.AlterSet: ("set_tblproperties",),
    UnsetTblProperties: ("unset_tblproperties",),
    SetIdentifierFields: ("set_identifier_fields",),
    DropIdentifierFields: ("drop_identifier_fields",),
    AddPartitionField: ("apf_",),
    DropPartitionField: ("dpf_",),
    ReplacePartitionField: ("rpf_",),
    WriteOrderedBy: ("wob_bare", "wob_paren"),
    WriteDistribution: ("wob_distributed", "wob_locally", "wob_unordered"),
}


def _isinstance_classes_in(func: Callable[..., object]) -> set[str]:
    """Collect class names used in ``isinstance(action, X)`` inside *func*."""
    src = inspect.getsource(func)
    tree = ast.parse(textwrap.dedent(src))
    names: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "isinstance"
            and len(node.args) == 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "action"
        ):
            target = node.args[1]
            elts = target.elts if isinstance(target, ast.Tuple) else [target]
            for elt in elts:
                if isinstance(elt, ast.Attribute):
                    names.add(elt.attr)
                elif isinstance(elt, ast.Name):
                    names.add(elt.id)
    return names


def test_alter_action_classes_match_source() -> None:
    branched = _isinstance_classes_in(dispatch_mod._dispatch_alter_action)
    mapped = {cls.__name__ for cls in _ALTER_ACTION_COVERAGE}
    assert branched == mapped, (
        "ALTER action coverage map is out of sync with _dispatch_alter_action; "
        f"source branches on {sorted(branched)}, map has {sorted(mapped)}"
    )


@pytest.mark.parametrize("action_cls", list(_ALTER_ACTION_COVERAGE), ids=lambda c: c.__name__)
def test_alter_action_has_positive_entry(action_cls: type[exp.Expression]) -> None:
    _assert_covered(f"ALTER action {action_cls.__name__}", _ALTER_ACTION_COVERAGE[action_cls])


# ---------------------------------------------------------------------------
# Top-level statement branches in dispatch().
# ---------------------------------------------------------------------------

# Each top-level statement shape dispatch() recognises, mapped to a corpus id
# substring. Semicolon is a no-op shim with no user-facing corpus entry, so it
# is intentionally excluded (and asserted excluded below).
_TOPLEVEL_COVERAGE: dict[str, tuple[str, ...]] = {
    "create_namespace": ("ns_create",),
    "drop_namespace": ("ns_drop",),
    "create_table": ("ct_simple",),
    "drop_table": ("dt_simple",),
    "alter_namespace_properties": ("ns_alter_set_properties",),
    "alter_table": ("rename_table", "add_column_single", "apf_identity"),
    "insert": ("insert_values_single",),
    "delete": ("delete_where",),
}


def test_toplevel_dispatch_branches_match_source() -> None:
    # Guard: the set of statement types dispatch() branches on (via
    # isinstance) should match our enumerated coverage, modulo the Semicolon
    # no-op. Collect the isinstance target names against ``statement``.
    src = inspect.getsource(dispatch_mod.dispatch)
    tree = ast.parse(textwrap.dedent(src))
    branched: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "isinstance"
            and len(node.args) == 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "statement"
        ):
            target = node.args[1]
            elts = target.elts if isinstance(target, ast.Tuple) else [target]
            for elt in elts:
                if isinstance(elt, ast.Attribute):
                    branched.add(elt.attr)
                elif isinstance(elt, ast.Name):
                    branched.add(elt.id)
    # Map node-class names to our logical feature keys. Create/Drop cover both
    # the namespace and table branches (disambiguated by ``kind`` at runtime),
    # so they map to two feature keys each.
    expected_classes = {
        "Create",  # namespace + table
        "Drop",  # namespace + table
        "AlterNamespaceProperties",
        "Alter",
        "Insert",
        "Delete",
        "Semicolon",  # no-op shim, intentionally uncovered
    }
    assert branched == expected_classes, (
        "dispatch() top-level branches changed; update _TOPLEVEL_COVERAGE and this guard. "
        f"source branches on {sorted(branched)}"
    )


@pytest.mark.parametrize("feature", sorted(_TOPLEVEL_COVERAGE))
def test_toplevel_statement_has_positive_entry(feature: str) -> None:
    _assert_covered(f"top-level statement {feature}", _TOPLEVEL_COVERAGE[feature])
