"""Fuzz the catalog-free front end: no input may escape as anything but a SaltationError.

The CLI and ``watch`` only catch ``SaltationError``. Each escape found so far is
pinned by a regression test at the bottom.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from unittest import mock
from unittest.mock import Mock

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyiceberg.catalog import Catalog

from saltation.config import load_config
from saltation.errors import (
    CircularIncludeError,
    DispatchError,
    IncludeNotFoundError,
    InvalidConfigError,
    MissingEnvVarError,
    SaltationError,
    SqlParseError,
)
from saltation.iceberg.affected import affected_tables_with_overrides, affected_views
from saltation.sql.dispatch import dispatch
from saltation.sql.include import expand
from saltation.sql.script import parse_script
from tests.fixtures.iceberg_corpus import CORPUS


def assert_typed(fn: Callable[[], object]) -> None:
    try:
        fn()
    except SaltationError:
        return


@pytest.fixture(autouse=True, scope="module")
def _quiet_sqlglot() -> Iterator[None]:
    logger = logging.getLogger("sqlglot")
    level = logger.level
    logger.setLevel(logging.ERROR)
    yield
    logger.setLevel(level)


class _CatalogReached(Exception):
    pass


def _untouchable_catalog() -> Catalog:
    """Dispatch stops at the first catalog call: everything before it is front end."""
    catalog = Mock(spec_set=Catalog)
    for name in dir(Catalog):
        if not name.startswith("_") and callable(getattr(Catalog, name)):
            getattr(catalog, name).side_effect = _CatalogReached(name)
    return cast(Catalog, catalog)


_CATALOG = _untouchable_catalog()
VIEW_DIALECTS = ("spark", "trino")


def _front_end(text: str, source_dialect: str) -> None:
    statements = parse_script(text, view_source_dialect=source_dialect)
    affected_tables_with_overrides(text, statements)
    for statement in statements:
        affected_views(statement)
        with contextlib.suppress(_CatalogReached):
            dispatch(_CATALOG, statement, view_dialects=VIEW_DIALECTS)


source_dialects = st.sampled_from(["spark", "trino", "postgres", "duckdb"])

_FRAGMENTS = [
    *("$$", "$", "${X}", "--!", "--! include x.sql\n", "-- saltation:tables a..b, ,c\n"),
    *(";", "(", ")", "'", '"', "`", "\\", "/*", "*/", "--", "\n", " ", "\t", "\x00", "\u202e"),
    *("CREATE", "OR REPLACE", "VIEW", "TABLE", "NAMESPACE", "ALTER", "DROP", "INSERT INTO"),
    *("AS", "DIALECT", "trino", "spark", "SELECT", "VALUES", "x", "a.t", "a..t", "1", ",", "."),
    *("USING iceberg", "PARTITIONED BY", "ADD PARTITION FIELD", "WRITE ORDERED BY"),
    *("bucket(16, id)", "days(ts)", "TBLPROPERTIES", "DEFAULT", "COLUMN", "DECIMAL(", "\u00e9"),
]

sql_soup = st.lists(st.one_of(st.sampled_from(_FRAGMENTS), st.text(max_size=4)), max_size=40).map(
    "".join
)
deep_nesting = st.integers(1, 400).map(lambda n: "SELECT " + "(" * n + "1" + ")" * n)


@given(text=st.one_of(st.text(), sql_soup, deep_nesting), source_dialect=source_dialects)
def test_fuzz_arbitrary_text_raises_only_typed_errors(text: str, source_dialect: str) -> None:
    assert_typed(lambda: _front_end(text, source_dialect))


_TOKEN = re.compile(r"\s+|\w+|'(?:[^'\\]|\\.)*'|`[^`]*`|\$\$|--|/\*|\*/|.", re.S)
_NOISE = ["(", ")", ",", ";", "'", "`", "$$", "NULL", "AS", "DIALECT trino", ".", "*", "\n"]


@st.composite
def mutated_corpus_sql(draw: st.DrawFn) -> str:
    tokens = _TOKEN.findall(draw(st.sampled_from([e.sql for e in CORPUS])))
    for _ in range(draw(st.integers(1, 4))):
        if not tokens:
            break
        i = draw(st.integers(0, len(tokens) - 1))
        match draw(st.sampled_from(["delete", "duplicate", "swap", "case", "space", "insert"])):
            case "delete":
                del tokens[i]
            case "duplicate":
                tokens.insert(i, tokens[i])
            case "swap":
                j = draw(st.integers(0, len(tokens) - 1))
                tokens[i], tokens[j] = tokens[j], tokens[i]
            case "case":
                tokens[i] = draw(st.sampled_from([str.upper, str.lower, str.swapcase]))(tokens[i])
            case "space":
                tokens[i] = draw(st.sampled_from(["", " ", "\n", "\t", "/* c */", "-- c\n"]))
            case _:
                tokens.insert(i, draw(st.sampled_from(_NOISE)))
    return "".join(tokens)


@given(text=mutated_corpus_sql(), source_dialect=source_dialects)
def test_fuzz_mutated_corpus_raises_only_typed_errors(text: str, source_dialect: str) -> None:
    assert_typed(lambda: _front_end(text, source_dialect))


# --- include resolution ---------------------------------------------------------

_LINE_BREAKS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"
body_lines = st.text(
    st.characters(exclude_categories=("Cs",), exclude_characters=_LINE_BREAKS), min_size=1
).filter(lambda s: s.strip() and not any(m in s for m in ("--!", "saltation:include-", "body of")))
directive_forms = st.sampled_from(
    ["--! include {}", "  --!include {}", "--!  include   {}  ", "\t--! include ./{}"]
)


@dataclass
class IncludeTree:
    """``files[i]`` is a list of body lines (str) and include targets (int; -1 = missing)."""

    files: list[list[str | int]]
    forms: list[str]

    def name(self, i: int) -> str:
        return f"f{i}.sql" if i >= 0 else "missing.sql"

    def text(self, i: int) -> str:
        lines = [
            item if isinstance(item, str) else self.forms[i].format(self.name(item))
            for item in self.files[i]
        ]
        return "\n".join(lines) + "\n"

    def expected(self) -> tuple[list[str], list[int]] | type[SaltationError]:
        """Body lines in include order and first-visit order, or the error expected first."""
        lines: list[str] = []
        visited: list[int] = []

        def walk(i: int, stack: list[int]) -> type[SaltationError] | None:
            if i in stack:
                return CircularIncludeError
            if i not in visited:
                visited.append(i)
            for item in self.files[i]:
                if isinstance(item, str):
                    lines.append(item)
                elif item < 0:
                    return IncludeNotFoundError
                elif err := walk(item, [*stack, i]):
                    return err
            return None

        return walk(0, []) or (lines, visited)


@st.composite
def include_trees(draw: st.DrawFn) -> IncludeTree:
    n = draw(st.integers(1, 6))
    parents = [-1] + [draw(st.integers(0, i - 1)) for i in range(1, n)]
    files: list[list[str | int]] = [
        [f"-- body of f{i}", *draw(st.lists(body_lines, max_size=3))] for i in range(n)
    ]
    for child in range(1, n):
        siblings = files[parents[child]]
        siblings.insert(draw(st.integers(0, len(siblings))), child)
    for _ in range(draw(st.integers(0, 2))):
        i = draw(st.integers(0, n - 1))
        ancestors = [i]
        while parents[ancestors[-1]] >= 0:
            ancestors.append(parents[ancestors[-1]])
        target = draw(st.sampled_from([*ancestors, -1]))
        files[i].insert(draw(st.integers(0, len(files[i]))), target)
    forms = draw(st.lists(directive_forms, min_size=n, max_size=n))
    return IncludeTree(files, forms)


@given(tree=include_trees())
def test_fuzz_include_tree_expands_each_file_once_in_order(tree: IncludeTree) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for i in range(len(tree.files)):
            (root / tree.name(i)).write_text(tree.text(i), encoding="utf-8")
        expected = tree.expected()
        if not isinstance(expected, tuple):
            with pytest.raises(expected):
                expand(root / "f0.sql")
            return
        result = expand(root / "f0.sql")

    lines, visited = expected
    got = [
        line
        for line in result.text.splitlines()
        if line.strip() and not line.startswith("-- saltation:include-")
    ]
    assert got == lines
    for i in visited:
        assert got.count(f"-- body of f{i}") == 1
    assert [p.name for p in result.sources] == [tree.name(i) for i in visited]


directive_targets = st.text(
    st.one_of(st.characters(exclude_categories=("Cs",), exclude_characters="/"), st.just("*")),
    min_size=1,
    max_size=300,
)


@given(target=directive_targets, content=st.binary(max_size=40))
def test_fuzz_include_directive_text_raises_only_typed_errors(target: str, content: bytes) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "sub").mkdir()
        (root / "child.sql").write_bytes(content)
        (root / "current.sql").write_text(
            f"--! include child.sql\n--! include {target}\n", encoding="utf-8"
        )
        assert_typed(lambda: expand(root / "current.sql"))


# --- config ----------------------------------------------------------------------

ENV_NAMES = ["SALTATION_FUZZ_A", "SALTATION_FUZZ_B", "_SALTATION_FUZZ_C"]
env_values = st.text(st.characters(exclude_categories=("Cs",), exclude_characters="\x00"))
template_parts = st.one_of(
    st.text(st.characters(exclude_categories=("Cs",), exclude_characters="\x00"), max_size=5),
    st.sampled_from(["$", "$$", "{", "}", "${", "$${", "${}", "${1X}"]),
    st.sampled_from(ENV_NAMES).map(lambda n: f"${{{n}}}"),
    st.sampled_from(ENV_NAMES).map(lambda n: f"$${{{n}}}"),
)
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _substitute(template: str, env: dict[str, str]) -> tuple[str, set[str]]:
    """Reference scanner for ``${NAME}`` / ``$${NAME}``, independent of config's regex."""
    out: list[str] = []
    missing: set[str] = set()
    i = 0
    while i < len(template):
        for escaped, start in ((True, "$${"), (False, "${")):
            if not template.startswith(start, i):
                continue
            name = _NAME.match(template, i + len(start))
            if name is None or not template.startswith("}", name.end()):
                continue
            if escaped:
                out.append("${" + name.group() + "}")
            elif name.group() in env:
                out.append(env[name.group()])
            else:
                missing.add(name.group())
                out.append(name.group(0))
            i = name.end() + 1
            break
        else:
            out.append(template[i])
            i += 1
    return "".join(out), missing


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


@given(
    template=st.lists(template_parts, max_size=8).map("".join),
    env=st.dictionaries(st.sampled_from(ENV_NAMES), env_values),
)
def test_fuzz_config_env_substitution_is_total(template: str, env: dict[str, str]) -> None:
    expected, missing = _substitute(template, env)
    with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ):
        for name in ENV_NAMES:
            os.environ.pop(name, None)
        os.environ.update(env)
        path = Path(tmp) / "saltation.toml"
        path.write_text(
            f'[catalog]\ntype = "sql"\nuri = {_toml_string(template)}\n', encoding="utf-8"
        )
        if missing:
            with pytest.raises(MissingEnvVarError) as info:
                load_config(path)
            for name in missing:
                assert f"${{{name}}}" in str(info.value)
            return
        assert load_config(path).catalog.uri == expected


def test_escaped_reference_is_literal_even_when_set(tmp_path: Path) -> None:
    path = tmp_path / "saltation.toml"
    path.write_text('[catalog]\ntype = "sql"\nuri = "$${HOME}/${HOME}"\n', encoding="utf-8")
    with mock.patch.dict(os.environ, {"HOME": "/h"}):
        assert load_config(path).catalog.uri == "${HOME}//h"


toml_soup = st.lists(
    st.one_of(
        st.sampled_from(["[catalog]", "[[a]]", "type", "uri", "=", '"sql"', "\n", "[", "]"]),
        st.sampled_from(["{", "}", "${X}", "'", '"""', "\\u00", "1979-05-27", "inf", "\\"]),
        st.text(max_size=4),
    ),
    max_size=30,
).map(lambda parts: "".join(parts).encode("utf-8", "surrogatepass"))


@given(content=st.one_of(st.binary(), toml_soup))
def test_fuzz_config_file_raises_only_typed_errors(content: bytes) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "saltation.toml"
        path.write_bytes(content)
        assert_typed(lambda: load_config(path))


# --- regressions: each untyped escape the fuzzers found -----------------------------


def _dispatch_text(text: str) -> None:
    _front_end(text, "spark")


def test_syntax_error_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        parse_script("CREATE TABLE a.t (id INT")


def test_syntax_error_reports_its_line_in_the_script() -> None:
    with pytest.raises(SqlParseError) as info:
        parse_script("CREATE NAMESPACE a;\n\nCREATE TABLE a.t (id INT")
    assert info.value.line == 3


def test_deeply_nested_sql_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        parse_script("SELECT " + "(" * 500 + "1" + ")" * 500)


@pytest.mark.parametrize(
    "sql",
    ["DROP TABLE a..t", "DROP VIEW a..v", "DELETE FROM a..t WHERE id = 1", "CREATE NAMESPACE a..b"],
)
def test_empty_identifier_part_is_a_saltation_error(sql: str) -> None:
    with pytest.raises(SaltationError):
        _dispatch_text(sql)


@pytest.mark.parametrize("transform", ["months()", "day()"])
def test_partition_transform_without_column_is_a_saltation_error(transform: str) -> None:
    with pytest.raises(SaltationError):
        _dispatch_text(
            f"CREATE TABLE a.t (ts TIMESTAMP) USING iceberg PARTITIONED BY ({transform})"
        )


@pytest.mark.parametrize("column", ["id BIGINT DEFAULT 0.5", "id INT DEFAULT 3000000000"])
def test_default_not_representable_in_the_column_type_is_a_dispatch_error(column: str) -> None:
    with pytest.raises(DispatchError):
        _dispatch_text(f"CREATE TABLE a.t ({column}) USING iceberg")


def test_non_numeric_format_version_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        _dispatch_text(
            "CREATE TABLE a.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='x')"
        )


def test_decimal_precision_out_of_range_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        _dispatch_text("CREATE TABLE a.t (d DECIMAL(39, 2)) USING iceberg")


def test_duplicate_column_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        _dispatch_text("CREATE TABLE a.t (id BIGINT, id INT) USING iceberg")


def test_unsupported_statement_that_cannot_render_is_a_saltation_error() -> None:
    with pytest.raises(SaltationError):
        _dispatch_text("x''")


def test_include_of_a_directory_is_a_saltation_error(tmp_path: Path) -> None:
    (tmp_path / "current").mkdir()
    (tmp_path / "current.sql").write_text("--! include current\n", encoding="utf-8")
    with pytest.raises(SaltationError):
        expand(tmp_path / "current.sql")


def test_non_utf8_include_is_a_saltation_error(tmp_path: Path) -> None:
    (tmp_path / "current.sql").write_bytes(b"-- caf\xe9\n")
    with pytest.raises(SaltationError):
        expand(tmp_path / "current.sql")


def test_non_utf8_config_is_a_saltation_error(tmp_path: Path) -> None:
    path = tmp_path / "saltation.toml"
    path.write_bytes(b'[catalog]\ntype = "sql"\nuri = "caf\xe9"\n')
    with pytest.raises(SaltationError):
        load_config(path)


def test_config_path_that_is_a_directory_is_a_saltation_error(tmp_path: Path) -> None:
    with pytest.raises(SaltationError):
        load_config(tmp_path)


@pytest.mark.parametrize("target", ["***", "a\x00b", "x" * 300])
def test_include_directive_that_is_not_a_path_is_a_saltation_error(
    tmp_path: Path, target: str
) -> None:
    (tmp_path / "current.sql").write_text(f"--! include {target}\n", encoding="utf-8")
    with pytest.raises(SaltationError):
        expand(tmp_path / "current.sql")


@pytest.mark.parametrize("value", ["[" * 2000 + "]" * 2000, "{a = " * 2000 + "1" + "}" * 2000])
def test_deeply_nested_config_is_an_invalid_config_error(tmp_path: Path, value: str) -> None:
    path = tmp_path / "saltation.toml"
    path.write_text(f'[catalog]\ntype = "sql"\ndeep = {value}\n', encoding="utf-8")
    with pytest.raises(InvalidConfigError):
        load_config(path)


def test_partition_on_unknown_column_is_a_saltation_error() -> None:
    sql = "CREATE TABLE foo.t (p STRUCT<x: INT>) USING iceberg PARTITIONED BY (bucket(4, p.y))"
    with pytest.raises(DispatchError, match="unknown column 'p.y'"):
        _front_end(sql, "spark")
