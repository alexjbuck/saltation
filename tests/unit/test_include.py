"""Tests for ``cambrian.sql.include`` — directive resolution + hashing.

The include resolver is pure I/O over a filesystem; tests use ``tmp_path`` to
build small file trees and assert on the expanded text, hash stability, source
list, and the error cases (missing file, circular include).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from cambrian.errors import CircularIncludeError, IncludeNotFoundError
from cambrian.sql.include import expand


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_expand_no_directives(tmp_path: Path) -> None:
    """A file with no ``--! include`` lines is returned unchanged."""
    sql = "CREATE TABLE t (id INT);\n"
    p = tmp_path / "current.sql"
    _write(p, sql)

    result = expand(p)

    assert result.text == sql
    assert result.sources == [p.resolve()]
    assert result.hash == hashlib.sha256(sql.encode("utf-8")).hexdigest()


def test_expand_single_include(tmp_path: Path) -> None:
    """A simple ``--! include child.sql`` inlines the child file with markers."""
    _write(tmp_path / "child.sql", "CREATE TABLE c (id INT);\n")
    _write(
        tmp_path / "current.sql",
        "-- top\n--! include child.sql\n-- end\n",
    )

    result = expand(tmp_path / "current.sql")

    assert "-- cambrian:include-begin child.sql" in result.text
    assert "-- cambrian:include-end child.sql" in result.text
    assert "CREATE TABLE c (id INT);" in result.text
    # Source list is the root + the child, in visit order.
    assert result.sources == [
        (tmp_path / "current.sql").resolve(),
        (tmp_path / "child.sql").resolve(),
    ]


def test_expand_glob_sorted_order(tmp_path: Path) -> None:
    """Glob expansion sorts matches lexicographically (deterministic)."""
    _write(tmp_path / "inc" / "20_b.sql", "-- contents_BBB\n")
    _write(tmp_path / "inc" / "10_a.sql", "-- contents_AAA\n")
    _write(tmp_path / "inc" / "30_c.sql", "-- contents_CCC\n")
    _write(tmp_path / "current.sql", "--! include inc/*.sql\n")

    result = expand(tmp_path / "current.sql")

    # The three child files must appear in lexicographic order.
    a_pos = result.text.index("contents_AAA")
    b_pos = result.text.index("contents_BBB")
    c_pos = result.text.index("contents_CCC")
    assert a_pos < b_pos < c_pos

    # All four files in sources, root first.
    assert result.sources[0] == (tmp_path / "current.sql").resolve()
    assert (tmp_path / "inc" / "10_a.sql").resolve() in result.sources
    assert (tmp_path / "inc" / "20_b.sql").resolve() in result.sources
    assert (tmp_path / "inc" / "30_c.sql").resolve() in result.sources


def test_expand_nested_includes(tmp_path: Path) -> None:
    """Include directives in included files are resolved recursively."""
    _write(tmp_path / "leaf.sql", "-- leaf\n")
    _write(tmp_path / "mid.sql", "-- mid\n--! include leaf.sql\n")
    _write(tmp_path / "current.sql", "-- root\n--! include mid.sql\n")

    result = expand(tmp_path / "current.sql")

    assert "-- root" in result.text
    assert "-- mid" in result.text
    assert "-- leaf" in result.text
    # Sources collected in first-visit order.
    assert result.sources == [
        (tmp_path / "current.sql").resolve(),
        (tmp_path / "mid.sql").resolve(),
        (tmp_path / "leaf.sql").resolve(),
    ]


def test_expand_paths_relative_to_including_file(tmp_path: Path) -> None:
    """Includes resolve relative to the *including* file, not cwd or the root."""
    _write(tmp_path / "a" / "b" / "leaf.sql", "-- leaf\n")
    _write(tmp_path / "a" / "mid.sql", "-- mid\n--! include b/leaf.sql\n")
    _write(tmp_path / "current.sql", "--! include a/mid.sql\n")

    result = expand(tmp_path / "current.sql")
    # If resolution had used the root's directory the include would fail.
    assert "-- leaf" in result.text


def test_expand_missing_file(tmp_path: Path) -> None:
    """A directive pointing at a non-existent file raises IncludeNotFoundError."""
    _write(tmp_path / "current.sql", "--! include nope.sql\n")
    with pytest.raises(IncludeNotFoundError) as info:
        expand(tmp_path / "current.sql")
    assert info.value.directive == "nope.sql"
    assert info.value.source == (tmp_path / "current.sql").resolve()


def test_expand_empty_glob(tmp_path: Path) -> None:
    """A glob with zero matches is treated as missing — explicit failure."""
    _write(tmp_path / "current.sql", "--! include nothere/*.sql\n")
    with pytest.raises(IncludeNotFoundError):
        expand(tmp_path / "current.sql")


def test_expand_self_circular(tmp_path: Path) -> None:
    """A file including itself raises CircularIncludeError with the chain."""
    p = tmp_path / "loop.sql"
    _write(p, "--! include loop.sql\n")
    with pytest.raises(CircularIncludeError) as info:
        expand(p)
    assert info.value.cycle[0] == p.resolve()
    assert info.value.cycle[-1] == p.resolve()


def test_expand_indirect_circular(tmp_path: Path) -> None:
    """A → B → A is detected via the visited-stack."""
    _write(tmp_path / "a.sql", "--! include b.sql\n")
    _write(tmp_path / "b.sql", "--! include a.sql\n")
    with pytest.raises(CircularIncludeError) as info:
        expand(tmp_path / "a.sql")
    # Chain should be a -> b -> a (resolved absolute).
    assert info.value.cycle[0] == (tmp_path / "a.sql").resolve()
    assert info.value.cycle[1] == (tmp_path / "b.sql").resolve()
    assert info.value.cycle[2] == (tmp_path / "a.sql").resolve()


def test_expand_same_file_twice_is_not_circular(tmp_path: Path) -> None:
    """Including the same file in two *parallel* branches is allowed."""
    _write(tmp_path / "leaf.sql", "-- leaf\n")
    _write(
        tmp_path / "current.sql",
        "--! include leaf.sql\n--! include leaf.sql\n",
    )

    result = expand(tmp_path / "current.sql")
    # Two markered blocks of the leaf in the output.
    assert result.text.count("-- leaf") == 2
    # But the source appears only once (deduplicated).
    leaf = (tmp_path / "leaf.sql").resolve()
    assert result.sources.count(leaf) == 1


def test_hash_is_stable_for_identical_input(tmp_path: Path) -> None:
    """Same files → same hash, regardless of when expand was called."""
    _write(tmp_path / "current.sql", "CREATE TABLE t (id INT);\n")
    h1 = expand(tmp_path / "current.sql").hash
    h2 = expand(tmp_path / "current.sql").hash
    assert h1 == h2


def test_hash_changes_when_content_changes(tmp_path: Path) -> None:
    """Mutating the content of the root file changes the hash."""
    p = tmp_path / "current.sql"
    _write(p, "CREATE TABLE t (id INT);\n")
    h1 = expand(p).hash
    _write(p, "CREATE TABLE t (id INT, name STRING);\n")
    h2 = expand(p).hash
    assert h1 != h2


def test_hash_changes_when_included_content_changes(tmp_path: Path) -> None:
    """Editing an included file flips the root's hash (so watch detects it)."""
    _write(tmp_path / "child.sql", "-- v1\n")
    _write(tmp_path / "current.sql", "--! include child.sql\n")
    h1 = expand(tmp_path / "current.sql").hash
    _write(tmp_path / "child.sql", "-- v2\n")
    h2 = expand(tmp_path / "current.sql").hash
    assert h1 != h2


def test_include_directive_can_be_indented(tmp_path: Path) -> None:
    """Leading whitespace before ``--!`` doesn't suppress the directive."""
    _write(tmp_path / "child.sql", "-- child\n")
    _write(tmp_path / "current.sql", "    --! include child.sql\n")
    result = expand(tmp_path / "current.sql")
    assert "-- child" in result.text


def test_non_directive_comment_passes_through(tmp_path: Path) -> None:
    """A regular ``-- comment`` is *not* an include directive (no ``!``)."""
    _write(
        tmp_path / "current.sql",
        "-- include child.sql\nCREATE TABLE t (id INT);\n",
    )
    result = expand(tmp_path / "current.sql")
    assert "-- include child.sql" in result.text
    assert "child.sql" not in [str(p.name) for p in result.sources[1:]]


def test_expand_preserves_trailing_newline(tmp_path: Path) -> None:
    """The expander shouldn't strip the source's trailing newline."""
    _write(tmp_path / "current.sql", "SELECT 1;\n")
    result = expand(tmp_path / "current.sql")
    assert result.text.endswith("\n")
