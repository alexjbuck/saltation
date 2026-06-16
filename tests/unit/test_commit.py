"""Unit tests for the M7 commit/uncommit helpers.

Pure-logic tests only — slugify, sequence numbering, filename parsing,
applied-set computation from a mocked events log. The catalog-touching
paths (commit + uncommit + reset --to end-to-end) live in
``tests/integration/test_commit.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from cambrian.errors import IllegalStateError
from cambrian.migrate.commit import (
    COMMITTED_TAG_PREFIX,
    SLUG_MAX_LENGTH,
    CommittedFile,
    compute_evolution_hash,
    discover_committed_files,
    next_sequence_number,
    parse_committed_filename,
    slugify,
)
from cambrian.sidecar.events import applied_committed_ids

# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


def test_slugify_basic() -> None:
    assert slugify("Add users table") == "add-users-table"


def test_slugify_collapses_runs_and_strips_edges() -> None:
    assert slugify("  --Add---users   table--") == "add-users-table"


def test_slugify_empty_falls_back() -> None:
    assert slugify("") == "evolution"
    assert slugify("   ") == "evolution"
    assert slugify("---") == "evolution"
    assert slugify("!@#$%^&*()") == "evolution"


def test_slugify_unicode_normalises_to_ascii() -> None:
    # NFKD strips accents to base letters; non-letter unicode is dropped.
    assert slugify("naïve café") == "naive-cafe"
    # NFKD decomposes mathematical-script chars to their ASCII equivalents.
    assert slugify("𝓊𝓃𝒾𝒸𝑜𝒹𝑒") == "unicode"  # noqa: RUF001
    # Pure-non-decomposable chars (emoji) collapse to the fallback.
    assert slugify("🎉🚀") == "evolution"


def test_slugify_preserves_existing_dashes_collapsed() -> None:
    # User wrote dashes; we keep them but collapse runs.
    assert slugify("foo-bar-baz") == "foo-bar-baz"
    assert slugify("foo--bar---baz") == "foo-bar-baz"


def test_slugify_caps_length() -> None:
    long = "a" * 200
    out = slugify(long)
    assert len(out) <= SLUG_MAX_LENGTH
    assert out == "a" * SLUG_MAX_LENGTH


def test_slugify_caps_length_and_strips_trailing_dash() -> None:
    # If the cap lands on a dash, strip it; the resulting slug shouldn't
    # end on a dash.
    msg = "a" * (SLUG_MAX_LENGTH - 1) + "-tail"
    out = slugify(msg)
    assert not out.endswith("-")
    assert len(out) <= SLUG_MAX_LENGTH


def test_slugify_lowercases() -> None:
    assert slugify("MIXED Case Words") == "mixed-case-words"


# ---------------------------------------------------------------------------
# parse_committed_filename + discover_committed_files
# ---------------------------------------------------------------------------


def test_parse_committed_filename_valid() -> None:
    assert parse_committed_filename("0001_add-users.sql") == (1, "add-users")
    assert parse_committed_filename("0042_a.sql") == (42, "a")


def test_parse_committed_filename_invalid() -> None:
    assert parse_committed_filename("README.md") is None
    assert parse_committed_filename("0001_NOT_LOWERCASE.sql") is None
    assert parse_committed_filename("1_too_short.sql") is None  # 3 digits is rejected
    assert parse_committed_filename("0001-wrong-sep.sql") is None
    assert parse_committed_filename("0001_slug.txt") is None


def test_discover_committed_files_empty_dir(tmp_path: Path) -> None:
    assert discover_committed_files(tmp_path) == []
    assert discover_committed_files(tmp_path / "nonexistent") == []


def test_discover_committed_files_sorts_numerically(tmp_path: Path) -> None:
    (tmp_path / "0003_third.sql").write_text("", encoding="utf-8")
    (tmp_path / "0001_first.sql").write_text("", encoding="utf-8")
    (tmp_path / "0002_second.sql").write_text("", encoding="utf-8")
    files = discover_committed_files(tmp_path)
    assert [f.number for f in files] == [1, 2, 3]
    assert [f.slug for f in files] == ["first", "second", "third"]


def test_discover_committed_files_ignores_unrecognised(tmp_path: Path) -> None:
    (tmp_path / "0001_real.sql").write_text("", encoding="utf-8")
    (tmp_path / ".gitkeep").write_text("", encoding="utf-8")
    (tmp_path / "README.md").write_text("", encoding="utf-8")
    (tmp_path / "current.sql.bak").write_text("", encoding="utf-8")
    files = discover_committed_files(tmp_path)
    assert len(files) == 1
    assert files[0].evolution_id == "0001_real"


# ---------------------------------------------------------------------------
# next_sequence_number
# ---------------------------------------------------------------------------


def test_next_sequence_number_empty(tmp_path: Path) -> None:
    assert next_sequence_number(tmp_path) == 1


def test_next_sequence_number_increments(tmp_path: Path) -> None:
    (tmp_path / "0001_one.sql").write_text("", encoding="utf-8")
    (tmp_path / "0002_two.sql").write_text("", encoding="utf-8")
    assert next_sequence_number(tmp_path) == 3


def test_next_sequence_number_refuses_gap(tmp_path: Path) -> None:
    (tmp_path / "0001_one.sql").write_text("", encoding="utf-8")
    (tmp_path / "0003_three.sql").write_text("", encoding="utf-8")  # missing 0002
    with pytest.raises(IllegalStateError, match=r"gap"):
        next_sequence_number(tmp_path)


def test_next_sequence_number_refuses_start_above_1(tmp_path: Path) -> None:
    (tmp_path / "0002_two.sql").write_text("", encoding="utf-8")
    with pytest.raises(IllegalStateError):
        next_sequence_number(tmp_path)


# ---------------------------------------------------------------------------
# CommittedFile tag generation
# ---------------------------------------------------------------------------


def test_committed_file_tag_ref_matches_documented_shape() -> None:
    cf = CommittedFile(number=7, slug="add-users", path=Path("/tmp/0007_add-users.sql"))
    assert cf.evolution_id == "0007_add-users"
    # CLAUDE.md specifies cambrian.committed.<n>.<msg> — n unpadded, msg = slug.
    assert cf.tag_ref() == "cambrian.committed.7.add-users"
    assert cf.tag_ref().startswith(COMMITTED_TAG_PREFIX)


# ---------------------------------------------------------------------------
# compute_evolution_hash
# ---------------------------------------------------------------------------


def test_compute_evolution_hash_stable() -> None:
    h1 = compute_evolution_hash("CREATE TABLE t (id BIGINT);")
    h2 = compute_evolution_hash("CREATE TABLE t (id BIGINT);")
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex


def test_compute_evolution_hash_differs_on_change() -> None:
    h1 = compute_evolution_hash("foo")
    h2 = compute_evolution_hash("bar")
    assert h1 != h2


# ---------------------------------------------------------------------------
# applied_committed_ids — exercises the events-log → applied-set logic
# ---------------------------------------------------------------------------


def _make_catalog(rows: list[dict]) -> MagicMock:
    """Build a mock catalog whose events table scan returns *rows*."""
    catalog = MagicMock()
    arrow = (
        pa.table(
            {
                "event_id": [r["event_id"] for r in rows],
                "event_ts": [r["event_ts"] for r in rows],
                "event_type": [r["event_type"] for r in rows],
                "evolution_id": [r["evolution_id"] for r in rows],
                "evolution_hash": [r["evolution_hash"] for r in rows],
                "evolution_sql": [r.get("evolution_sql", "") for r in rows],
                "actor": [r.get("actor", "test") for r in rows],
                "notes": [r.get("notes", None) for r in rows],
            }
        )
        if rows
        else pa.table(
            {
                "event_id": pa.array([], pa.string()),
                "event_ts": pa.array([], pa.timestamp("us", tz="UTC")),
                "event_type": pa.array([], pa.string()),
                "evolution_id": pa.array([], pa.string()),
                "evolution_hash": pa.array([], pa.string()),
                "evolution_sql": pa.array([], pa.string()),
                "actor": pa.array([], pa.string()),
                "notes": pa.array([], pa.string()),
            }
        )
    )
    table = MagicMock()
    table.scan.return_value.to_arrow.return_value = arrow
    catalog.load_table.return_value = table
    return catalog


def _ts(offset_seconds: int) -> datetime:
    return datetime(2025, 1, 1, tzinfo=UTC) + timedelta(seconds=offset_seconds)


def test_applied_committed_ids_empty() -> None:
    catalog = _make_catalog([])
    assert applied_committed_ids(catalog, "ns") == {}


def test_applied_committed_ids_includes_applied_committed() -> None:
    catalog = _make_catalog(
        [
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(1),
                "event_type": "commit",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(2),
                "event_type": "apply",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
        ]
    )
    assert applied_committed_ids(catalog, "ns") == {"0001_first": "abc"}


def test_applied_committed_ids_excludes_uncommitted() -> None:
    catalog = _make_catalog(
        [
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(1),
                "event_type": "apply",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(2),
                "event_type": "uncommit",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
        ]
    )
    assert applied_committed_ids(catalog, "ns") == {}


def test_applied_committed_ids_re_applied_after_uncommit() -> None:
    """uncommit then commit-and-apply again → evolution is applied again."""
    catalog = _make_catalog(
        [
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(1),
                "event_type": "apply",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(2),
                "event_type": "uncommit",
                "evolution_id": "0001_first",
                "evolution_hash": "abc",
            },
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(3),
                "event_type": "apply",
                "evolution_id": "0001_first",
                "evolution_hash": "def",
            },
        ]
    )
    assert applied_committed_ids(catalog, "ns") == {"0001_first": "def"}


def test_applied_committed_ids_ignores_current() -> None:
    """``evolution_id="current"`` (the dev-loop slot) is never in the applied set."""
    catalog = _make_catalog(
        [
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": _ts(1),
                "event_type": "apply",
                "evolution_id": "current",
                "evolution_hash": "abc",
            },
        ]
    )
    assert applied_committed_ids(catalog, "ns") == {}
