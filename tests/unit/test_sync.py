"""Unit tests for ``cambrian.migrate.sync``.

Pure-logic tests against a mocked catalog. The end-to-end Lakekeeper
roundtrips live in ``tests/integration/test_sync.py``.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from cambrian.config import CambrianConfig, CatalogConfig, EvolutionsConfig
from cambrian.errors import IllegalStateError
from cambrian.migrate.sync import (
    SyncResult,
    cambrian_sync,
)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _ts(offset: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=offset)


def _event_row(
    *,
    event_type: str,
    evolution_id: str,
    evolution_sql: str = "",
    evolution_hash: str | None = None,
    ts: datetime | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_ts": ts or _ts(0),
        "event_type": event_type,
        "evolution_id": evolution_id,
        "evolution_hash": evolution_hash if evolution_hash is not None else _sha(evolution_sql),
        "evolution_sql": evolution_sql,
        "actor": "test",
        "notes": None,
    }


def _events_arrow(rows: list[dict[str, Any]]) -> pa.Table:
    if not rows:
        return pa.table(
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
    return pa.table(
        {
            "event_id": [r["event_id"] for r in rows],
            "event_ts": [r["event_ts"] for r in rows],
            "event_type": [r["event_type"] for r in rows],
            "evolution_id": [r["evolution_id"] for r in rows],
            "evolution_hash": [r["evolution_hash"] for r in rows],
            "evolution_sql": [r["evolution_sql"] for r in rows],
            "actor": [r["actor"] for r in rows],
            "notes": [r["notes"] for r in rows],
        }
    )


def _mock_catalog(rows: list[dict[str, Any]]) -> MagicMock:
    """Build a catalog mock whose events scan yields *rows* and selfmigrate succeeds."""
    catalog = MagicMock()
    arrow = _events_arrow(rows)
    table = MagicMock()
    table.scan.return_value.to_arrow.return_value = arrow
    catalog.load_table.return_value = table
    return catalog


@pytest.fixture
def stub_catalog_and_selfmigrate(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[list[dict[str, Any]]]:
    """Bypass real catalog construction and self-migration.

    Tests append to the yielded list to set the events the mock catalog will
    return. ``ensure_current`` is patched to return a stub state object.
    """
    rows: list[dict[str, Any]] = []

    def _fake_load_catalog(_cfg: CambrianConfig) -> MagicMock:
        return _mock_catalog(rows)

    class _StubState:
        sidecar_namespace = "_cambrian"
        version = 1
        is_version_ahead = False

    def _fake_ensure_current(*args: object, **kwargs: object) -> _StubState:
        del args, kwargs
        return _StubState()

    monkeypatch.setattr("cambrian.migrate.sync.load_catalog", _fake_load_catalog)
    monkeypatch.setattr("cambrian.migrate.sync.ensure_current", _fake_ensure_current)
    yield rows


def _config(tmp_path: Path) -> CambrianConfig:
    return CambrianConfig(
        catalog=CatalogConfig(type="sql", uri=f"sqlite:///{tmp_path}/cat.db"),
        evolutions=EvolutionsConfig(dir=str(tmp_path / "evolutions")),
    )


def test_sync_writes_missing_files(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    stub_catalog_and_selfmigrate.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(1))
    )

    result = cambrian_sync(_config(tmp_path))

    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    assert target.exists()
    assert target.read_text(encoding="utf-8") == sql
    assert result.written == 1
    assert result.refused == 0
    assert result.files[0].status == "written"


def test_sync_skips_hash_matched_files(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    target.parent.mkdir(parents=True)
    target.write_text(sql, encoding="utf-8")

    stub_catalog_and_selfmigrate.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(1))
    )

    result = cambrian_sync(_config(tmp_path))

    assert result.skipped == 1
    assert result.written == 0
    assert result.files[0].status == "skipped"


def test_sync_refuses_conflict_without_force(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    catalog_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    local_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT, extra STRING);\n"
    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    target.parent.mkdir(parents=True)
    target.write_text(local_sql, encoding="utf-8")

    stub_catalog_and_selfmigrate.append(
        _event_row(
            event_type="commit", evolution_id="0001_seed", evolution_sql=catalog_sql, ts=_ts(1)
        )
    )

    result = cambrian_sync(_config(tmp_path))

    assert result.refused == 1
    assert target.read_text(encoding="utf-8") == local_sql  # untouched
    assert result.has_refusals


def test_sync_force_overwrites_conflict(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    catalog_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    local_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT, extra STRING);\n"
    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    target.parent.mkdir(parents=True)
    target.write_text(local_sql, encoding="utf-8")

    stub_catalog_and_selfmigrate.append(
        _event_row(
            event_type="commit", evolution_id="0001_seed", evolution_sql=catalog_sql, ts=_ts(1)
        )
    )

    result = cambrian_sync(_config(tmp_path), force=True)

    assert result.overwritten == 1
    assert target.read_text(encoding="utf-8") == catalog_sql
    assert not result.has_refusals


def test_sync_dry_run_writes_nothing(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    stub_catalog_and_selfmigrate.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(1))
    )

    result = cambrian_sync(_config(tmp_path), dry_run=True)

    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    assert not target.exists()
    assert result.dry_run is True
    assert result.written == 1
    assert result.files[0].note == "would write"


def test_sync_diff_implies_dry_run_without_force(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    catalog_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    local_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT, evil STRING);\n"
    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    target.parent.mkdir(parents=True)
    target.write_text(local_sql, encoding="utf-8")

    stub_catalog_and_selfmigrate.append(
        _event_row(
            event_type="commit", evolution_id="0001_seed", evolution_sql=catalog_sql, ts=_ts(1)
        )
    )

    result = cambrian_sync(_config(tmp_path), diff=True)

    assert result.dry_run is True
    assert result.refused == 1
    diff = result.files[0].diff
    assert diff is not None
    assert "evil" in diff
    # File still has the local content (diff is dry-run).
    assert target.read_text(encoding="utf-8") == local_sql


def test_sync_diff_with_force_still_overwrites(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    catalog_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    local_sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT, evil STRING);\n"
    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    target.parent.mkdir(parents=True)
    target.write_text(local_sql, encoding="utf-8")

    stub_catalog_and_selfmigrate.append(
        _event_row(
            event_type="commit", evolution_id="0001_seed", evolution_sql=catalog_sql, ts=_ts(1)
        )
    )

    result = cambrian_sync(_config(tmp_path), diff=True, force=True)

    assert result.dry_run is False
    assert result.overwritten == 1
    assert result.files[0].diff is not None
    assert target.read_text(encoding="utf-8") == catalog_sql


def test_sync_filters_out_uncommitted(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    rows = stub_catalog_and_selfmigrate
    rows.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(1))
    )
    rows.append(
        _event_row(event_type="uncommit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(2))
    )

    result = cambrian_sync(_config(tmp_path))

    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    assert not target.exists()
    assert result.files == []


def test_sync_re_committed_after_uncommit_is_synced(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    """commit → uncommit → commit again: the later commit wins, file is synced."""
    sql_v1 = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    sql_v2 = "CREATE TABLE IF NOT EXISTS t (id BIGINT, name STRING);\n"
    rows = stub_catalog_and_selfmigrate
    rows.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql_v1, ts=_ts(1))
    )
    rows.append(
        _event_row(event_type="uncommit", evolution_id="0001_seed", evolution_sql=sql_v1, ts=_ts(2))
    )
    rows.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql_v2, ts=_ts(3))
    )

    result = cambrian_sync(_config(tmp_path))

    target = tmp_path / "evolutions" / "committed" / "0001_seed.sql"
    assert target.exists()
    assert target.read_text(encoding="utf-8") == sql_v2
    assert result.written == 1


def test_sync_refuses_internally_inconsistent_catalog_row(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    stub_catalog_and_selfmigrate.append(
        _event_row(
            event_type="commit",
            evolution_id="0001_seed",
            evolution_sql=sql,
            evolution_hash="deadbeef" * 8,  # 64 hex chars, but not sha256(sql)
            ts=_ts(1),
        )
    )

    with pytest.raises(IllegalStateError, match=r"internally inconsistent"):
        cambrian_sync(_config(tmp_path))


def test_sync_writes_no_events(
    tmp_path: Path,
    stub_catalog_and_selfmigrate: list[dict[str, Any]],
) -> None:
    """sync is read-only against the catalog. No write_event must fire."""
    sql = "CREATE TABLE IF NOT EXISTS t (id BIGINT);\n"
    stub_catalog_and_selfmigrate.append(
        _event_row(event_type="commit", evolution_id="0001_seed", evolution_sql=sql, ts=_ts(1))
    )

    with patch("cambrian.sidecar.events.write_event") as mock_write:
        cambrian_sync(_config(tmp_path))

    mock_write.assert_not_called()


def test_sync_result_summary_counts(tmp_path: Path) -> None:
    """The aggregate-property counts on SyncResult match the per-file status list."""
    from cambrian.migrate.sync import SyncFileResult

    sr = SyncResult(
        files=[
            SyncFileResult(
                evolution_id="a", path=tmp_path / "a.sql", status="written", catalog_hash="x"
            ),
            SyncFileResult(
                evolution_id="b", path=tmp_path / "b.sql", status="written", catalog_hash="x"
            ),
            SyncFileResult(
                evolution_id="c", path=tmp_path / "c.sql", status="skipped", catalog_hash="x"
            ),
            SyncFileResult(
                evolution_id="d", path=tmp_path / "d.sql", status="overwritten", catalog_hash="x"
            ),
            SyncFileResult(
                evolution_id="e", path=tmp_path / "e.sql", status="refused", catalog_hash="x"
            ),
        ]
    )

    assert sr.written == 2
    assert sr.skipped == 1
    assert sr.overwritten == 1
    assert sr.refused == 1
    assert sr.discrepancies == 0
    assert sr.has_refusals
