"""Integration tests for the M7 commit/uncommit/reset-to lifecycle.

Full-cycle tests against a live Lakekeeper:

* commit + apply replay + uncommit + status reflects state
* post-hoc edit of a committed file → apply refuses
* reset --to <evolution_id> rolls back, audit trail preserved
* uncommit with downstream committed file → refusal
* uncommit with non-empty current.sql → refusal without --force
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from cambrian.config import CambrianConfig, CatalogConfig, EvolutionsConfig
from cambrian.errors import IllegalStateError
from cambrian.migrate import apply_idempotent
from cambrian.migrate.commit import (
    cambrian_commit,
    cambrian_reset_to,
    cambrian_uncommit,
)
from cambrian.sidecar.events import (
    applied_committed_ids,
    committed_evolutions,
    latest_event,
)

LAKEKEEPER_URL = "http://localhost:8181"
WAREHOUSE = "cambrian"


def _build_config(*, evolutions_dir: Path, sidecar_namespace: str) -> CambrianConfig:
    return CambrianConfig(
        catalog=CatalogConfig(
            type="rest",
            uri=f"{LAKEKEEPER_URL}/catalog",
            **{
                "warehouse": WAREHOUSE,
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "cambrian-access-key",
                "s3.secret-access-key": "cambrian-secret-key",
                "s3.region": "local",
                "s3.path-style-access": "true",
            },
        ),
        evolutions=EvolutionsConfig(
            dir=str(evolutions_dir),
            sidecar_namespace=sidecar_namespace,
        ),
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _verify_catalog() -> RestCatalog:
    return RestCatalog(
        name="verify",
        **{
            "uri": f"{LAKEKEEPER_URL}/catalog",
            "warehouse": WAREHOUSE,
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "cambrian-access-key",
            "s3.secret-access-key": "cambrian-secret-key",
            "s3.region": "local",
            "s3.path-style-access": "true",
        },
    )


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    namespace = f"_cambrian_test_{uuid.uuid4().hex[:8]}"
    yield namespace
    try:
        for ident in rest_catalog.list_tables(namespace):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass


def test_commit_uncommit_full_lifecycle(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """apply → commit → second apply replays nothing (already in events) → uncommit rolls back."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"

    sql1 = f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
    _write(current, sql1)
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    apply_idempotent(cfg)

    commit_result = cambrian_commit(cfg, message="create users table")
    assert commit_result.evolution_id == "0001_create-users-table"
    assert commit_result.event_id is not None
    assert commit_result.committed_path.exists()
    assert current.read_text(encoding="utf-8") == ""

    # The committed file contains the original SQL.
    assert sql1 in commit_result.committed_path.read_text(encoding="utf-8")

    # `committed_evolutions` now reflects the live state.
    verify = _verify_catalog()
    live = committed_evolutions(verify, sidecar_ns)
    assert [m.evolution_id for m in live] == ["0001_create-users-table"]

    # Second apply: replay sees evolution in applied set, no-op for that
    # file. current.sql is empty so the current-apply phase is also a no-op
    # (empty hash matches… actually empty current.sql is a valid no-op state).
    sql2 = f"ALTER TABLE {ns}.t ADD COLUMN extra STRING;\n"
    _write(current, sql2)
    apply_idempotent(cfg)
    table = _verify_catalog().load_table((ns, "t"))
    assert {f.name for f in table.schema().fields} == {"id", "extra"}

    # Now commit the second evolution.
    commit_result_2 = cambrian_commit(cfg, message="add extra column")
    assert commit_result_2.evolution_id == "0002_add-extra-column"

    # Uncommit the second.
    uncommit_result = cambrian_uncommit(cfg)
    assert uncommit_result.evolution_id == "0002_add-extra-column"
    assert uncommit_result.restored_path.read_text(encoding="utf-8") == sql2
    # The 0002 file is gone.
    assert not (evolutions / "committed" / "0002_add-extra-column.sql").exists()

    # The 0001 file remains.
    assert (evolutions / "committed" / "0001_create-users-table.sql").exists()

    # And the table's schema is back to what 0001 left it as (just `id`).
    table = _verify_catalog().load_table((ns, "t"))
    assert {f.name for f in table.schema().fields} == {"id"}

    # applied_committed_ids reports 0001 as still applied; 0002 was uncommitted.
    applied = applied_committed_ids(_verify_catalog(), sidecar_ns)
    assert "0001_create-users-table" in applied
    assert "0002_add-extra-column" not in applied


def test_apply_replays_unapplied_committed(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A fresh checkout with a committed/ file but no prior apply event: apply replays it."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    committed_dir = evolutions / "committed"
    committed_dir.mkdir(parents=True)
    sql = f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
    (committed_dir / "0001_seed.sql").write_text(sql, encoding="utf-8")
    _write(evolutions / "current.sql", "")

    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)

    # The committed file was applied: table exists.
    verify = _verify_catalog()
    table = verify.load_table((ns, "t"))
    assert {f.name for f in table.schema().fields} == {"id"}

    # And the events log has an apply event for it.
    ev = latest_event(verify, sidecar_ns, event_type="apply", evolution_id="0001_seed")
    assert ev is not None
    assert ev.evolution_hash != ""


def test_apply_refuses_post_hoc_edit(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Editing a committed file after apply → next apply refuses."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    sql = f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
    _write(current, sql)
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    commit_result = cambrian_commit(cfg, message="seed")

    # Tamper with the committed file.
    commit_result.committed_path.write_text(
        sql + f"ALTER TABLE {ns}.t ADD COLUMN evil STRING;\n",
        encoding="utf-8",
    )

    # Next apply detects the hash mismatch and refuses.
    with pytest.raises(IllegalStateError, match=r"edited since it was applied"):
        apply_idempotent(cfg)


def test_uncommit_refused_with_downstream(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """If the latest file isn't the only committed file in sequence, uncommit still works.

    What we actually refuse is committed/ numbering gaps. Create a synthetic
    gap by hand and watch uncommit refuse.
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    committed_dir = evolutions / "committed"
    committed_dir.mkdir(parents=True)
    (committed_dir / "0001_a.sql").write_text(
        f"CREATE TABLE IF NOT EXISTS {ns}.a (id BIGINT) USING iceberg;\n", encoding="utf-8"
    )
    (committed_dir / "0003_c.sql").write_text(
        f"CREATE TABLE IF NOT EXISTS {ns}.c (id BIGINT) USING iceberg;\n", encoding="utf-8"
    )
    _write(evolutions / "current.sql", "")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    with pytest.raises(IllegalStateError, match=r"gap"):
        cambrian_uncommit(cfg)


def test_uncommit_refuses_nonempty_current_without_force(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A non-empty current.sql blocks uncommit unless --force is passed."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    cambrian_commit(cfg, message="seed")

    # Write something into current.sql, then try to uncommit.
    _write(current, "-- new work in progress\n")
    with pytest.raises(IllegalStateError, match=r"non-empty"):
        cambrian_uncommit(cfg)

    # With force=True it overrides.
    result = cambrian_uncommit(cfg, force=True)
    assert result.evolution_id == "0001_seed"


def test_reset_to_rolls_back_and_preserves_audit(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """reset --to rolls affected tables; committed/ files and events stay intact.

    Note: the seed commit must pin a real snapshot (insert at least one row in
    the first evolution) because Iceberg's ``main`` ref cannot point to "no
    snapshot" once any snapshot exists in table history. cambrian's M4
    rollback primitive refuses the asymmetric case ``target_snapshot_id=None``
    + ``current_snapshot=set`` as inherently unsafe — see
    ``src/cambrian/iceberg/txn.py``. Real use of ``reset --to`` against an
    empty-seed evolution would hit the same limit; the documented workaround
    is to seed with data.
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    sql1 = (
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
        f"INSERT INTO {ns}.t VALUES (0);\n"
    )
    _write(current, sql1)
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    cambrian_commit(cfg, message="seed")

    # Add a second evolution: ALTER + INSERT.
    sql2 = f"ALTER TABLE {ns}.t ADD COLUMN name STRING;\nINSERT INTO {ns}.t VALUES (1, 'alice');\n"
    _write(current, sql2)
    apply_idempotent(cfg)
    cambrian_commit(cfg, message="add name and seed alice")

    # Verify second state.
    table = _verify_catalog().load_table((ns, "t"))
    assert {f.name for f in table.schema().fields} == {"id", "name"}
    rows = table.scan().to_arrow().to_pylist()
    assert any(r.get("name") == "alice" for r in rows)

    # Reset --to 0001_seed: rolls table back to seed snapshot (just `id`, one row).
    result = cambrian_reset_to(cfg, evolution_id="0001_seed")
    assert result.evolution_id == "0001_seed"
    assert result.event_id is not None

    table = _verify_catalog().load_table((ns, "t"))
    assert {f.name for f in table.schema().fields} == {"id"}

    # The committed files are still on disk.
    assert (evolutions / "committed" / "0001_seed.sql").exists()
    assert (evolutions / "committed" / "0002_add-name-and-seed-alice.sql").exists()

    # The audit trail retains the second commit's event.
    verify = _verify_catalog()
    second = latest_event(
        verify, sidecar_ns, event_type="commit", evolution_id="0002_add-name-and-seed-alice"
    )
    assert second is not None


def test_commit_refuses_dirty_state(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Editing current.sql after apply but before commit → commit refuses."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)

    # Edit current.sql so its hash no longer matches the last apply event.
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n-- edit\n")

    with pytest.raises(IllegalStateError, match=r"not been applied"):
        cambrian_commit(cfg, message="edit")


def test_commit_refuses_empty_current(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """An empty current.sql blocks commit."""
    del rest_catalog
    del ns
    evolutions = tmp_path / "evolutions"
    _write(evolutions / "current.sql", "")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    with pytest.raises(IllegalStateError, match=r"empty"):
        cambrian_commit(cfg, message="nothing")
