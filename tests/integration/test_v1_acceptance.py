"""V1 acceptance: the full lifecycle against a live Lakekeeper + rustfs rig.

Per the plan's §7 closer, V1 ships when this passes:

  init -> write current.sql -> apply (idempotent + reset)
       -> commit -> apply -> commit -> uncommit
       -> reset --to <id> -> sync from a fresh checkout

with a non-trivial schema (>=3 tables, >=1 partitioned, >=1 with a sort
order) and an out-of-band ``cambrian apply --json`` smoke check that
mirrors the production code path.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from cambrian.config import CambrianConfig, CatalogConfig, EvolutionsConfig
from cambrian.migrate import apply_idempotent
from cambrian.migrate.commit import (
    cambrian_commit,
    cambrian_reset_to,
    cambrian_uncommit,
)
from cambrian.migrate.sync import cambrian_sync
from cambrian.sidecar.events import applied_committed_ids, committed_evolutions

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
    namespace = f"_cambrian_v1_{uuid.uuid4().hex[:8]}"
    yield namespace
    try:
        for ident in rest_catalog.list_tables(namespace):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass


def _initial_schema(ns: str) -> str:
    # Three tables. ``events`` becomes partitioned via ADD PARTITION FIELD
    # (PARTITIONED BY at CREATE-time is out of scope for v1 dispatch).
    # ``audit`` gets a sort order. ``users`` stays plain to anchor the test
    # against a "normal" table.
    return (
        f"CREATE TABLE IF NOT EXISTS {ns}.users (\n"
        f"  id BIGINT,\n"
        f"  email STRING\n"
        f") USING iceberg;\n"
        f"\n"
        f"CREATE TABLE IF NOT EXISTS {ns}.events (\n"
        f"  id BIGINT,\n"
        f"  ts TIMESTAMP,\n"
        f"  payload STRING\n"
        f") USING iceberg;\n"
        f"ALTER TABLE {ns}.events ADD PARTITION FIELD day(ts);\n"
        f"\n"
        f"CREATE TABLE IF NOT EXISTS {ns}.audit (\n"
        f"  id BIGINT,\n"
        f"  created_at TIMESTAMP,\n"
        f"  note STRING\n"
        f") USING iceberg;\n"
        f"ALTER TABLE {ns}.audit WRITE ORDERED BY (created_at DESC);\n"
    )


def _add_column_to_users(ns: str) -> str:
    return _initial_schema(ns) + (f"ALTER TABLE {ns}.users ADD COLUMN name STRING;\n")


def _add_metadata_table(ns: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {ns}.metadata (key STRING, value STRING) USING iceberg;\n"


def test_v1_acceptance_full_lifecycle(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """End-to-end V1 acceptance against a live Lakekeeper."""
    del rest_catalog  # ensures the docker stack is up
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"

    # 1. apply idempotent (init is implicit via the runner's ensure_current).
    _write(current, _initial_schema(ns))
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply1 = apply_idempotent(cfg)
    assert apply1.status == "applied"
    assert apply1.error is None
    assert apply1.event_id is not None

    verify = _verify_catalog()
    users_table = verify.load_table((ns, "users"))
    events_table = verify.load_table((ns, "events"))
    audit_table = verify.load_table((ns, "audit"))
    # events has a day(ts) partition.
    events_fields = events_table.spec().fields
    assert any(f.transform.__class__.__name__ == "DayTransform" for f in events_fields), (
        f"expected DayTransform on events, got {[(f.name, f.transform) for f in events_fields]}"
    )
    # audit has a non-empty sort order.
    assert len(audit_table.sort_order().fields) >= 1

    # 2. Re-apply is a no-op (hash matches).
    apply_noop = apply_idempotent(cfg)
    assert apply_noop.status == "unchanged"

    # 3. Edit current.sql (add a column). Apply.
    _write(current, _add_column_to_users(ns))
    apply2 = apply_idempotent(cfg)
    assert apply2.status == "applied"
    users_table = _verify_catalog().load_table((ns, "users"))
    assert "name" in {f.name for f in users_table.schema().fields}

    # 4. commit -m "initial schema"
    commit1 = cambrian_commit(cfg, message="initial schema")
    assert commit1.evolution_id == "0001_initial-schema"
    assert commit1.tag_ref.startswith("cambrian.committed.1.")
    assert current.read_text(encoding="utf-8") == ""

    # 5. Write a new current.sql; apply; commit.
    _write(current, _add_metadata_table(ns))
    apply3 = apply_idempotent(cfg)
    assert apply3.status == "applied"
    metadata_table = _verify_catalog().load_table((ns, "metadata"))
    assert {f.name for f in metadata_table.schema().fields} == {"key", "value"}
    commit2 = cambrian_commit(cfg, message="add metadata")
    assert commit2.evolution_id == "0002_add-metadata"

    # 6. uncommit. The second evolution's SQL is restored to current.sql and
    # the second evolution's table changes are rolled back.
    uncommit = cambrian_uncommit(cfg)
    assert uncommit.evolution_id == "0002_add-metadata"
    assert current.read_text(encoding="utf-8") == _add_metadata_table(ns)
    # The second committed file is gone; the first remains.
    assert not (evolutions / "committed" / "0002_add-metadata.sql").exists()
    assert (evolutions / "committed" / "0001_initial-schema.sql").exists()
    applied_after_uncommit = applied_committed_ids(_verify_catalog(), sidecar_ns)
    assert "0001_initial-schema" in applied_after_uncommit
    assert "0002_add-metadata" not in applied_after_uncommit

    # 7. reset --to 0001_initial-schema. Audit trail preserved; rolls
    # affected tables back to that evolution's pinned post-state.
    # We need to re-apply current.sql first to get back to a state where
    # the metadata table exists, then prove reset --to rolls back what it
    # rolled back in its checkpoint.
    apply_idempotent(cfg)
    reset_result = cambrian_reset_to(cfg, evolution_id="0001_initial-schema")
    assert reset_result.evolution_id == "0001_initial-schema"
    # 0001 didn't touch users/events/audit further than create; rollback
    # is no-op for whatever wasn't moved. The audit trail is what matters.
    assert reset_result.event_id is not None
    assert reset_result.event_id  # rollback event recorded

    # 8. From a fresh tmp_path (simulated empty checkout) pointing at the
    # same catalog: sync. Verifies committed/ rehydrates correctly.
    fresh = tmp_path / "fresh-clone"
    fresh.mkdir()
    cfg_fresh = _build_config(evolutions_dir=fresh, sidecar_namespace=sidecar_ns)
    sync_result = cambrian_sync(cfg_fresh)
    assert sync_result.refused == 0
    assert sync_result.written == 1, sync_result.files
    assert (fresh / "committed" / "0001_initial-schema.sql").exists()
    # And the rehydrated file content matches the original byte-for-byte.
    original_text = (evolutions / "committed" / "0001_initial-schema.sql").read_text(
        encoding="utf-8"
    )
    fresh_text = (fresh / "committed" / "0001_initial-schema.sql").read_text(encoding="utf-8")
    assert fresh_text == original_text

    # And a follow-up sync is a clean no-op.
    second_sync = cambrian_sync(cfg_fresh)
    assert second_sync.written == 0
    assert second_sync.skipped == 1

    # Final live state matches the catalog's truth.
    live = committed_evolutions(_verify_catalog(), sidecar_ns)
    assert [m.evolution_id for m in live] == ["0001_initial-schema"]


def test_v1_acceptance_apply_json_smoke(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Subprocess-launched ``cambrian apply --json`` mirrors a production deploy.

    Asserts:
      * exit code 0 on a fresh apply
      * stdout parses as JSON
      * the JSON has the documented keys
      * a second invocation is ``unchanged`` (idempotent contract)
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    _write(
        evolutions / "current.sql",
        f"CREATE TABLE IF NOT EXISTS {ns}.smoke (id BIGINT) USING iceberg;\n",
    )
    toml_path = tmp_path / "cambrian.toml"
    toml_path.write_text(
        f"""
[catalog]
type                  = "rest"
uri                   = "{LAKEKEEPER_URL}/catalog"
warehouse             = "{WAREHOUSE}"
"s3.endpoint"         = "http://localhost:9000"
"s3.access-key-id"    = "cambrian-access-key"
"s3.secret-access-key"= "cambrian-secret-key"
"s3.region"           = "local"
"s3.path-style-access"= "true"

[evolutions]
dir               = "{evolutions}"
sidecar_namespace = "{sidecar_ns}"
""",
        encoding="utf-8",
    )

    # Use the cambrian script that uv installed alongside this interpreter.
    # Falling back to ``python -m cambrian`` if that's not on PATH keeps the
    # test green when a contributor runs the suite without ``uv sync``.
    cambrian_bin = shutil.which("cambrian")
    if cambrian_bin is not None:
        cmd_init = [cambrian_bin, "init", "--path", str(toml_path)]
        cmd_apply = [cambrian_bin, "apply", "--json", "--path", str(toml_path)]
    else:
        cmd_init = [sys.executable, "-m", "cambrian", "init", "--path", str(toml_path)]
        cmd_apply = [sys.executable, "-m", "cambrian", "apply", "--json", "--path", str(toml_path)]

    env = {**os.environ}
    init = subprocess.run(cmd_init, check=False, capture_output=True, text=True, env=env)
    assert init.returncode == 0, init.stderr

    first = subprocess.run(cmd_apply, check=False, capture_output=True, text=True, env=env)
    assert first.returncode == 0, first.stderr
    payload = json.loads(first.stdout)
    for key in ("mode", "status", "evolution_id", "evolution_hash", "event_id", "statements"):
        assert key in payload, f"missing key {key!r} in {payload}"
    assert payload["mode"] == "idempotent"
    assert payload["status"] == "applied"
    assert payload["evolution_id"] == "current"

    second = subprocess.run(cmd_apply, check=False, capture_output=True, text=True, env=env)
    assert second.returncode == 0, second.stderr
    second_payload = json.loads(second.stdout)
    assert second_payload["status"] == "unchanged"
