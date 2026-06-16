"""Integration tests for ``cambrian apply`` in idempotent mode.

These exercise the full M5 stack against a live Lakekeeper rig:

* include resolution + hashing
* sqlglot dialect parsing
* dispatch (CREATE, INSERT VALUES, ALTER, partition fields, props)
* sidecar event/state writes
* hash-driven short-circuit on re-apply

Tests use the ``rest_catalog`` and ``ns`` fixtures from ``conftest.py``.
The runner needs a config that points at the same catalog; we build one
synthetically per test rather than reading a TOML.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog

from cambrian.config import CambrianConfig, CatalogConfig, EvolutionsConfig
from cambrian.errors import UnsupportedStatementError
from cambrian.migrate import apply_idempotent
from cambrian.sidecar.events import latest_event

# Re-use the rig coordinates from conftest.py. Keeping these as constants
# rather than importing the fixture's internals so the integration tests
# stay independent of conftest implementation details.
LAKEKEEPER_URL = "http://localhost:8181"
WAREHOUSE = "cambrian"


def _build_config(
    *,
    evolutions_dir: Path,
    sidecar_namespace: str,
) -> CambrianConfig:
    """Construct a CambrianConfig pointing at the test rig.

    Bypasses TOML parsing so each test can spin up a unique sidecar namespace
    without touching the filesystem outside ``tmp_path``.
    """
    return CambrianConfig(
        catalog=CatalogConfig(
            type="rest",
            uri=f"{LAKEKEEPER_URL}/catalog",
            **{  # extras forwarded to PyIceberg
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


def _write_current(evolutions_dir: Path, sql: str) -> Path:
    """Write a ``current.sql`` into *evolutions_dir* and return the path."""
    evolutions_dir.mkdir(parents=True, exist_ok=True)
    p = evolutions_dir / "current.sql"
    p.write_text(sql, encoding="utf-8")
    return p


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    """A unique sidecar namespace per test; cleaned up after.

    cambrian's own ``_cambrian_test_<uuid>`` namespace; on teardown drop the
    three sidecar tables we created. The ``ns`` fixture from conftest is
    used separately for user-facing tables.
    """
    namespace = f"_cambrian_test_{uuid.uuid4().hex[:8]}"
    yield namespace
    # Teardown: drop sidecar tables (if present) and the namespace.
    from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

    try:
        for ident in rest_catalog.list_tables(namespace):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass


def test_apply_creates_namespace_and_table(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Apply a fresh current.sql; verify tables created; re-apply is a no-op."""
    del rest_catalog  # ensure docker-stack is up
    sql = (
        f"CREATE TABLE {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
        f"INSERT INTO {ns}.t VALUES (1, 'alice'), (2, 'bob');\n"
    )
    _write_current(tmp_path / "evolutions", sql)
    cfg = _build_config(
        evolutions_dir=tmp_path / "evolutions",
        sidecar_namespace=sidecar_ns,
    )

    result1 = apply_idempotent(cfg)
    assert result1.status == "applied", result1
    assert result1.error is None

    # Verify table exists and has two rows.
    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    table = catalog.load_table((ns, "t"))
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 2
    assert sorted(arrow.column("name").to_pylist()) == ["alice", "bob"]

    # Re-apply: hash matches → no-op.
    result2 = apply_idempotent(cfg)
    assert result2.status == "unchanged"
    assert result2.event_id is None  # no event written on no-op


def test_apply_edit_reapplies(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Apply, edit current.sql (add ALTER), re-apply: new column exists."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    _write_current(
        evolutions,
        f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    result1 = apply_idempotent(cfg)
    assert result1.status == "applied"

    # Edit: add an ALTER. Even without IF NOT EXISTS, the runner is
    # idempotent — but a fresh column on the next pass should land.
    _write_current(
        evolutions,
        (
            f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\n"
            f"ALTER TABLE {ns}.t ADD COLUMN added STRING;\n"
        ),
    )
    result2 = apply_idempotent(cfg)
    assert result2.status == "applied"

    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    table = catalog.load_table((ns, "t"))
    fields = [f.name for f in table.schema().fields]
    assert "added" in fields


def test_apply_multi_column_alter_splits(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """``ADD COLUMNS (a, b, c)`` produces three sequential metadata commits."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    _write_current(
        evolutions,
        (
            f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\n"
            f"ALTER TABLE {ns}.t ADD COLUMNS (a INT, b INT, c INT);\n"
        ),
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    result = apply_idempotent(cfg)
    assert result.status == "applied"

    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    table = catalog.load_table((ns, "t"))
    names = [f.name for f in table.schema().fields]
    assert names == ["id", "a", "b", "c"]
    # Each column add is its own schema commit → 4 distinct schema_ids in
    # the table's history (initial create + 3 adds). We can verify via the
    # schemas dict on the metadata.
    schemas = table.metadata.schemas
    assert len(schemas) == 4


def test_apply_include(rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str) -> None:
    """``--! include current/*.sql`` pulls in glob-sorted child files."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    evolutions.mkdir(parents=True)
    children = evolutions / "current"
    children.mkdir()
    (children / "10_create.sql").write_text(
        f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\n",
        encoding="utf-8",
    )
    (children / "20_insert.sql").write_text(
        f"INSERT INTO {ns}.t VALUES (42);\n",
        encoding="utf-8",
    )
    (evolutions / "current.sql").write_text(
        "--! include current/*.sql\n",
        encoding="utf-8",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    result = apply_idempotent(cfg)
    assert result.status == "applied", result

    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    table = catalog.load_table((ns, "t"))
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 1
    assert arrow.column("id").to_pylist() == [42]


def test_apply_unsupported_statement_errors(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """``INSERT ... SELECT`` raises UnsupportedStatementError under default mode."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    # Need a real source table; we don't actually run the SELECT, just the
    # parser/dispatch path.
    _write_current(
        evolutions,
        (
            f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\n"
            f"INSERT INTO {ns}.t SELECT * FROM {ns}.other;\n"
        ),
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    with pytest.raises(UnsupportedStatementError):
        apply_idempotent(cfg)


def test_apply_idempotent_redo(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Applying ``CREATE TABLE IF NOT EXISTS`` twice: second is no-op via hash."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    _write_current(
        evolutions,
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    result1 = apply_idempotent(cfg)
    assert result1.status == "applied"
    result2 = apply_idempotent(cfg)
    assert result2.status == "unchanged"


def test_apply_emits_event_with_table_states(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A successful apply writes an event with the new hash + table_states rows."""
    sql = f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg;\nINSERT INTO {ns}.t VALUES (1);\n"
    _write_current(tmp_path / "evolutions", sql)
    cfg = _build_config(
        evolutions_dir=tmp_path / "evolutions",
        sidecar_namespace=sidecar_ns,
    )
    result = apply_idempotent(cfg)
    assert result.status == "applied"
    assert result.event_id is not None

    # Look up the event we just emitted.
    event = latest_event(rest_catalog, sidecar_ns, event_type="apply", evolution_id="current")
    assert event is not None
    assert event.event_id == result.event_id
    assert event.evolution_hash == result.evolution_hash
    assert event.evolution_sql == sql
