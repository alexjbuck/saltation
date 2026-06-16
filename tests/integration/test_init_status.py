"""Integration tests for ``cambrian init`` and ``cambrian status``.

These exercise the full path through config + catalog + sidecar bootstrap +
read against the docker-compose stack (Lakekeeper + rustfs + Postgres).
Each test gets a fresh ``_cambrian_test_<uuid>`` sidecar namespace so they
don't collide if run in parallel.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from typer.testing import CliRunner

from cambrian.cli import EXIT_NOT_INITIALIZED, EXIT_VERSION_AHEAD, app
from cambrian.sidecar.schema import VERSION_TABLE
from tests.integration.conftest import (
    LAKEKEEPER_HOST_URL,
    RUSTFS_ACCESS_KEY,
    RUSTFS_HOST_URL,
    RUSTFS_REGION,
    RUSTFS_SECRET_KEY,
    WAREHOUSE_NAME,
    _drop_namespace_recursively,
)


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    """A unique sidecar namespace that is guaranteed-clean before and after the test."""
    namespace = f"_cambrian_test_{uuid.uuid4().hex[:12]}"
    _drop_namespace_recursively(rest_catalog, namespace)
    try:
        yield namespace
    finally:
        _drop_namespace_recursively(rest_catalog, namespace)


@pytest.fixture
def config_path(tmp_path: Path, sidecar_ns: str) -> Path:
    """Write a cambrian.toml pointed at the test rig + per-test sidecar namespace."""
    body = f"""
[catalog]
type = "rest"
uri = "{LAKEKEEPER_HOST_URL}/catalog"
warehouse = "{WAREHOUSE_NAME}"
"s3.endpoint" = "{RUSTFS_HOST_URL}"
"s3.access-key-id" = "{RUSTFS_ACCESS_KEY}"
"s3.secret-access-key" = "{RUSTFS_SECRET_KEY}"
"s3.region" = "{RUSTFS_REGION}"
"s3.path-style-access" = "true"

[evolutions]
sidecar_namespace = "{sidecar_ns}"
"""
    path = tmp_path / "cambrian.toml"
    path.write_text(body)
    return path


def _invoke(args: list[str]) -> object:
    return CliRunner().invoke(app, args)


def test_init_fresh_then_status(config_path: Path, sidecar_ns: str) -> None:
    """Init bootstraps a clean catalog; status then reports a fresh sidecar."""
    init_result = _invoke(["init", "--path", str(config_path)])
    assert init_result.exit_code == 0, init_result.output
    assert "Initialized sidecar" in init_result.output
    assert sidecar_ns in init_result.output

    status_result = _invoke(["status", "--path", str(config_path), "--json"])
    assert status_result.exit_code == 0, status_result.output
    payload = json.loads(status_result.output)
    assert payload["initialized"] is True
    assert payload["sidecar_namespace"] == sidecar_ns
    assert payload["sidecar_version"] == 1
    assert payload["is_version_ahead"] is False
    assert payload["committed_count"] == 0
    assert payload["committed_evolutions"] == []
    assert payload["current_applied"] is None


def test_init_is_idempotent(config_path: Path) -> None:
    first = _invoke(["init", "--path", str(config_path)])
    assert first.exit_code == 0, first.output
    assert "Initialized sidecar" in first.output

    second = _invoke(["init", "--path", str(config_path)])
    assert second.exit_code == 0, second.output
    assert "Already initialized" in second.output


def test_status_uninitialized_exits_with_hint(config_path: Path) -> None:
    """``status`` on a never-initialized catalog must exit non-zero with the hint."""
    result = _invoke(["status", "--path", str(config_path)])
    assert result.exit_code == EXIT_NOT_INITIALIZED, result.output
    assert "not initialized" in result.output.lower()
    assert "cambrian init" in result.output


def test_status_uninitialized_json_payload(config_path: Path) -> None:
    result = _invoke(["status", "--path", str(config_path), "--json"])
    assert result.exit_code == EXIT_NOT_INITIALIZED, result.output
    payload = json.loads(result.output)
    assert payload["initialized"] is False
    assert payload["hint"] == "run `cambrian init`"


def test_status_version_ahead_warns_but_succeeds(
    config_path: Path, sidecar_ns: str, rest_catalog: RestCatalog
) -> None:
    """If the persisted version is ahead, ``status`` is read-only-OK but ``init`` refuses."""
    # Bootstrap, then hand-write a version row of 999.
    init = _invoke(["init", "--path", str(config_path)])
    assert init.exit_code == 0, init.output

    version_table = rest_catalog.load_table((sidecar_ns, VERSION_TABLE))
    # PyIceberg matches PyArrow nullability against Iceberg required flags
    # exactly, so we have to pin nullable=False on the column we hand-write.
    version_table.append(
        pa.table(
            {"version": pa.array([999], type=pa.int64())},
            schema=pa.schema([pa.field("version", pa.int64(), nullable=False)]),
        )
    )

    status = _invoke(["status", "--path", str(config_path), "--json"])
    assert status.exit_code == 0, status.output
    payload = json.loads(status.output)
    assert payload["initialized"] is True
    assert payload["sidecar_version"] == 999
    assert payload["is_version_ahead"] is True

    refused = _invoke(["init", "--path", str(config_path)])
    assert refused.exit_code == EXIT_VERSION_AHEAD, refused.output
    assert "999" in refused.output
