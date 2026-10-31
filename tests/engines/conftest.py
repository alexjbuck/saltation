"""Fixtures for the engine-compatibility suite.

saltation writes tables and views through PyIceberg against the Lakekeeper
rig; a real query engine, selected by ``SALTATION_TEST_ENGINE`` (trino, spark,
starrocks or duckdb), then reads and writes them. The session brings up
``docker/compose.yml`` plus the engine's overlay and tears both down with
``down -v``.

The suite skips when ``SALTATION_TEST_ENGINE`` is unset (so a plain
``pytest`` run never starts a second copy of the rig alongside
``tests/integration``), when docker is not on ``PATH``, or when the engine's
client library (``uv sync --group engines``) is missing.
``SALTATION_TEST_KEEP_STACK=1`` leaves the stack up afterwards (CI sets it so
a failed run can still dump the containers' logs).
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from saltation.config import CatalogConfig, EvolutionsConfig, SaltationConfig, ViewsConfig
from saltation.migrate.runner import ApplyResult, apply_idempotent
from tests.integration.conftest import (
    HEALTH_TIMEOUT_S,
    _docker_available,
    _drop_namespace_recursively,
    _wait_for_bootstrap,
    _wait_for_http_ok,
)
from tests.integration.conftest import _raise_open_file_limit as _raise_open_file_limit
from tests.integration.rig import RIGS

from .engines import ENGINES, SELECTED_ENGINE, Engine, EngineSpec

# Engines only run against Lakekeeper, whatever SALTATION_TEST_CATALOG says.
RIG = RIGS["lakekeeper"]

# After the container reports healthy, how long the driver may keep failing
# its first query (catalog registration, Spark Connect session start).
PING_TIMEOUT_S = 180.0


def _compose_cmd(spec: EngineSpec) -> list[str]:
    cmd = ["docker", "compose", "-f", str(RIG.compose_file)]
    if spec.overlay is not None:
        cmd += ["-f", str(spec.overlay)]
    return cmd


def _wait_for_healthy(compose_cmd: list[str], service: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    status = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            [*compose_cmd, "ps", "-a", "--format", "{{.State}} {{.Health}}", service],
            check=True,
            capture_output=True,
            text=True,
        )
        status = result.stdout.strip()
        if status == "running healthy":
            return
        if status.startswith(("exited", "dead")) or status.endswith("unhealthy"):
            msg = f"{service} is {status!r}; see `docker compose logs {service}`"
            raise RuntimeError(msg)
        time.sleep(5.0)
    msg = f"timed out after {timeout:.0f}s waiting for {service} to be healthy (last: {status!r})"
    raise TimeoutError(msg)


def _connect_when_ready(spec: EngineSpec, timeout: float) -> Engine:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        engine: Engine | None = None
        try:
            engine = spec.connect()
            engine.ping()
            return engine
        except Exception as err:
            last_err = err
            if engine is not None:
                try:
                    engine.close()
                except Exception:
                    pass
        time.sleep(5.0)
    msg = f"{spec.name} never answered its readiness query ({last_err!r})"
    raise TimeoutError(msg)


@pytest.fixture(scope="session", params=[SELECTED_ENGINE or "none"])
def engine(request: pytest.FixtureRequest) -> Iterator[Engine]:
    """The selected engine, connected and ready, with the rig up underneath it."""
    name = request.param
    if name == "none":
        pytest.skip("set SALTATION_TEST_ENGINE to one of: " + ", ".join(ENGINES))
    spec = ENGINES.get(name)
    if spec is None:
        pytest.fail(f"unknown SALTATION_TEST_ENGINE {name!r}; expected one of {sorted(ENGINES)}")
    if not _docker_available():
        pytest.skip("docker is not on PATH; skipping engine tests")
    if not spec.client_available():
        pytest.skip(
            f"the {spec.name} client ({spec.client_module}) is not installed; "
            "run `uv sync --group engines`"
        )

    compose_cmd = _compose_cmd(spec)
    # No `up --wait`: it treats the one-shot bootstrap container exiting 0 as
    # a failure (see tests/integration/conftest.py).
    subprocess.run([*compose_cmd, "up", "-d"], check=True)
    connected: Engine | None = None
    try:
        try:
            _wait_for_http_ok(RIG.health_url, HEALTH_TIMEOUT_S)
            _wait_for_bootstrap(compose_cmd, HEALTH_TIMEOUT_S)
            if spec.service is not None:
                _wait_for_healthy(compose_cmd, spec.service, spec.start_timeout)
            connected = _connect_when_ready(spec, PING_TIMEOUT_S)
        except Exception:
            subprocess.run([*compose_cmd, "logs", "--no-color", "--tail", "200"], check=False)
            raise
        yield connected
    finally:
        if connected is not None:
            connected.close()
        if not os.environ.get("SALTATION_TEST_KEEP_STACK"):
            subprocess.run([*compose_cmd, "down", "-v"], check=False)


@pytest.fixture(scope="session")
def rest_catalog(engine: Engine) -> RestCatalog:
    del engine  # the stack must be up
    return RestCatalog(name="saltation-engines", **RIG.properties)


@pytest.fixture
def ns(rest_catalog: RestCatalog) -> Iterator[str]:
    """A unique namespace; dropped (with its tables and views) after the test."""
    namespace = f"eng_{uuid.uuid4().hex[:12]}"
    rest_catalog.create_namespace(namespace)
    try:
        yield namespace
    finally:
        _drop_namespace_recursively(rest_catalog, namespace)


@dataclass(frozen=True)
class SaltationProject:
    """saltation's production apply path, pointed at the rig."""

    config: SaltationConfig

    @property
    def current(self) -> Path:
        return Path(self.config.evolutions.dir) / "current.sql"

    def apply(self, sql: str, *, rerun: bool = False) -> ApplyResult:
        self.current.parent.mkdir(parents=True, exist_ok=True)
        self.current.write_text(sql, encoding="utf-8")
        result = apply_idempotent(self.config, rerun=rerun)
        assert result.error is None, result.error
        return result


@pytest.fixture
def saltation(
    engine: Engine, rest_catalog: RestCatalog, tmp_path: Path
) -> Iterator[SaltationProject]:
    """A saltation project with its own evolutions dir and sidecar namespace.

    View bodies are stored in the selected engine's dialect only, so each
    engine reads exactly the representation the test wrote for it.
    """
    sidecar = f"_saltation_eng_{uuid.uuid4().hex[:8]}"
    config = SaltationConfig(
        catalog=CatalogConfig(type="rest", **RIG.properties),
        evolutions=EvolutionsConfig(dir=str(tmp_path / "evolutions"), sidecar_namespace=sidecar),
        views=ViewsConfig(dialects=[engine.view_dialect], source_dialect=engine.view_dialect),
    )
    yield SaltationProject(config=config)
    try:
        for ident in rest_catalog.list_tables(sidecar):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(sidecar)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass
