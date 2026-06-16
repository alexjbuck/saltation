"""Fixtures for the integration test suite.

These tests run against the docker-compose stack defined in ``docker/compose.yml``
(Lakekeeper + rustfs + Postgres). The stack is brought up once per test session
and torn down at the end.

If ``docker`` is not on ``PATH`` the whole integration suite is skipped, so
contributors who only care about unit tests aren't blocked.
"""

from __future__ import annotations

import os
import resource
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

# Coordinates for the test rig as exposed to the host (compose maps these
# ports out of the cambrian-test network). When talking to Lakekeeper from
# inside the network the URLs would use the service names instead.
LAKEKEEPER_HOST_URL = os.environ.get("CAMBRIAN_TEST_LAKEKEEPER_URL", "http://localhost:8181")
RUSTFS_HOST_URL = os.environ.get("CAMBRIAN_TEST_RUSTFS_URL", "http://localhost:9000")
WAREHOUSE_NAME = os.environ.get("CAMBRIAN_TEST_WAREHOUSE", "cambrian")
RUSTFS_ACCESS_KEY = os.environ.get("CAMBRIAN_TEST_RUSTFS_ACCESS_KEY", "cambrian-access-key")
RUSTFS_SECRET_KEY = os.environ.get("CAMBRIAN_TEST_RUSTFS_SECRET_KEY", "cambrian-secret-key")
RUSTFS_REGION = os.environ.get("CAMBRIAN_TEST_RUSTFS_REGION", "local")

COMPOSE_FILE = Path(__file__).resolve().parents[2] / "docker" / "compose.yml"

# How long the per-step polling waits for individual services to become
# ready (lakekeeper /health, bootstrap exit-zero). The whole stack should
# settle in well under a minute on a warm-image laptop / runner.
HEALTH_TIMEOUT_S = 90.0


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _wait_for_http_ok(url: str, timeout: float) -> None:
    """Poll an HTTP endpoint until it returns 2xx, raising on timeout."""
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                if 200 <= resp.status < 300:
                    return
                last_err = RuntimeError(f"HTTP {resp.status}")
        except (urllib.error.URLError, OSError) as err:
            last_err = err
        time.sleep(1.0)
    msg = f"timed out waiting for {url} ({last_err!r})"
    raise TimeoutError(msg)


def _bootstrap_completed(compose_cmd: list[str]) -> bool:
    """Return True iff the one-shot ``bootstrap`` container has exited 0."""
    result = subprocess.run(
        [*compose_cmd, "ps", "-a", "--format", "{{.Service}} {{.ExitCode}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "bootstrap" and parts[1] == "0":
            return True
    return False


def _wait_for_bootstrap(compose_cmd: list[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _bootstrap_completed(compose_cmd):
            return
        time.sleep(1.0)
    msg = "timed out waiting for the bootstrap container to exit successfully"
    raise TimeoutError(msg)


@pytest.fixture(scope="session")
def compose_stack() -> Iterator[None]:
    """Bring up the docker-compose rig for the whole test session."""
    if not _docker_available():
        pytest.skip("docker is not on PATH; skipping integration tests")

    if not COMPOSE_FILE.exists():
        pytest.fail(f"compose file not found at {COMPOSE_FILE}")

    compose_cmd = ["docker", "compose", "-f", str(COMPOSE_FILE)]

    # We deliberately don't use ``up --wait`` here. ``--wait`` treats a
    # one-shot container (``bootstrap``) exiting as a stack failure even
    # when it exits 0, so we poll for readiness ourselves: Lakekeeper
    # /health for the long-running services, plus an explicit check that
    # bootstrap exited successfully.
    subprocess.run([*compose_cmd, "up", "-d"], check=True)

    try:
        _wait_for_http_ok(f"{LAKEKEEPER_HOST_URL}/health", HEALTH_TIMEOUT_S)
        _wait_for_bootstrap(compose_cmd, HEALTH_TIMEOUT_S)
        yield
    finally:
        subprocess.run([*compose_cmd, "down", "-v"], check=False)


@pytest.fixture(scope="session", autouse=True)
def _raise_open_file_limit() -> None:
    """Raise the session's soft open-file limit.

    The integration suite reaches rustfs through PyArrow's ``S3FileSystem``,
    which keeps per-filesystem connection pools, and uses watchfiles for the
    watch tests — together that needs far more file descriptors than macOS's
    default soft limit of 256. Without headroom the later tests in a run fail
    with "Too many open files" (or a PyArrow socket/thread-start error, which
    is the same exhaustion). Raise toward a generous target, capped by the
    hard limit and whatever the OS will accept.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    for target in (8192, 4096, 2048, 1024):
        if target <= soft or (hard != resource.RLIM_INFINITY and target > hard):
            continue
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            return
        except (ValueError, OSError):
            continue


@pytest.fixture(scope="session")
def rest_catalog(compose_stack: None) -> RestCatalog:
    """A PyIceberg ``RestCatalog`` configured for the test rig.

    The S3 credentials are set on the catalog client too so that PyArrow
    file IO can talk to rustfs directly. Some Lakekeeper versions vend
    short-lived credentials in the table-load response and the client
    doesn't need its own, but pinning static creds here is robust against
    that variability for the smoke test.
    """
    del compose_stack  # ensures the fixture order: stack must be up
    return RestCatalog(
        name="cambrian-test",
        **{
            # Lakekeeper exposes the Iceberg REST API at /catalog. The
            # warehouse is selected by the ``warehouse`` config below; the
            # ``prefix`` Lakekeeper hands back from /v1/config encodes
            # the warehouse id used in subsequent table URLs.
            "uri": f"{LAKEKEEPER_HOST_URL}/catalog",
            "warehouse": WAREHOUSE_NAME,
            # PyIceberg auto-selects PyArrowFileIO for ``s3://`` table
            # locations when no ``py-io-impl`` override is sent by the
            # server, which is the case when the warehouse is configured
            # with remote-signing disabled (see docker/bootstrap.sh).
            "s3.endpoint": RUSTFS_HOST_URL,
            "s3.access-key-id": RUSTFS_ACCESS_KEY,
            "s3.secret-access-key": RUSTFS_SECRET_KEY,
            "s3.region": RUSTFS_REGION,
            "s3.path-style-access": "true",
        },
    )


def _drop_namespace_recursively(catalog: Catalog, namespace: str) -> None:
    """Drop every table in *namespace* and then the namespace itself."""
    try:
        identifiers = catalog.list_tables(namespace)
    except NoSuchNamespaceError:
        return
    for ident in identifiers:
        catalog.drop_table(ident)
    try:
        catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        # Best-effort teardown - if a downstream test left junk behind we
        # don't want that to mask the real test failure.
        pass


@pytest.fixture
def ns(rest_catalog: RestCatalog) -> Iterator[str]:
    """A unique, freshly-created namespace; dropped (with tables) after the test."""
    namespace = f"test_{uuid.uuid4().hex[:12]}"
    rest_catalog.create_namespace(namespace)
    try:
        yield namespace
    finally:
        _drop_namespace_recursively(rest_catalog, namespace)
