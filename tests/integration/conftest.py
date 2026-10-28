"""Fixtures for the integration test suite.

These tests run against the docker-compose stack of the catalog selected by
``SALTATION_TEST_CATALOG`` (see ``rig.py``; Lakekeeper by default). The stack is
brought up once per test session and torn down at the end.

If ``docker`` is not on ``PATH`` the whole integration suite is skipped, so
contributors who only care about unit tests aren't blocked.
"""

from __future__ import annotations

import resource
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator

import pytest
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from .rig import RIG
from .rig import rest_catalog as rig_catalog

# How long the per-step polling waits for individual services to become
# ready (catalog health, bootstrap exit-zero). The whole stack should
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
        [*compose_cmd, "ps", "-a", "--format", "{{.Service}} {{.State}} {{.ExitCode}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    # A running container also reports exit code 0.
    return "bootstrap exited 0" in result.stdout.splitlines()


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

    if not RIG.compose_file.exists():
        pytest.fail(f"compose file not found at {RIG.compose_file}")

    compose_cmd = ["docker", "compose", "-f", str(RIG.compose_file)]

    # We deliberately don't use ``up --wait`` here. ``--wait`` treats a
    # one-shot container (``bootstrap``) exiting as a stack failure even
    # when it exits 0, so we poll for readiness ourselves: the catalog's
    # health endpoint for the long-running services, plus an explicit check
    # that bootstrap exited successfully.
    subprocess.run([*compose_cmd, "up", "-d"], check=True)

    try:
        _wait_for_http_ok(RIG.health_url, HEALTH_TIMEOUT_S)
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
    """A PyIceberg ``RestCatalog`` for the rig, with static rustfs credentials.

    Pinning the S3 credentials on the client lets PyArrow reach rustfs
    whether or not the catalog vends credentials in its load-table response.
    """
    del compose_stack  # ensures the fixture order: stack must be up
    return rig_catalog()


def _drop_namespace_recursively(catalog: Catalog, namespace: str) -> None:
    """Drop every view and table in *namespace* and then the namespace itself."""
    try:
        identifiers = catalog.list_tables(namespace)
    except NoSuchNamespaceError:
        return
    for view in catalog.list_views(namespace):
        catalog.drop_view(view)
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
