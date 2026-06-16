"""Unit tests for ``cambrian.catalog``.

We don't actually want to talk to a real catalog from a unit test, so we
monkeypatch :func:`pyiceberg.catalog.load_catalog` and assert the kwargs we
forward. This validates the passthrough contract without any network I/O.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cambrian import catalog as cambrian_catalog
from cambrian.catalog import CATALOG_NAME, load_catalog
from cambrian.config import load_config


class _FakeCatalog:
    """Sentinel returned by the patched pyiceberg.load_catalog."""


def _capture_load_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake(name: str | None = None, **properties: Any) -> _FakeCatalog:
        captured["name"] = name
        captured["properties"] = properties
        return _FakeCatalog()

    monkeypatch.setattr(cambrian_catalog, "_pyiceberg_load_catalog", fake)
    return captured


def _write_cfg(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "cambrian.toml"
    path.write_text(body)
    return path


def test_load_catalog_forwards_required_kwargs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = _capture_load_catalog(monkeypatch)
    cfg = load_config(
        _write_cfg(
            tmp_path,
            """
[catalog]
type = "rest"
uri = "http://localhost:8181"
""",
        )
    )
    result = load_catalog(cfg)
    assert isinstance(result, _FakeCatalog)
    assert captured["name"] == CATALOG_NAME
    assert captured["properties"]["type"] == "rest"
    assert captured["properties"]["uri"] == "http://localhost:8181"


def test_load_catalog_passes_extras_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = _capture_load_catalog(monkeypatch)
    cfg = load_config(
        _write_cfg(
            tmp_path,
            """
[catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://bucket/"
token = "abc"
credential = "id:secret"
""",
        )
    )
    load_catalog(cfg)
    props = captured["properties"]
    assert props["warehouse"] == "s3://bucket/"
    assert props["token"] == "abc"
    assert props["credential"] == "id:secret"


def test_load_catalog_name_is_hardcoded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _capture_load_catalog(monkeypatch)
    cfg = load_config(
        _write_cfg(
            tmp_path,
            """
[catalog]
type = "sql"
uri = "sqlite:///:memory:"
""",
        )
    )
    load_catalog(cfg)
    # PyIceberg uses this name only for its config-file lookup; we always
    # pass "cambrian" so behavior is reproducible.
    assert captured["name"] == "cambrian"
