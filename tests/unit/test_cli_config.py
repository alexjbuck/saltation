"""CLI tests for the ``cambrian config`` sub-app."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from cambrian.cli import app


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "cambrian.toml"
    path.write_text(body)
    return path


def test_config_check_succeeds(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
""",
    )
    result = CliRunner().invoke(app, ["config", "check", "--path", str(path)])
    assert result.exit_code == 0, result.output
    assert "Config valid" in result.output
    assert str(path) in result.output


def test_config_check_fails_on_missing_file(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, ["config", "check", "--path", str(tmp_path / "nope.toml")])
    assert result.exit_code != 0
    # CliRunner mixes stderr+stdout into .output by default in modern click.
    assert "error" in result.output.lower()
    assert "nope.toml" in result.output


def test_config_show_redacts_secrets_json(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
token = "super-secret"
warehouse = "s3://bucket/"
""",
    )
    result = CliRunner().invoke(app, ["config", "show", "--path", str(path), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["catalog"]["token"] == "***"
    assert payload["catalog"]["warehouse"] == "s3://bucket/"
    assert payload["catalog"]["uri"] == "http://localhost:8181"


def test_config_show_human_view(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
token = "super-secret"
""",
    )
    result = CliRunner().invoke(app, ["config", "show", "--path", str(path)])
    assert result.exit_code == 0, result.output
    # Token is redacted, secret value is not present.
    assert "super-secret" not in result.output
    assert "***" in result.output
    # Section header is rendered.
    assert "[catalog]" in result.output
