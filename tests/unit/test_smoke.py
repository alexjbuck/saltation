"""Sanity tests that the package imports and exposes its version + CLI."""

from typer.testing import CliRunner

import saltation
from saltation.cli import app


def test_version_string_is_set() -> None:
    assert isinstance(saltation.__version__, str)
    assert saltation.__version__


def test_cli_version_flag() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert saltation.__version__ in result.stdout


def test_cli_help_renders() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "saltation" in result.stdout.lower()
