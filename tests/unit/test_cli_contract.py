"""Snapshots of saltation's public contract: CLI help, options, exit codes, JSON keys, API names.

Changing a snapshot is a public-contract change: update it with
`uv run pytest --snapshot-update` and add a `major` or `minor` change file
accordingly (see docs/compatibility.md).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import click
import griffe
import pytest
import typer.main
import typer.rich_utils
from syrupy.assertion import SnapshotAssertion
from typer.testing import CliRunner

from saltation import cli

ROOT = Path(__file__).resolve().parents[2]
_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _command_paths(cmd: click.Command, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    paths = [path]
    if isinstance(cmd, click.Group):
        for name, sub in sorted(cmd.commands.items()):
            paths += _command_paths(sub, (*path, name))
    return paths


COMMANDS = _command_paths(typer.main.get_command(cli.app))


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    out = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "check_semver.py"), "--dump-cli", "src"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return json.loads(out)


@pytest.mark.parametrize("args", COMMANDS, ids=lambda p: " ".join(("saltation", *p)))
def test_help(
    args: tuple[str, ...], snapshot: SnapshotAssertion, monkeypatch: pytest.MonkeyPatch
) -> None:
    # typer reads GITHUB_ACTIONS/FORCE_COLOR/TERMINAL_WIDTH at import time; pin what they set.
    monkeypatch.setattr(typer.rich_utils, "MAX_WIDTH", 120)
    monkeypatch.setattr(typer.rich_utils, "FORCE_TERMINAL", False)
    monkeypatch.setattr(typer.rich_utils, "COLOR_SYSTEM", None)
    env = {"TERM": "dumb", "NO_COLOR": "1", "COLUMNS": "120"}
    result = CliRunner().invoke(cli.app, [*args, "--help"], env=env)
    assert result.exit_code == 0, result.output
    text = _ANSI.sub("", result.output)
    assert "\n".join(line.rstrip() for line in text.splitlines()) == snapshot


def test_exit_codes(snapshot: SnapshotAssertion) -> None:
    codes = {name: value for name, value in vars(cli).items() if name.startswith("EXIT_")}
    assert {"EXIT_SUCCESS (implicit)": 0, "EXIT_ERROR (implicit)": 1, **codes} == snapshot


def test_options_and_arguments(contract: dict[str, Any], snapshot: SnapshotAssertion) -> None:
    assert contract["commands"] == snapshot


def test_json_payload_keys(contract: dict[str, Any], snapshot: SnapshotAssertion) -> None:
    assert contract["json_payloads"] == snapshot


def test_public_python_names(snapshot: SnapshotAssertion) -> None:
    package = griffe.load("saltation", search_paths=[ROOT / "src"])
    names: dict[str, list[str]] = {}

    def walk(module: griffe.Module) -> None:
        names[module.path] = sorted(n for n, m in module.members.items() if m.is_public)
        for sub in module.modules.values():
            walk(sub)

    assert isinstance(package, griffe.Module)
    walk(package)
    assert dict(sorted(names.items())) == snapshot
