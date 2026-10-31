"""Fail when the Python API or CLI contract breaks without a `major` change file.

Usage: uv run python scripts/check_semver.py --base origin/main [--explain] [--style github]
"""

from __future__ import annotations

import argparse
import importlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import griffe

PACKAGE = "saltation"
LEVELS = ("major", "minor", "patch")
ROOT = Path(__file__).resolve().parent.parent
VALUE = "<value>"

PAYLOADS: dict[str, tuple[str, str, int]] = {
    "apply": ("saltation.cli", "_apply_payload", 1),
    "apply --reset / redo / rollback": ("saltation.cli", "_reset_payload", 1),
    "commit": ("saltation.cli", "_commit_payload", 1),
    "uncommit": ("saltation.cli", "_uncommit_payload", 1),
    "reset-to": ("saltation.cli", "_reset_to_payload", 1),
    "sync / download": ("saltation.cli", "_sync_payload", 1),
    "status": ("saltation.cli", "_status_payload", 3),
    "watch": ("saltation.migrate.watch", "_format_json", 1),
}


def _git(*args: str) -> list[str]:
    out = subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    return [line for line in out.splitlines() if line]


# --- Python API ---------------------------------------------------------------


def api_breaking_changes(base: str) -> list[griffe.Breakage]:
    new = griffe.load(PACKAGE, search_paths=[ROOT / "src"], resolve_aliases=True)
    try:
        old = griffe.load_git(
            PACKAGE, ref=base, repo=ROOT, search_paths=["src"], resolve_aliases=True
        )
    except ModuleNotFoundError:
        print(f"note: {base} has no {PACKAGE} package; no Python API to compare against")
        return []
    return list(griffe.find_breaking_changes(old, new))


# --- CLI contract -------------------------------------------------------------


class _Stub:
    """Stands in for any result object so a payload builder reveals its keys."""

    def __getattr__(self, name: str) -> _Stub:
        return _Stub()

    def __call__(self, *args: object, **kwargs: object) -> _Stub:
        return _Stub()

    def __iter__(self) -> Iterator[_Stub]:
        return iter([_Stub()])

    def __len__(self) -> int:
        return 1

    def __getitem__(self, key: object) -> str:
        return VALUE

    def __str__(self) -> str:
        return VALUE

    __repr__ = __str__


def _shape(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_shape(v) for v in value[:1]]
    if isinstance(value, str | bool) or value is None:
        return value
    return VALUE


def cli_contract() -> dict[str, Any]:
    """The CLI surface of the importable ``saltation`` as plain JSON-able data."""
    import click
    import typer.main

    cli = importlib.import_module("saltation.cli")
    commands: dict[str, list[dict[str, Any]]] = {}

    def walk(cmd: click.Command, path: str) -> None:
        commands[path] = [
            {
                "kind": "argument" if isinstance(p, click.Argument) else "option",
                "decls": [*p.opts, *p.secondary_opts],
                "required": p.required,
                "flag": bool(getattr(p, "is_flag", False)),
                "default": None if callable(p.default) else _shape(str(p.default)),
            }
            for p in cmd.params
        ]
        if isinstance(cmd, click.Group):
            for name, sub in sorted(cmd.commands.items()):
                walk(sub, f"{path} {name}")

    walk(typer.main.get_command(cli.app), PACKAGE)

    payloads: dict[str, Any] = {}
    for name, (module, func, arity) in PAYLOADS.items():
        builder: Callable[..., Any] | None = getattr(importlib.import_module(module), func, None)
        if builder is None:
            payloads[name] = None
            continue
        out = builder(*(_Stub() for _ in range(arity)))
        payloads[name] = _shape(json.loads(out) if isinstance(out, str) else out)

    exit_codes = {k: v for k, v in vars(cli).items() if k.startswith("EXIT_")}
    return {"commands": commands, "exit_codes": exit_codes, "json_payloads": payloads}


def _contract_from(src: Path) -> dict[str, Any] | None:
    env = {**os.environ, "PYTHONPATH": str(src)}
    proc = subprocess.run(
        [sys.executable, __file__, "--dump-cli", str(src)],
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        print(f"warning: could not load the CLI from {src}:\n{proc.stderr.strip()}")
        return None
    return json.loads(proc.stdout)


def base_cli_contract(base: str) -> dict[str, Any] | None:
    archive = subprocess.run(
        ["git", "archive", "--format=tar", base, f"src/{PACKAGE}"],
        cwd=ROOT,
        capture_output=True,
    )
    if archive.returncode != 0:
        print(f"note: {base} has no src/{PACKAGE}; no CLI to compare against")
        return None
    with tempfile.TemporaryDirectory(prefix="saltation-semver-") as tmp:
        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
            tar.extractall(tmp, filter="data")
        return _contract_from(Path(tmp) / "src")


def _flatten(shape: Any, prefix: str) -> dict[str, Any]:
    if isinstance(shape, dict):
        out: dict[str, Any] = {prefix: "{}"}
        for key, value in shape.items():
            out |= _flatten(value, f"{prefix}.{key}")
        return out
    if isinstance(shape, list):
        return {prefix: "[]"} | (_flatten(shape[0], f"{prefix}[]") if shape else {})
    return {prefix: shape}


def cli_breaking_changes(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for path, old_params in old["commands"].items():
        if path not in new["commands"]:
            found.append(f"`{path}`: command was removed")
            continue
        new_params = new["commands"][path]
        found += [f"`{path}` {msg}" for msg in _param_breaks(old_params, new_params)]

    for name, code in old["exit_codes"].items():
        if new["exit_codes"].get(name) != code:
            found.append(f"exit code {name}={code} is now {new['exit_codes'].get(name)}")

    for name, old_shape in old["json_payloads"].items():
        new_shape = new["json_payloads"].get(name)
        if old_shape is None or new_shape is None:
            continue
        before, after = _flatten(old_shape, "$"), _flatten(new_shape, "$")
        for key, value in before.items():
            if key not in after:
                found.append(f"`{name} --json`: {key} was removed")
            elif value != VALUE and after[key] != value:
                found.append(f"`{name} --json`: {key} changed from {value!r} to {after[key]!r}")
    return found


def _param_breaks(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> Iterator[str]:
    old_args = [p for p in old if p["kind"] == "argument"]
    new_args = [p for p in new if p["kind"] == "argument"]
    if len(new_args) < len(old_args):
        yield f"positional argument(s) removed ({len(old_args)} -> {len(new_args)})"
    for p in new_args[len(old_args) :]:
        if p["required"]:
            yield f"new required argument {p['decls'][0]}"

    new_opts = [p for p in new if p["kind"] == "option"]
    matched: list[dict[str, Any]] = []
    for o in (p for p in old if p["kind"] == "option"):
        n = next((n for n in new_opts if set(n["decls"]) & set(o["decls"])), None)
        if n is None:
            yield f"option {'/'.join(o['decls'])} was removed"
            continue
        matched.append(n)
        for decl in sorted(set(o["decls"]) - set(n["decls"])):
            yield f"option alias {decl} was removed"
        if n["required"] and not o["required"]:
            yield f"option {o['decls'][0]} became required"
        if o["flag"] and not n["flag"]:
            yield f"option {o['decls'][0]} now takes a value"
        if o["default"] != n["default"]:
            yield f"default of {o['decls'][0]} changed from {o['default']!r} to {n['default']!r}"
    for n in new_opts:
        if n not in matched and n["required"]:
            yield f"new required option {n['decls'][0]}"


# --- Change files -------------------------------------------------------------


def change_files(base: str) -> list[Path]:
    pattern = ".changeset/*.md"
    names = {
        *_git("diff", "--name-only", "--diff-filter=AM", f"{base}...HEAD", "--", pattern),
        *_git("diff", "--name-only", "--diff-filter=AM", "HEAD", "--", pattern),
        *_git("ls-files", "--others", "--exclude-standard", "--", pattern),
    }
    return sorted(ROOT / name for name in names if (ROOT / name).is_file())


def change_level(path: Path) -> str | None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:]:
        if line.strip() == "---":
            break
        key, sep, value = line.partition(":")
        if sep and key.strip() == "default":
            level = value.strip().strip("'\"").lower()
            return level if level in LEVELS else None
    return None


# --- Entry point --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--base", default="origin/main", help="git ref to compare against")
    parser.add_argument(
        "--explain", action="store_true", help="list breaking changes without failing"
    )
    parser.add_argument(
        "--style",
        default=griffe.ExplanationStyle.ONE_LINE.value,
        choices=[s.value for s in griffe.ExplanationStyle],
        help="griffe explanation style for each breaking change",
    )
    parser.add_argument("--dump-cli", metavar="SRC", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.dump_cli:
        sys.path.insert(0, args.dump_cli)
        print(json.dumps(cli_contract(), indent=2, sort_keys=True))
        return 0

    try:
        breakages = api_breaking_changes(args.base)
        old_cli, new_cli = base_cli_contract(args.base), _contract_from(ROOT / "src")
        files = change_files(args.base)
    except subprocess.CalledProcessError as err:
        print(f"error: git {' '.join(err.cmd[1:])} failed: {err.stderr.strip()}", file=sys.stderr)
        return 2
    if new_cli is None:
        return 2

    style = griffe.ExplanationStyle(args.style)
    for breakage in breakages:
        print(breakage.explain(style=style))
    cli_breaks = cli_breaking_changes(old_cli, new_cli) if old_cli else []
    for msg in cli_breaks:
        prefix = "::warning title=CLI contract::" if style is griffe.ExplanationStyle.GITHUB else ""
        print(f"{prefix}{msg}")

    levels = {path.relative_to(ROOT).as_posix(): change_level(path) for path in files}
    for name, level in levels.items():
        print(f"change file: {name} ({level or 'no valid `default:` level'})")

    total = len(breakages) + len(cli_breaks)
    if not total:
        print(f"no breaking changes to the {PACKAGE} Python API or CLI since {args.base}")
        return 0
    print(
        f"{len(breakages)} Python API and {len(cli_breaks)} CLI breaking change(s) "
        f"since {args.base}"
    )
    if "major" in levels.values():
        print("a `major` change file declares them")
        return 0
    if args.explain:
        return 0
    print(
        "error: these break saltation's public contract but no change file declares\n"
        "  `default: major`. Keep the old name/option/key working (deprecate it instead),\n"
        "  or add .changeset/<slug>.md with `default: major` describing the break.\n"
        "  See docs/compatibility.md.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
