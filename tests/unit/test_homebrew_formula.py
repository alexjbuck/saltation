"""scripts/render_homebrew_formula.py and the Formula/saltation.rb it generates."""

from __future__ import annotations

import importlib.util
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
FORMULA = ROOT / "Formula" / "saltation.rb"


def _load_script() -> Any:
    path = ROOT / "scripts" / "render_homebrew_formula.py"
    spec = importlib.util.spec_from_file_location("render_homebrew_formula", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


render_script = _load_script()
LOCK = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _render(stable: Any = None) -> str:
    return render_script.render(render_script.resolve_resources(LOCK, PYPROJECT), PYPROJECT, stable)


def _resource_names(formula: str) -> list[str]:
    return re.findall(r'^  resource "([^"]+)" do$', formula, re.MULTILINE)


def _resource_body(formula: str, name: str) -> str:
    m = re.search(rf'^  resource "{re.escape(name)}" do\n(.*?)^  end$', formula, re.M | re.S)
    assert m, name
    return m.group(1)


def test_render_is_deterministic() -> None:
    assert _render() == _render()


def test_every_runtime_dependency_appears_exactly_once() -> None:
    formula = _render()
    names = _resource_names(formula)
    assert names == sorted(set(names))
    for requirement in PYPROJECT["project"]["dependencies"]:
        direct = re.match(r"[A-Za-z0-9._-]+", requirement)
        assert direct
        assert names.count(direct.group(0).lower().replace("_", "-")) == 1, requirement
    # Reached only through the pyiceberg-core extra, and through pydantic.
    assert {"pyiceberg-core", "pydantic-core", "typing-extensions"} <= set(names)
    # click's colorama is Windows-only; the closure never includes dev groups.
    assert not {"colorama", "pytest", "ruff", "saltation"} & set(names)


def test_compiled_dependencies_are_pinned_wheels_per_platform() -> None:
    formula = _render()
    pyarrow = _resource_body(formula, "pyarrow")
    wheels = re.findall(r'url "\S+/(pyarrow-[^"]+\.whl)"', pyarrow)
    assert len(wheels) == 4
    assert all("-cp314-cp314-" in w for w in wheels)
    assert re.search(r"on_macos do\n\s+on_arm do\n\s+url \S+macosx_\d+_\d+_arm64\.whl", pyarrow)
    assert re.search(r"on_linux do\n\s+on_arm do\n\s+url \S+_aarch64\.whl", pyarrow)
    assert ".tar.gz" in _resource_body(formula, "typer")
    assert ".tar.gz" in _resource_body(formula, "charset-normalizer")


def test_committed_formula_is_up_to_date() -> None:
    committed = FORMULA.read_text(encoding="utf-8")
    fresh = _render(render_script.existing_stable(committed))
    assert committed == fresh, (
        "Formula/saltation.rb is stale (uv.lock or the renderer changed); "
        "regenerate it with `uv run python scripts/render_homebrew_formula.py`"
    )


def test_stable_url_renders_and_survives_a_rerender() -> None:
    stable = render_script.Stable(
        "https://github.com/alexjbuck/saltation/releases/download/v9.9.9/saltation-9.9.9.tar.gz",
        "ab" * 32,
    )
    formula = _render(stable)
    assert f'  homepage "https://github.com/alexjbuck/saltation"\n  url "{stable.url}"\n' in formula
    assert render_script.existing_stable(formula) == stable
    assert render_script.existing_stable(_render()) is None


def test_a_missing_platform_wheel_fails_the_render() -> None:
    def wheel(tag: str) -> dict[str, str]:
        return {
            "url": f"https://example.invalid/native-1.0-cp314-cp314-{tag}.whl",
            "hash": "sha256:00",
        }

    lock = {
        "package": [
            {
                "name": "demo",
                "version": "1.0",
                "source": {"editable": "."},
                "dependencies": [{"name": "native"}],
            },
            {
                "name": "native",
                "version": "1.0",
                "source": {"registry": "https://pypi.org/simple"},
                "sdist": {"url": "https://example.invalid/native-1.0.tar.gz", "hash": "sha256:00"},
                "wheels": [
                    wheel("macosx_11_0_arm64"),
                    wheel("macosx_10_13_x86_64"),
                    wheel("manylinux_2_17_x86_64"),
                ],
            },
        ]
    }
    pyproject = {
        "project": {"name": "demo", "requires-python": ">=3.12", "dependencies": ["native"]}
    }
    with pytest.raises(render_script.RenderError, match=r"no native wheel .* linux/arm"):
        render_script.resolve_resources(lock, pyproject)


@pytest.mark.parametrize(
    ("marker", "expected"),
    [
        ("python_full_version < '3.13'", False),
        ("python_full_version >= '3.14'", True),
        ("sys_platform == 'win32'", False),
        ("sys_platform != 'emscripten' and sys_platform != 'win32'", True),
        (
            "(platform_machine == 'arm64' or platform_machine == 'aarch64') and os_name == 'posix'",
            True,
        ),
        ("platform_machine == 'x86_64'", False),
    ],
)
def test_markers_evaluate_for_macos_arm(marker: str, expected: bool) -> None:
    platform = render_script.PLATFORMS[0]
    env = render_script.marker_envs(platform, "3.14")[0]
    assert render_script.evaluate_marker(marker, env) is expected
