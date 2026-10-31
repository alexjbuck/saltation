"""Render the Homebrew formula (Formula/saltation.rb) from uv.lock and pyproject.toml.

Usage: uv run python scripts/render_homebrew_formula.py [--version V --url URL --sha256 HEX]

Without --version/--url/--sha256 the stable url/sha256 already in the output file is kept
(none before the first release, which renders a head-only formula). See docs/homebrew.md.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "Formula" / "saltation.rb"
REGEN_COMMAND = "uv run python scripts/render_homebrew_formula.py"

# The newest CPython minor that every compiled dependency in uv.lock publishes wheels
# for on all four platforms below. Bump it (and re-render) once they all do for the next.
PYTHON_MINOR = "3.14"
HEAD_URL = "https://github.com/alexjbuck/saltation.git"
# Homebrew's audit caps desc at 80 characters, which pyproject's description exceeds.
DESC = "SQL-driven evolution runner for Apache Iceberg tables, powered by PyIceberg"
# Oldest systems Homebrew supports at Tier 1 (docs.brew.sh/Support-Tiers). A wheel that
# needs anything newer fails the render rather than the install on a supported machine.
MACOS_OLDEST = (15, 0)
GLIBC_OLDEST = (2, 39)


class RenderError(Exception):
    pass


@dataclass(frozen=True)
class Platform:
    os: str
    arch: str
    sys_platform: str
    platform_system: str
    platform_machine: str
    wheel_archs: tuple[str, ...]


PLATFORMS = (
    Platform("macos", "arm", "darwin", "Darwin", "arm64", ("arm64", "universal2")),
    Platform("macos", "intel", "darwin", "Darwin", "x86_64", ("x86_64", "universal2")),
    Platform("linux", "arm", "linux", "Linux", "aarch64", ("aarch64",)),
    Platform("linux", "intel", "linux", "Linux", "x86_64", ("x86_64",)),
)


@dataclass(frozen=True)
class Artifact:
    url: str
    sha256: str

    @property
    def filename(self) -> str:
        return self.url.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class Resource:
    name: str
    sdist: Artifact | None
    wheels: Mapping[Platform, Artifact] | None


@dataclass(frozen=True)
class Stable:
    url: str
    sha256: str


# --- PEP 508 markers ----------------------------------------------------------

_MARKER_TOKEN = re.compile(
    r"""\s*(?:
        (?P<str>'[^']*'|"[^"]*")
      | (?P<op>===|==|!=|<=|>=|~=|<|>|\(|\)|not\s+in\b|in\b|and\b|or\b)
      | (?P<var>[a-z_]+)
    )""",
    re.VERBOSE,
)
_VERSION_VARS = {"python_version", "python_full_version", "implementation_version"}


def _tokenize(marker: str) -> list[tuple[str, str]]:
    tokens, pos = [], 0
    while pos < len(marker.rstrip()):
        m = _MARKER_TOKEN.match(marker, pos)
        if not m:
            raise RenderError(f"cannot parse marker {marker!r} at {marker[pos:]!r}")
        kind = m.lastgroup
        assert kind is not None
        text = m.group(kind)
        tokens.append((kind, " ".join(text.split()) if kind == "op" else text))
        pos = m.end()
    return tokens


def _version_tuple(text: str) -> tuple[int, ...]:
    if not re.fullmatch(r"\d+(\.\d+)*", text):
        raise RenderError(f"unsupported version {text!r} in a marker")
    return tuple(int(part) for part in text.split("."))


def _compare_versions(left: str, op: str, right: str) -> bool:
    a, b = _version_tuple(left), _version_tuple(right)
    if op == "~=":
        prefix = b[:-1]
        return a[: len(prefix)] == prefix and _pad(a, b)[0] >= _pad(a, b)[1]
    x, y = _pad(a, b)
    return {
        "==": x == y,
        "!=": x != y,
        "<": x < y,
        "<=": x <= y,
        ">": x > y,
        ">=": x >= y,
    }[op]


def _pad(a: tuple[int, ...], b: tuple[int, ...]) -> tuple[tuple[int, ...], tuple[int, ...]]:
    width = max(len(a), len(b))
    return a + (0,) * (width - len(a)), b + (0,) * (width - len(b))


class _MarkerParser:
    def __init__(self, marker: str, env: Mapping[str, str]) -> None:
        self.marker = marker
        self.tokens = _tokenize(marker)
        self.pos = 0
        self.env = env

    def parse(self) -> bool:
        result = self._or()
        if self.pos != len(self.tokens):
            raise RenderError(f"trailing tokens in marker {self.marker!r}")
        return result

    def _peek(self) -> tuple[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _take(self) -> tuple[str, str]:
        token = self._peek()
        if token is None:
            raise RenderError(f"marker {self.marker!r} ends early")
        self.pos += 1
        return token

    def _or(self) -> bool:
        result = self._and()
        while self._peek() == ("op", "or"):
            self.pos += 1
            right = self._and()
            result = result or right
        return result

    def _and(self) -> bool:
        result = self._atom()
        while self._peek() == ("op", "and"):
            self.pos += 1
            right = self._atom()
            result = result and right
        return result

    def _atom(self) -> bool:
        if self._peek() == ("op", "("):
            self.pos += 1
            result = self._or()
            if self._take() != ("op", ")"):
                raise RenderError(f"unbalanced parentheses in marker {self.marker!r}")
            return result
        left_kind, left = self._take()
        op_kind, op = self._take()
        right_kind, right = self._take()
        if op_kind != "op" or op in {"(", ")", "and", "or"}:
            raise RenderError(f"expected a comparison in marker {self.marker!r}")
        is_version = (left_kind == "var" and left in _VERSION_VARS) or (
            right_kind == "var" and right in _VERSION_VARS
        )
        lval, rval = self._value(left_kind, left), self._value(right_kind, right)
        if op == "in":
            return lval in rval
        if op == "not in":
            return lval not in rval
        if op == "===":
            return lval == rval
        if is_version:
            return _compare_versions(lval, op, rval)
        if op == "==":
            return lval == rval
        if op == "!=":
            return lval != rval
        raise RenderError(f"cannot compare non-versions with {op} in marker {self.marker!r}")

    def _value(self, kind: str, text: str) -> str:
        if kind == "str":
            return text[1:-1]
        if kind != "var" or text not in self.env:
            raise RenderError(f"unknown marker variable {text!r} in {self.marker!r}")
        return self.env[text]


def evaluate_marker(marker: str, env: Mapping[str, str]) -> bool:
    return _MarkerParser(marker, env).parse()


def marker_envs(platform: Platform, python_minor: str) -> list[dict[str, str]]:
    # Homebrew moves python@3.x through its patch releases, so a marker must hold the
    # same way for the first and any later patch of the chosen minor.
    return [
        {
            "python_version": python_minor,
            "python_full_version": f"{python_minor}.{patch}",
            "implementation_version": f"{python_minor}.{patch}",
            "implementation_name": "cpython",
            "platform_python_implementation": "CPython",
            "sys_platform": platform.sys_platform,
            "platform_system": platform.platform_system,
            "platform_machine": platform.platform_machine,
            "os_name": "posix",
            "platform_release": "",
            "platform_version": "",
            "extra": "",
        }
        for patch in (0, 99)
    ]


def _holds(marker: str | None, envs: list[dict[str, str]], where: str) -> bool:
    if not marker:
        return True
    results = {evaluate_marker(marker, env) for env in envs}
    if len(results) > 1:
        raise RenderError(f"marker {marker!r} on {where} depends on the Python patch version")
    return results.pop()


# --- uv.lock ------------------------------------------------------------------


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _requirement_key(requirement: str) -> tuple[str, tuple[str, ...]]:
    m = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[([^\]]*)\])?", requirement)
    if not m:
        raise RenderError(f"cannot parse dependency {requirement!r} in pyproject.toml")
    extras = tuple(sorted(_normalize(e) for e in (m.group(2) or "").split(",") if e.strip()))
    return _normalize(m.group(1)), extras


def _check_requires_python(spec: str, python_minor: str) -> None:
    for clause in filter(None, (c.strip() for c in spec.split(","))):
        m = re.fullmatch(r"(===|==|!=|<=|>=|~=|<|>)\s*([\d.]+)(\.\*)?", clause)
        if not m:
            raise RenderError(f"unsupported requires-python clause {clause!r}")
        op, version, _ = m.groups()
        if not _compare_versions(python_minor, op, version):
            raise RenderError(f"Python {python_minor} does not satisfy requires-python {spec!r}")


def _artifact(entry: Mapping[str, Any], package: str) -> Artifact:
    digest = entry.get("hash", "")
    if not digest.startswith("sha256:"):
        raise RenderError(f"{package}: {entry.get('url')} has no sha256 hash in uv.lock")
    return Artifact(entry["url"], digest.removeprefix("sha256:"))


def _wheel_tags(filename: str) -> tuple[set[str], set[str], set[str]]:
    parts = filename.removesuffix(".whl").split("-")
    if len(parts) not in (5, 6):
        raise RenderError(f"malformed wheel filename {filename!r}")
    py, abi, plat = parts[-3:]
    return set(py.split(".")), set(abi.split(".")), set(plat.split("."))


def _is_pure(filename: str) -> bool:
    py, abi, plat = _wheel_tags(filename)
    return plat == {"any"} and abi == {"none"} and any(t.startswith("py3") for t in py)


def _interpreter_rank(py_tags: set[str], abi_tags: set[str], python_minor: str) -> int | None:
    minor = int(python_minor.split(".")[1])
    cp = f"cp3{minor}"
    best = None
    for py in py_tags:
        for abi in abi_tags:
            rank = None
            if py == cp and abi == cp:
                rank = 2
            elif abi == "abi3" and re.fullmatch(r"cp3\d+", py) and int(py[3:]) <= minor:
                rank = 1
            elif abi == "none" and (
                py in {"py3", cp} or (py.startswith("py3") and int(py[3:] or 0) <= minor)
            ):
                rank = 0
            if rank is not None and (best is None or rank > best):
                best = rank
    return best


_MANYLINUX_LEGACY = {"manylinux1": (2, 5), "manylinux2010": (2, 12), "manylinux2014": (2, 17)}


def _platform_tags(plat_tags: set[str], platform: Platform) -> list[tuple[tuple[int, ...], int]]:
    """(system version, 1 for a thin arch or 0 for universal2) of each tag usable on platform."""
    usable = []
    for tag in plat_tags:
        if platform.os == "macos":
            m = re.fullmatch(r"macosx_(\d+)_(\d+)_(\w+)", tag)
            if m and m.group(3) in platform.wheel_archs:
                version = (int(m.group(1)), int(m.group(2)))
                if version <= MACOS_OLDEST:
                    usable.append((version, int(m.group(3) != "universal2")))
            continue
        m = re.fullmatch(r"manylinux_(\d+)_(\d+)_(\w+)", tag)
        legacy = re.fullmatch(r"(manylinux1|manylinux2010|manylinux2014)_(\w+)", tag)
        if m:
            version, arch = (int(m.group(1)), int(m.group(2))), m.group(3)
        elif legacy:
            version, arch = _MANYLINUX_LEGACY[legacy.group(1)], legacy.group(2)
        else:
            continue
        if arch in platform.wheel_archs and version <= GLIBC_OLDEST:
            usable.append((version, 1))
    return usable


def select_wheel(
    package: str, wheels: Iterable[Artifact], platform: Platform, python_minor: str
) -> Artifact:
    candidates = []
    for wheel in wheels:
        py, abi, plat = _wheel_tags(wheel.filename)
        interpreter = _interpreter_rank(py, abi, python_minor)
        usable = _platform_tags(plat, platform)
        if interpreter is not None and usable:
            candidates.append(((interpreter, *max(usable)), wheel.filename, wheel))
    if not candidates:
        raise RenderError(
            f"uv.lock has no {package} wheel for CPython {python_minor} on "
            f"{platform.os}/{platform.arch} (macOS <= {MACOS_OLDEST}, glibc <= {GLIBC_OLDEST}); "
            "the formula would build it from source there"
        )
    candidates.sort(key=lambda c: c[1])
    candidates.sort(key=lambda c: c[0], reverse=True)
    return candidates[0][2]


class Lock:
    def __init__(self, data: Mapping[str, Any], project: str) -> None:
        self.packages: dict[str, list[Mapping[str, Any]]] = {}
        for package in data.get("package", []):
            self.packages.setdefault(package["name"], []).append(package)
        roots = [
            p
            for p in self.packages.get(project, [])
            if "editable" in p.get("source", {}) or "virtual" in p.get("source", {})
        ]
        if len(roots) != 1:
            raise RenderError(f"uv.lock has no project entry for {project!r}")
        self.root = roots[0]

    def _package(self, dep: Mapping[str, Any], envs: list[dict[str, str]]) -> Mapping[str, Any]:
        name = dep["name"]
        candidates = self.packages.get(name, [])
        if "version" in dep:
            candidates = [p for p in candidates if p.get("version") == dep["version"]]
        if len(candidates) > 1:
            candidates = [
                p
                for p in candidates
                if any(_holds(m, envs, name) for m in p.get("resolution-markers", [""]))
            ]
        if len(candidates) != 1:
            raise RenderError(
                f"cannot pick one {name} entry from uv.lock ({len(candidates)} match)"
            )
        return candidates[0]

    def closure(self, envs: list[dict[str, str]]) -> dict[str, Mapping[str, Any]]:
        found: dict[str, Mapping[str, Any]] = {}
        seen: set[tuple[str, str]] = set()
        queue: list[tuple[Mapping[str, Any], tuple[str, ...]]] = [(self.root, ())]
        while queue:
            package, extras = queue.pop()
            deps = list(package.get("dependencies", []))
            for extra in extras:
                deps += package.get("optional-dependencies", {}).get(extra, [])
            for dep in deps:
                if not _holds(dep.get("marker"), envs, f"{package['name']} -> {dep['name']}"):
                    continue
                target = self._package(dep, envs)
                found[target["name"]] = target
                for extra in ("", *dep.get("extra", [])):
                    key = (target["name"], extra)
                    if key not in seen:
                        seen.add(key)
                        queue.append((target, (extra,) if extra else ()))
        return found


def resolve_resources(
    lock_data: Mapping[str, Any], pyproject: Mapping[str, Any], python_minor: str = PYTHON_MINOR
) -> list[Resource]:
    project = pyproject["project"]
    name = _normalize(project["name"])
    _check_requires_python(project.get("requires-python", ""), python_minor)
    lock = Lock(lock_data, name)

    declared = sorted(_requirement_key(r) for r in project.get("dependencies", []))
    locked = sorted(
        (_normalize(d["name"]), tuple(sorted(d.get("extra", []))))
        for d in lock.root.get("dependencies", [])
    )
    if declared != locked:
        raise RenderError("uv.lock is out of date with pyproject.toml; run `uv lock`")

    closures = {p: lock.closure(marker_envs(p, python_minor)) for p in PLATFORMS}
    names = set().union(*(c.keys() for c in closures.values()))
    partial = sorted(n for n in names if any(n not in c for c in closures.values()))
    if partial:
        raise RenderError(
            f"dependencies needed on only some platforms are not supported yet: {partial}"
        )

    resources = []
    for package_name in sorted(names):
        package = closures[PLATFORMS[0]][package_name]
        if "registry" not in package.get("source", {}):
            raise RenderError(f"{package_name} does not come from a package index")
        wheels = [_artifact(w, package_name) for w in package.get("wheels", [])]
        sdist = _artifact(package["sdist"], package_name) if "sdist" in package else None
        pure = [w for w in wheels if _is_pure(w.filename)]
        if pure or not wheels:
            source = sdist or (pure[0] if pure else None)
            if source is None:
                raise RenderError(f"{package_name} has neither an sdist nor a wheel in uv.lock")
            resources.append(Resource(package_name, source, None))
        else:
            chosen = {p: select_wheel(package_name, wheels, p, python_minor) for p in PLATFORMS}
            resources.append(Resource(package_name, None, chosen))
    return resources


# --- Ruby ---------------------------------------------------------------------


def _ruby_string(value: str) -> str:
    if re.search(r'["\\#\n]', value):
        raise RenderError(f"refusing to embed {value!r} in a Ruby string")
    return f'"{value}"'


def _url_lines(artifact: Artifact, indent: str) -> list[str]:
    return [
        f"{indent}url {_ruby_string(artifact.url)}",
        f"{indent}sha256 {_ruby_string(artifact.sha256)}",
    ]


def _render_resource(resource: Resource) -> list[str]:
    lines = [f"  resource {_ruby_string(resource.name)} do"]
    if resource.sdist is not None:
        lines += _url_lines(resource.sdist, "    ")
    else:
        assert resource.wheels is not None
        for os_name in ("macos", "linux"):
            lines.append(f"    on_{os_name} do")
            for platform in PLATFORMS:
                if platform.os == os_name:
                    lines.append(f"      on_{platform.arch} do")
                    lines += _url_lines(resource.wheels[platform], "        ")
                    lines.append("      end")
            lines.append("    end")
    lines.append("  end")
    return lines


def _requirements_comment(resources: list[Resource]) -> str:
    macos: tuple[int, ...] = (0,)
    glibc: tuple[int, ...] = (0,)
    for resource in resources:
        for platform, wheel in (resource.wheels or {}).items():
            oldest = min(v for v, _ in _platform_tags(_wheel_tags(wheel.filename)[2], platform))
            if platform.os == "macos":
                macos = max(macos, oldest)
            else:
                glibc = max(glibc, oldest)
    return (
        f"# Compiled dependencies install as pinned wheels: macOS {'.'.join(map(str, macos))}+ "
        f"and glibc {'.'.join(map(str, glibc))}+."
    )


_INSTALL_AND_TEST = r"""  def install
    python3 = "@PYTHON@"
    venv = virtualenv_create(libexec, python3)
    resources.each do |r|
      if r.url.end_with?(".whl")
        # A .whl resource is staged as the file itself, and pip_install only looks
        # inside the staging directory for py3-none-any wheels.
        r.stage { venv.pip_install Pathname.pwd/r.downloader.basename }
      else
        venv.pip_install r
      end
    end
    # std_pip_args forbids every wheel, which would compile saltation's build backend
    # (uv_build, Rust) from source; let pip use its wheel for the isolated build.
    system python3, "-m", "pip", "--python=#{venv.root}/bin/python", "install",
           *std_pip_args(prefix: false, build_isolation: true),
           "--only-binary=uv-build", buildpath
    bin.install_symlink libexec/"bin/saltation"
  end

  test do
    output = shell_output("#{bin}/saltation --version")
    assert_match(/\Asaltation \d+\.\d+\.\d+/, output)
    assert_match "saltation #{version}", output unless version.head?

    config = testpath/"saltation.toml"
    config.write <<~TOML
      [catalog]
      type = "rest"
      uri = "http://127.0.0.1:9/catalog"
      warehouse = "smoke"

      [evolutions]
      dir = "./evolutions"
    TOML
    assert_match "Config valid", shell_output("#{bin}/saltation config check --path #{config}")
  end
end
"""


def render(
    resources: list[Resource],
    pyproject: Mapping[str, Any],
    stable: Stable | None = None,
    python_minor: str = PYTHON_MINOR,
) -> str:
    project = pyproject["project"]
    homepage = project["urls"]["Repository"]
    python = f"python{python_minor}"
    head = [
        "# Generated by scripts/render_homebrew_formula.py from uv.lock and pyproject.toml.",
        f"# Do not edit by hand; regenerate with `{REGEN_COMMAND}`.",
    ]
    if any(r.wheels for r in resources):
        head.append(_requirements_comment(resources))
    lines = [
        *head,
        "class Saltation < Formula",
        "  include Language::Python::Virtualenv",
        "",
        f"  desc {_ruby_string(DESC)}",
        f"  homepage {_ruby_string(homepage)}",
    ]
    if stable is not None:
        lines += _url_lines(Artifact(stable.url, stable.sha256), "  ")
    lines += [
        f"  license {_ruby_string(project['license'])}",
        f'  head {_ruby_string(HEAD_URL)}, branch: "main"',
        "",
        f'  depends_on "python@{python_minor}"',
        "",
        "  # Keep the @rpath dylib IDs of libraries bundled in the wheels: rewriting them to",
        "  # absolute paths needs Mach-O header space a wheel need not have reserved.",
        "  preserve_rpath",
        "",
    ]
    for resource in resources:
        lines += [*_render_resource(resource), ""]
    lines.append(_INSTALL_AND_TEST.replace("@PYTHON@", python))
    return "\n".join(lines)


_STABLE_URL = re.compile(r'^  url "([^"]+)"\n  sha256 "([0-9a-f]{64})"$', re.MULTILINE)


def existing_stable(formula_text: str) -> Stable | None:
    m = _STABLE_URL.search(formula_text)
    return Stable(m.group(1), m.group(2)) if m else None


def render_from_files(lock_path: Path, pyproject_path: Path, stable: Stable | None) -> str:
    lock_data = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return render(resolve_resources(lock_data, pyproject), pyproject, stable)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--version", help="released version the stable url points at")
    parser.add_argument("--url", help="URL of the release's sdist")
    parser.add_argument("--sha256", help="sha256 of that sdist")
    parser.add_argument(
        "--head-only", action="store_true", help="drop any stable url/sha256 from the output"
    )
    parser.add_argument("--lock", type=Path, default=ROOT / "uv.lock")
    parser.add_argument("--pyproject", type=Path, default=ROOT / "pyproject.toml")
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT, help="formula path, or - for stdout"
    )
    args = parser.parse_args(argv)

    release_args = (args.version, args.url, args.sha256)
    if any(release_args) and not all(release_args):
        parser.error("--version, --url and --sha256 go together")
    if all(release_args) and args.head_only:
        parser.error("--head-only cannot be combined with --version/--url/--sha256")

    to_stdout = str(args.output) == "-"
    stable = None
    try:
        if all(release_args):
            if not re.fullmatch(r"[0-9a-f]{64}", args.sha256):
                raise RenderError(f"--sha256 {args.sha256!r} is not a sha256 hex digest")
            if args.version not in args.url:
                raise RenderError(f"--url {args.url!r} does not mention version {args.version}")
            pyproject = tomllib.loads(args.pyproject.read_text(encoding="utf-8"))
            if pyproject["project"]["version"] != args.version:
                raise RenderError(
                    f"pyproject.toml is at {pyproject['project']['version']}, not {args.version}"
                )
            stable = Stable(args.url, args.sha256)
        elif not args.head_only and not to_stdout and args.output.exists():
            stable = existing_stable(args.output.read_text(encoding="utf-8"))
        text = render_from_files(args.lock, args.pyproject, stable)
    except RenderError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    if to_stdout:
        sys.stdout.write(text)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
        print(f"wrote {args.output} ({'stable ' + stable.url if stable else 'head-only'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
