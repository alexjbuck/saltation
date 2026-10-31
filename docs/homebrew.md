# Homebrew formula

This repository doubles as a Homebrew tap. `Formula/saltation.rb` is generated
from `uv.lock` and `pyproject.toml` by `scripts/render_homebrew_formula.py`;
never edit it by hand.

```bash
brew tap alexjbuck/saltation https://github.com/alexjbuck/saltation
brew install saltation          # the latest release
brew install --HEAD saltation   # main, before the first release or to try unreleased work
```

## How the formula installs saltation

- **A private virtualenv.** The formula creates a venv in the keg's `libexec`
  with Homebrew's `python@3.14`, installs every dependency into it with
  `--no-deps` from the pinned `resource` blocks, installs saltation itself,
  and symlinks `libexec/bin/saltation` into `bin`. Nothing touches the
  system or Homebrew Python's `site-packages`.
- **Every dependency is pinned.** Each `resource` carries the exact URL and
  sha256 from `uv.lock`, so the formula installs the same versions CI tests.
- **Wheels for compiled dependencies, sdists for the rest.** Homebrew's pip
  arguments (`std_pip_args`) include `--no-binary=:all:`, which builds
  everything pip finds on an index from source. For pyarrow that means
  compiling Arrow C++, and for pydantic-core, pyiceberg-core and watchfiles a
  Rust toolchain. That option only governs what pip picks from an index, so
  the formula pins a platform wheel file instead for every package whose
  `uv.lock` entry has no pure-Python (`py3-none-any`) wheel, inside
  `on_macos`/`on_linux` × `on_arm`/`on_intel` blocks. Pure-Python packages
  are installed from their sdists, as homebrew-core does. Today the wheels
  are mmh3, pyarrow, pydantic-core, pyiceberg, pyiceberg-core, pyroaring,
  watchfiles and zstandard; the header comment in the formula records the
  oldest macOS and glibc those wheels accept.
- **Its own install loop.** `virtualenv_install_with_resources` can't be
  used: Homebrew stages a `.whl` resource as the file itself, and its
  `pip_install` only looks for that file for `py3-none-any` wheels, so a
  platform wheel would be handed to pip as an empty directory. The formula
  installs `.whl` resources by file path and everything else with
  `venv.pip_install`.
- **saltation's build backend comes as a wheel.** saltation builds with
  `uv_build`, which only ships compiled wheels (its sdist needs Rust). The
  formula adds `--only-binary=uv-build` after `std_pip_args` for the one pip
  call that builds saltation, so the isolated build environment fetches that
  wheel instead of compiling it. This is the only thing the formula
  downloads that `uv.lock` doesn't pin.
- **`preserve_rpath`.** On macOS Homebrew rewrites `@rpath` dylib IDs to
  absolute paths after installing, which needs Mach-O header space the
  libraries bundled in the wheels need not have reserved. The formula keeps
  those IDs as built.

### Which Python

The formula depends on `python@3.14`, the newest CPython minor for which
every compiled dependency in `uv.lock` publishes wheels on macOS arm64,
macOS x86_64, Linux x86_64 and Linux aarch64. Homebrew already has
`python@3.15`, but pyarrow and the other compiled dependencies publish no
`cp315` wheels yet. When they do and `uv.lock` has them, bump `PYTHON_MINOR`
in the script and re-render.

The script fails instead of rendering a formula that would compile a
dependency from source: a compiled package with no wheel for that Python on
one of the four platforms, or a wheel that needs a newer system than
Homebrew's Tier 1 floor (macOS 15, glibc 2.39), is an error. Intel macOS is
Homebrew Tier 3: the formula still selects x86_64 wheels, but Homebrew no
longer ships Intel bottles, so `python@3.14` builds from source there.

## Release updates

`release.yml` has a `homebrew` job that runs after a release (and after a
manual republish of the latest release). It hashes the sdist attached to the
GitHub release, runs

```bash
python3 scripts/render_homebrew_formula.py --version <v> \
  --url https://github.com/alexjbuck/saltation/releases/download/v<v>/saltation-<v>.tar.gz \
  --sha256 <sha256>
```

on the default branch, and opens (or updates) a PR titled
`chore: homebrew formula v<version>` with the `no-changeset` label. Merge it
to make `brew install saltation` pick up the release. Opening the PR needs
the same `RELEASE_TOKEN` secret or Actions setting as the release PR
(see [releasing.md](./releasing.md#release-pr-token-or-actions-setting-needed-now));
without either, the job uploads the rendered formula as the
`homebrew-formula` artifact and warns, and the release itself is unaffected.

Between releases the formula's resources track `uv.lock` on `main`, and a
unit test (`tests/unit/test_homebrew_formula.py`) fails when the committed
formula is stale. Re-rendering keeps the stable `url`/`sha256` of the last
release, so `brew install saltation` installs the last release with `main`'s
dependency pins. Those pins only stay compatible while `main` keeps the
release's runtime dependencies; if a change drops one that the last release
still imports, cut a release soon after.

## Regenerating by hand

```bash
uv run python scripts/render_homebrew_formula.py              # keeps the stable url/sha256
uv run python scripts/render_homebrew_formula.py --head-only  # drops them
uv run python scripts/render_homebrew_formula.py --output -   # print instead of writing
```

Run it after anything changes `uv.lock`, and commit the result with the lock.

## Testing locally

This needs a Mac or Linux machine with Homebrew. Homebrew refuses formula
files given by path unless `HOMEBREW_DEVELOPER` is set, so either tap a local
clone (the tap tracks the commit checked out there):

```bash
brew tap alexjbuck/saltation "$PWD"
brew install --HEAD --build-from-source --verbose alexjbuck/saltation/saltation
brew test alexjbuck/saltation/saltation
brew audit --strict --online alexjbuck/saltation/saltation
brew style alexjbuck/saltation
```

or point Homebrew at the working-tree file:

```bash
HOMEBREW_DEVELOPER=1 brew install --HEAD --build-from-source ./Formula/saltation.rb
brew test saltation
HOMEBREW_DEVELOPER=1 brew audit --strict Formula/saltation.rb
```

Drop `--HEAD` once the formula has a stable `url`. Clean up with
`brew uninstall saltation` and `brew untap alexjbuck/saltation`.
