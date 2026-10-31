# Releasing saltation

Versions and release notes come from **change files** managed by
[knope](https://knope.tech). Releases are cut by merging a release PR; the
build, tag and GitHub release (with standalone binaries attached) follow from
that merge. PyPI publishing is a separate workflow that stays switched off
until the one-time setup below is done.

## Day to day: one change file per PR

Every PR with a user-facing change adds a file under `.changeset/`, either by
running `knope document-change` (interactive) or by hand:

```markdown
---
default: minor
---

# Reset restores everything current.sql touched

Optional detail. Becomes part of the CHANGELOG entry.
```

The front matter maps the package (`default`, since this repo has one) to a
change type: `major`, `minor` or `patch`. Before 1.0, knope shifts these down
one level (a `major` change bumps 0.x to 0.(x+1), `minor` and `patch` bump the
patch number).

The `changeset` check fails a PR without a change file. Label the PR
`no-changeset` when nothing user-facing changed (CI, tests, docs).

## Cutting a release

1. Every push to `main` runs `prepare-release.yml`. When `.changeset/` has
   files, knope bumps `pyproject.toml`, writes the `CHANGELOG.md` entry,
   deletes the consumed change files, re-locks `uv.lock`, and force-pushes
   the result to the `release` branch with a PR titled
   `chore: prepare release <version>`. More merges to `main` refresh that PR.
2. Review the PR (the body is the release notes) and merge it.
3. `release.yml` then:
   - runs `package.yml` on the merge commit (below) with attestations on;
   - tags `v<version>` and creates the GitHub release with every artifact
     attached (`knope release`, which uploads `dist/*`);
   - if `PYPI_PUBLISH_ENABLED` is set, dispatches `publish-pypi.yml` for the
     new tag;
   - renders `Formula/saltation.rb` against the release's sdist and opens a
     PR titled `chore: homebrew formula v<version>`. Merge it so
     `brew install saltation` gets the release ([homebrew.md](./homebrew.md)).
4. `publish-pypi.yml` (once enabled) downloads the sdist and wheel from the
   release, publishes to **TestPyPI**, installs that upload and runs
   `saltation --version`, then waits for approval on the **`pypi`**
   environment and publishes to PyPI.

### What `package.yml` builds

It runs on every pull request, so a packaging break shows up in review, not
on release day:

| Job             | What it does                                                                                                  |
|-----------------|---------------------------------------------------------------------------------------------------------------|
| `wheel`         | `uv build`; checks for exactly one `py3-none-any` wheel and the sdist; `twine check --strict`. Artifact `dist`. |
| `install-smoke` | Installs the wheel into a fresh `uv venv` on Linux x86_64/arm64, macOS arm64/x86_64 and Windows x86_64, each with Python 3.12, 3.13 and 3.14; runs `--version`, `--help` and `config check`. |
| `binaries`      | Builds a standalone PyInstaller bundle per target (`packaging/saltation.spec`), archives it as `saltation-<version>-<target>.tar.gz` (`.zip` on Windows) with a `.sha256` beside it, extracts it elsewhere and smoke-runs it. Artifact `binary-<target>`. |
| `attest`        | Build-provenance attestations for the wheel, sdist and archives. Only when called with `attest: true` (release builds), never for fork PRs. |

A release therefore carries `saltation-<version>.tar.gz` (sdist),
`saltation-<version>-py3-none-any.whl`, and for each of `linux-x86_64`,
`linux-aarch64`, `macos-arm64`, `macos-x86_64` and `windows-x86_64` an
archive plus its `.sha256`. What the binaries are for and their limits
(unsigned, macOS quarantine, glibc, no SBOM yet) is in
[`packaging/README.md`](../packaging/README.md), which also ships inside each
archive.

### Re-running parts of a release

- **Rebuild a release's assets** (e.g. a binary job failed after the tag was
  made): run `release.yml` manually with the tag. It rebuilds from the tag
  and re-uploads the binaries to the existing release. A wheel or sdist
  already on the release is kept, since PyPI may hold those exact bytes.
  The `homebrew` job runs again too; it only updates the formula PR when the
  tag is the version on `main`.
- **Republish to PyPI** (e.g. after fixing a PyPI-side problem): run
  `publish-pypi.yml` manually with the tag. Both uploads skip files the index
  already has, so a re-run after a partial upload converges.

## One-time setup

Nothing here can be done from the repo itself; it needs an owner in each UI.

### Release PR: token or Actions setting (needed now)

The latest `prepare-release` run on `main` failed with **"GitHub Actions is not
permitted to create or approve pull requests"**: no `RELEASE_TOKEN` secret is
set, so knope fell back to the workflow's `GITHUB_TOKEN`, and the repository
doesn't let that token open PRs. Do one of these (the first is recommended):

1. **Add a `RELEASE_TOKEN` repository secret** (Settings → Secrets and
   variables → Actions → Secrets): a fine-grained personal access token, or a
   GitHub App token, scoped to this repository with **Contents: read and
   write**, **Pull requests: read and write** and **Workflows: read and
   write** (the force-push to `release` carries any workflow changes made on
   `main`). A PR opened with it triggers CI and `package.yml` like any other
   PR; one opened with `GITHUB_TOKEN` triggers no workflows, so the release
   PR would have no checks.
2. **Or** turn on Settings → Actions → General → Workflow permissions →
   **Allow GitHub Actions to create and approve pull requests**. That makes
   the `GITHUB_TOKEN` fallback work, but the release PR then gets no CI runs
   (close and reopen it to trigger them).

After either change, re-run the failed `prepare-release` run or push to
`main`.

### Labels

Create the `no-changeset` label.

### PyPI publishing (switched off until you do this)

`publish-pypi.yml` gates every job on the repository **variable**
`PYPI_PUBLISH_ENABLED`; until it is `true`, the workflow's runs only show
skipped jobs and `release.yml` doesn't dispatch it. Releases still get tagged
and attached to GitHub in the meantime. To switch it on:

1. **Trusted publishers.** On both [PyPI](https://pypi.org/manage/account/publishing/)
   and [TestPyPI](https://test.pypi.org/manage/account/publishing/), add a
   (pending, if the project doesn't exist there yet) trusted publisher:

   | Field             | Value                                 |
   |-------------------|---------------------------------------|
   | PyPI project name | `saltation`                           |
   | Owner             | `alexjbuck`                           |
   | Repository        | `saltation`                           |
   | Workflow name     | `publish-pypi.yml`                    |
   | Environment name  | `pypi` (PyPI) / `testpypi` (TestPyPI) |

   No API tokens are stored anywhere; the jobs mint short-lived ones via
   OIDC. The workflow name must be `publish-pypi.yml`, not `release.yml`: a
   publisher registered for the old workflow won't match.
2. **GitHub environments.** In Settings → Environments, create:
   - `testpypi`, with no protection rules;
   - `pypi`, with **Required reviewers** set to whoever approves releases.
     Leave its deployment branches and tags unrestricted, or allow `main`
     (where `release.yml` dispatches it from).
3. **Enable.** In Settings → Secrets and variables → Actions → **Variables**
   (not Secrets), add `PYPI_PUBLISH_ENABLED` with value `true`.
4. **Publish releases made before this.** Run `publish-pypi.yml` manually
   with each tag that should reach PyPI.

To pause publishing later, delete the variable or set it to anything else.
