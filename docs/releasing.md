# Releasing saltation

Versions and release notes come from **change files** managed by
[knope](https://knope.tech). Releases are cut by merging a release PR; the tag,
GitHub release, TestPyPI upload and (after approval) PyPI upload all follow from
that merge.

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
   - builds the sdist and wheel and checks them (`twine check --strict`);
   - tags `v<version>` and creates the GitHub release with the dists attached
     (`knope release`);
   - publishes to **TestPyPI**, then installs that upload and runs
     `saltation --version` against it;
   - waits for approval on the **`pypi`** environment, then publishes to PyPI.

To republish an existing tag (for example after fixing a PyPI-side problem),
run `release.yml` manually with that tag. It skips tagging and goes straight
to TestPyPI and the PyPI approval gate; TestPyPI skips files it already has.

## One-time setup

Nothing here can be done from the repo itself; it needs an owner in each UI.

### PyPI and TestPyPI: trusted publishing

On both [PyPI](https://pypi.org/manage/account/publishing/) and
[TestPyPI](https://test.pypi.org/manage/account/publishing/), add a (pending)
trusted publisher for the project:

| Field             | Value                                 |
|-------------------|---------------------------------------|
| Owner             | `alexjbuck`                           |
| Repository        | `saltation`                           |
| Workflow name     | `release.yml`                         |
| Environment name  | `pypi` (PyPI) / `testpypi` (TestPyPI) |

No API tokens are stored anywhere; the jobs mint short-lived ones via OIDC.

### GitHub environments

In **Settings → Environments**, create:

- `testpypi`, with no protection rules;
- `pypi`, with **Required reviewers** set to whoever approves releases (and,
  optionally, deployment branches limited to `main` plus tags `v*`).

### Release PR token

A PR opened with the workflow's default `GITHUB_TOKEN` doesn't trigger other
workflows, so CI wouldn't run on the release PR. Add a repository secret
`RELEASE_TOKEN`: a fine-grained personal access token (or GitHub App token)
for this repository with **Contents: read and write** and **Pull requests:
read and write**. Without it the workflow falls back to `GITHUB_TOKEN`, which
also needs **Settings → Actions → General → Allow GitHub Actions to create and
approve pull requests** turned on.

### Labels

Create the `no-changeset` label.
