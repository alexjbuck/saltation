# Standalone saltation binaries

Each GitHub release carries a self-contained build of the `saltation` CLI for
machines with no Python, no package index and no internet access, such as
air-gapped CI runners, locked-down jump hosts or minimal containers. Python,
saltation and every dependency (PyIceberg, PyArrow, sqlglot, ...) are bundled
together. Nothing is downloaded at run time; the only network traffic is to
the catalog and object store you configure.

If you can `pip install saltation` (or `uv tool install saltation`), prefer
that: the wheel is smaller and runs on any platform PyArrow supports.

## Targets

| Archive                                   | Platform                       |
|-------------------------------------------|--------------------------------|
| `saltation-<version>-linux-x86_64.tar.gz`  | Linux x86_64 (glibc)           |
| `saltation-<version>-linux-aarch64.tar.gz` | Linux arm64 (glibc)            |
| `saltation-<version>-macos-arm64.tar.gz`   | macOS, Apple silicon           |
| `saltation-<version>-macos-x86_64.tar.gz`  | macOS, Intel                   |
| `saltation-<version>-windows-x86_64.zip`   | Windows x86_64                 |

Each archive has a matching `<archive>.sha256`.

## Install

```sh
sha256sum -c saltation-0.2.0-linux-x86_64.tar.gz.sha256   # macOS: shasum -a 256 -c
tar -xzf saltation-0.2.0-linux-x86_64.tar.gz
./saltation-0.2.0-linux-x86_64/saltation --version
```

The archive holds one directory: the `saltation` executable next to an
`_internal/` directory with everything it loads. Keep the two together; to
put it on `PATH`, symlink the executable (or add the directory) rather than
copying it out.

The archives, the wheel and the sdist carry
[build provenance attestations](https://docs.github.com/actions/security-for-github-actions/using-artifact-attestations):

```sh
gh attestation verify saltation-0.2.0-linux-x86_64.tar.gz --repo alexjbuck/saltation
```

## Limits

- **Unsigned.** The binaries are not code-signed or notarized.
  - macOS: an archive downloaded through a browser is quarantined and
    Gatekeeper refuses to run it ("cannot be opened because the developer
    cannot be verified"). After checking the SHA-256, clear the flag:
    `xattr -dr com.apple.quarantine saltation-<version>-macos-arm64`.
    Archives fetched with `curl`, `gh release download` or similar are not
    quarantined.
  - Windows: SmartScreen may warn on first run; choose "More info", then
    "Run anyway", or unblock the zip (Properties, Unblock) before extracting.
- **No SBOM yet.** The bundled dependency versions are the ones pinned in
  `uv.lock` at the release tag.
- **Linux needs a recent glibc.** The Linux builds are made on Ubuntu 24.04
  (glibc 2.39). Older distributions (RHEL 8/9, Ubuntu 22.04, Debian 12) may
  refuse to start them; use the wheel there. There is no musl (Alpine) build.
- **Catalogs and storage.** The REST catalog and PyArrow's FileIO (local
  files, S3 and S3-compatible stores, plus whatever else that platform's
  PyArrow build supports, such as GCS) are included.
  Catalog backends that need extra Python packages (`sql`, `glue`, `hive`,
  `dynamodb`) and fsspec-based FileIO plugins (s3fs, adlfs, gcsfs) are not.
- **Size.** About 90 MB compressed and 260 MB extracted, mostly PyArrow.
- **Start-up.** The first run after extracting can take a few seconds while
  the OS scans the bundled libraries.

## Building one locally

From the repository root, on the platform you are building for:

```sh
uv sync --frozen --no-dev --no-editable --group package
uv run --no-sync pyinstaller --noconfirm --clean packaging/saltation.spec
./dist/saltation/saltation --version
```

PyInstaller embeds the interpreter it runs under, so that Python needs a
shared `libpython` (python.org, distribution and `actions/setup-python`
builds have one). `.github/workflows/package.yml` builds, archives and
smoke-tests all five targets on every pull request.
