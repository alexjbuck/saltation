# PyInstaller spec for the standalone saltation bundle (onedir).
# Build from the repo root: uv run pyinstaller packaging/saltation.spec
# Output: dist/saltation/saltation[.exe]
import os

from PyInstaller.utils.hooks import collect_all, collect_submodules, copy_metadata

datas = []
binaries = []
hiddenimports = []

# saltation.__version__ and pydantic/pyiceberg read importlib.metadata at
# runtime; recursive=True also bundles every dependency's dist-info.
datas += copy_metadata("saltation", recursive=True)

# pyiceberg resolves catalogs, FileIO and pyiceberg_core by import-path
# strings (importlib.import_module), which static analysis can't follow.
# sqlglot imports dialect modules lazily by name.
for pkg in ("pyiceberg", "pyiceberg_core", "sqlglot"):
    pkg_datas, pkg_binaries, pkg_hidden = collect_all(pkg)
    datas += pkg_datas
    binaries += pkg_binaries
    hiddenimports += pkg_hidden

hiddenimports += [
    "pyiceberg.catalog.rest",
    "pyiceberg.catalog.sql",
    "pyiceberg.io.pyarrow",
    "pyiceberg.io.fsspec",
    "pyiceberg_core",
]
hiddenimports += collect_submodules("saltation")

a = Analysis(
    [os.path.join(SPECPATH, "entry.py")],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tkinter", "pytest", "IPython"],
    noarchive=False,
)
# The build host's C++ runtime would pin the bundle to that host's glibc
# (2.38 on Ubuntu 24.04). Every target ships its own, and pyarrow's
# manylinux_2_28 wheels already expect it there; leaving these out drops the
# Linux floor to glibc 2.28 (RHEL 8, Ubuntu 20.04 and newer).
_HOST_RUNTIME = ("libstdc++.so.6", "libgcc_s.so.1")
a.binaries = [b for b in a.binaries if os.path.basename(b[0]) not in _HOST_RUNTIME]

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="saltation",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name="saltation",
)
