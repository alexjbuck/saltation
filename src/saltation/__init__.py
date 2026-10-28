"""saltation — SQL-driven evolution runner for Apache Iceberg tables."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("saltation")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
