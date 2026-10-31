---
default: minor
---

# Python 3.14, standalone binaries, and engine interoperability coverage

saltation now runs its test suite on Python 3.12, 3.13 and 3.14. Releases attach standalone binaries for Linux (x86_64, aarch64), macOS (arm64, x86_64) and Windows (x86_64) next to the wheel and sdist, for hosts without Python. Tables and views written by saltation are verified in CI through Trino, Apache Spark, StarRocks and DuckDB, and locked dependencies are audited for known vulnerabilities.
