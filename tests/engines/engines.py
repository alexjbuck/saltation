"""Query-engine drivers for the engine-compatibility suite.

Each driver talks to one engine that reads the rig's Lakekeeper catalog and
exposes the same small surface, so the tests stay engine-agnostic. Client
libraries (the ``engines`` dependency group) are imported inside the
constructors: the suite collects without them, and the session fixture skips
with a clear message when the selected engine's client is missing.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

__all__ = [
    "ENGINES",
    "SELECTED_ENGINE",
    "Capabilities",
    "Engine",
    "EngineSpec",
    "coerce_rows",
    "sql_literal",
]

_DOCKER = Path(__file__).resolve().parents[2] / "docker"

# rustfs and Lakekeeper as the host-side test process sees them.
_CATALOG_URI = "http://localhost:8181/catalog"
_WAREHOUSE = "saltation"
_S3_KEY = "saltation-access-key"
_S3_SECRET = "saltation-secret-key"

SELECTED_ENGINE = os.environ.get("SALTATION_TEST_ENGINE", "")


@dataclass(frozen=True)
class Capabilities:
    reads_views: bool
    writes: bool


class Engine(Protocol):
    name: str
    capabilities: Capabilities
    # sqlglot dialect name of the view representation this engine reads.
    view_dialect: str

    def ping(self) -> None: ...

    def query(self, sql: str) -> list[tuple[Any, ...]]: ...

    def columns(self, namespace: str, table: str) -> list[str]: ...

    def table_ref(self, namespace: str, table: str) -> str: ...

    def insert(self, namespace: str, table: str, rows: Sequence[Sequence[object]]) -> None: ...

    def close(self) -> None: ...


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"no portable SQL literal for {value!r}")


def _values(rows: Sequence[Sequence[object]]) -> str:
    return ", ".join("(" + ", ".join(sql_literal(v) for v in row) + ")" for row in rows)


def _coerce(value: object, like: object) -> object:
    if value is None or like is None:
        return value
    if isinstance(like, bool):
        # StarRocks returns BOOLEAN over the MySQL protocol as 0/1.
        return bool(value)
    if isinstance(like, Decimal):
        return Decimal(str(value))
    if isinstance(like, int):
        return int(str(value))
    if isinstance(like, float):
        return float(str(value))
    if isinstance(like, datetime):
        return value
    if isinstance(like, date) and isinstance(value, datetime):
        return value.date()
    if isinstance(like, str) and isinstance(value, bytes):
        return value.decode()
    return value


def coerce_rows(
    actual: Sequence[Sequence[object]], like: Sequence[Sequence[object]]
) -> list[tuple[object, ...]]:
    """Coerce each value in *actual* to the type of its counterpart in *like*.

    Engines disagree on wire types (Decimal vs float, bool vs 0/1, str vs
    bytes); comparing after this keeps assertions engine-agnostic.
    """
    if len(actual) != len(like):
        return [tuple(row) for row in actual]
    return [
        tuple(_coerce(v, w) for v, w in zip(row, ref, strict=False))
        for row, ref in zip(actual, like, strict=True)
    ]


# Vended credentials are off so StarRocks uses the in-network rustfs
# endpoint; the metadata cache is off because saltation changes tables
# between queries and the suite must see every change immediately.
_STARROCKS_CATALOG = {
    "type": "iceberg",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://lakekeeper:8181/catalog",
    "iceberg.catalog.warehouse": _WAREHOUSE,
    "iceberg.catalog.vended-credentials-enabled": "false",
    "enable_iceberg_metadata_cache": "false",
    "aws.s3.endpoint": "http://rustfs:9000",
    "aws.s3.enable_ssl": "false",
    "aws.s3.enable_path_style_access": "true",
    "aws.s3.access_key": _S3_KEY,
    "aws.s3.secret_key": _S3_SECRET,
    "aws.s3.region": "local",
}


class _DbApiEngine:
    _conn: Any

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return [tuple(row) for row in cur.fetchall()]
        finally:
            cur.close()

    def columns(self, namespace: str, table: str) -> list[str]:
        return [str(row[0]) for row in self.query(f"DESCRIBE {self.table_ref(namespace, table)}")]

    def table_ref(self, namespace: str, table: str) -> str:
        raise NotImplementedError

    def insert(self, namespace: str, table: str, rows: Sequence[Sequence[object]]) -> None:
        self.query(f"INSERT INTO {self.table_ref(namespace, table)} VALUES {_values(rows)}")

    def close(self) -> None:
        self._conn.close()


class TrinoEngine(_DbApiEngine):
    name = "trino"
    capabilities = Capabilities(reads_views=True, writes=True)
    view_dialect = "trino"
    catalog = "lakekeeper"

    def __init__(self, host: str = "localhost", port: int = 8080) -> None:
        dbapi = importlib.import_module("trino.dbapi")
        self._conn = dbapi.connect(host=host, port=port, user="saltation", catalog=self.catalog)

    def ping(self) -> None:
        self.query(f"SHOW SCHEMAS FROM {self.catalog}")

    def table_ref(self, namespace: str, table: str) -> str:
        return f'{self.catalog}."{namespace}"."{table}"'


class StarRocksEngine(_DbApiEngine):
    name = "starrocks"
    capabilities = Capabilities(reads_views=True, writes=True)
    view_dialect = "starrocks"
    catalog = "lakekeeper"

    def __init__(self, host: str = "127.0.0.1", port: int = 9030) -> None:
        pymysql = importlib.import_module("pymysql")
        self._conn = pymysql.connect(
            host=host, port=port, user="root", password="", autocommit=True, connect_timeout=5
        )

    def ping(self) -> None:
        catalogs = {str(row[0]) for row in self.query("SHOW CATALOGS")}
        if self.catalog not in catalogs:
            props = ",\n".join(f'"{k}" = "{v}"' for k, v in _STARROCKS_CATALOG.items())
            self.query(f"CREATE EXTERNAL CATALOG {self.catalog} PROPERTIES (\n{props}\n)")
        self.query(f"SHOW DATABASES FROM {self.catalog}")

    def table_ref(self, namespace: str, table: str) -> str:
        return f"{self.catalog}.`{namespace}`.`{table}`"


class DuckDBEngine:
    name = "duckdb"
    # The iceberg extension's docs (DuckDB 2.0) cover INSERT/UPDATE/DELETE on
    # REST-catalog tables, partitioned or not, but no Iceberg views.
    capabilities = Capabilities(reads_views=False, writes=True)
    view_dialect = "duckdb"
    catalog = "lk"

    def __init__(self) -> None:
        self._duckdb = importlib.import_module("duckdb")
        con = self._duckdb.connect()
        try:
            con.execute("INSTALL iceberg")
            con.execute("INSTALL httpfs")
        finally:
            con.close()

    def _connect(self) -> Any:
        # A fresh in-process database per statement: nothing DuckDB caches
        # about the attached catalog can outlive a saltation change.
        con = self._duckdb.connect()
        con.execute("LOAD iceberg")
        con.execute("LOAD httpfs")
        con.execute(
            "CREATE SECRET rustfs (TYPE s3, "
            f"KEY_ID '{_S3_KEY}', SECRET '{_S3_SECRET}', REGION 'local', "
            "ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false)"
        )
        con.execute(
            f"ATTACH '{_WAREHOUSE}' AS {self.catalog} (TYPE iceberg, URI '{_CATALOG_URI}', "
            "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
        )
        return con

    def ping(self) -> None:
        self.query("SELECT 1")

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        con = self._connect()
        try:
            return [tuple(row) for row in con.execute(sql).fetchall()]
        finally:
            con.close()

    def columns(self, namespace: str, table: str) -> list[str]:
        return [str(row[0]) for row in self.query(f"DESCRIBE {self.table_ref(namespace, table)}")]

    def table_ref(self, namespace: str, table: str) -> str:
        return f'{self.catalog}."{namespace}"."{table}"'

    def insert(self, namespace: str, table: str, rows: Sequence[Sequence[object]]) -> None:
        self.query(f"INSERT INTO {self.table_ref(namespace, table)} VALUES {_values(rows)}")

    def close(self) -> None:
        pass


class SparkEngine:
    name = "spark"
    capabilities = Capabilities(reads_views=True, writes=True)
    view_dialect = "spark"
    catalog = "lk"

    def __init__(self, url: str = "sc://localhost:15002") -> None:
        sql = importlib.import_module("pyspark.sql")
        self._spark = sql.SparkSession.builder.remote(url).getOrCreate()

    def ping(self) -> None:
        self.query(f"SHOW NAMESPACES IN {self.catalog}")

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        return [tuple(row) for row in self._spark.sql(sql).collect()]

    def columns(self, namespace: str, table: str) -> list[str]:
        return list(self._spark.table(self.table_ref(namespace, table)).columns)

    def table_ref(self, namespace: str, table: str) -> str:
        return f"{self.catalog}.`{namespace}`.`{table}`"

    def insert(self, namespace: str, table: str, rows: Sequence[Sequence[object]]) -> None:
        self.query(f"INSERT INTO {self.table_ref(namespace, table)} VALUES {_values(rows)}")

    def close(self) -> None:
        self._spark.stop()


@dataclass(frozen=True)
class EngineSpec:
    name: str
    # Import name of the client library from the ``engines`` dependency group.
    client_module: str
    connect: Callable[[], Engine]
    # Compose overlay and the service in it to wait on; None for in-process engines.
    overlay: Path | None = None
    service: str | None = None
    # Seconds to wait for the service to turn healthy after ``up -d``.
    start_timeout: float = 300.0

    def client_available(self) -> bool:
        return importlib.util.find_spec(self.client_module) is not None


ENGINES: dict[str, EngineSpec] = {
    "trino": EngineSpec(
        name="trino",
        client_module="trino",
        connect=TrinoEngine,
        overlay=_DOCKER / "engines" / "trino" / "compose.yml",
        service="trino",
    ),
    "spark": EngineSpec(
        name="spark",
        client_module="pyspark",
        connect=SparkEngine,
        overlay=_DOCKER / "engines" / "spark" / "compose.yml",
        service="spark",
        # --packages downloads the Iceberg runtime from Maven Central at start.
        start_timeout=900.0,
    ),
    "starrocks": EngineSpec(
        name="starrocks",
        client_module="pymysql",
        connect=StarRocksEngine,
        overlay=_DOCKER / "engines" / "starrocks" / "compose.yml",
        service="starrocks",
        start_timeout=600.0,
    ),
    "duckdb": EngineSpec(name="duckdb", client_module="duckdb", connect=DuckDBEngine),
}
