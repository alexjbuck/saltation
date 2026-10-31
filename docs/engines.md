# Engine compatibility tests

saltation applies DDL and `INSERT ... VALUES` through PyIceberg. Nothing in
that path runs a query engine, so `tests/engines/` checks that what saltation
writes is what engines expect: each test applies an evolution with saltation's
own runner (`apply_idempotent`, the code behind `saltation apply`), then a
real engine reads the result from the same Iceberg REST catalog
(Lakekeeper + rustfs, the integration rig). Where the engine can write, it
writes too, and saltation has to stay idempotent on the table afterwards.

The suite runs in `.github/workflows/engines.yml`: one job per engine on pull
requests, pushes to `main`, a weekly schedule and manual dispatch.

## What is tested

| Scenario (test module) | Trino | Spark | StarRocks | DuckDB |
| --- | --- | --- | --- | --- |
| Typed table, `PARTITIONED BY (days(ts), bucket(4, id))`, TBLPROPERTIES; column order, row count, scalar, decimal, date and struct-field values (`test_read_types`) | yes | yes | yes | yes |
| Schema evolution: add, rename, widen `int` to `bigint`, drop, with rows written before and after (`test_schema_evolution`) | yes | yes | yes | yes |
| Partition evolution: `ADD PARTITION FIELD` on a populated table, rows under both specs, filter on the new source column (`test_partition_evolution`) | yes | yes | yes | yes |
| Engine `INSERT`s into a saltation table; PyIceberg reads them back; a forced re-apply changes nothing (`test_engine_writes`) | yes | yes | yes | yes |
| Iceberg view stored by saltation in the engine's dialect (`test_views`) | yes | yes | yes | skipped |
| `WRITE ORDERED BY` + `SET IDENTIFIER FIELDS` (`test_sort_order`) | yes | yes | yes | yes |

DuckDB's iceberg extension documents no Iceberg view support, so the view
test is skipped through the driver's capabilities, not removed.

Two deliberate shapes in the tests:

- saltation's `INSERT ... VALUES` only takes untyped literals (numbers,
  strings, booleans, `NULL`), so the typed-table test writes its decimal,
  date, timestamp, struct and array values with a PyIceberg `append`.
- View bodies use the engine's three-part table name (`lakekeeper."ns"."t"`,
  `lk.ns.t`). saltation's views carry no `default-catalog`, and Trino and
  StarRocks then resolve a body outside the view's own catalog. The view's
  column list is spelled out because saltation can't type a query over a
  catalog-qualified name.

## Versions

| Engine | Image / client | Talks to |
| --- | --- | --- |
| Trino | `trinodb/trino:484`, `trino` (Python DB-API) | Iceberg connector, REST catalog, native S3 (`docker/engines/trino/etc/catalog/lakekeeper.properties`) |
| Spark | `apache/spark:4.1.4` running Spark Connect with `iceberg-spark-runtime-4.1_2.13:1.12.0` + `iceberg-aws-bundle:1.12.0`; `pyspark-client` 4.1 | `SparkCatalog` named `lk` (`docker/engines/spark/compose.yml`) |
| StarRocks | `starrocks/allin1-ubuntu:4.1.7`, `pymysql` | external catalog `lakekeeper`, created by the driver at session start |
| DuckDB | `duckdb` 2.0 (in-process) with the `iceberg` and `httpfs` extensions | `ATTACH ... (TYPE iceberg)` as `lk` |

Engine clients live in the `engines` dependency group, which a plain
`uv sync` does not install. Keep `pyspark-client` on the Spark minor of the
image.

## Running one engine locally

```sh
uv sync --group engines
SALTATION_TEST_ENGINE=trino uv run pytest tests/engines -v
```

`SALTATION_TEST_ENGINE` is one of `trino`, `spark`, `starrocks`, `duckdb`.
Without it the suite skips, so a plain `pytest` never starts a second copy of
the rig next to `tests/integration`. The session fixture runs
`docker compose -f docker/compose.yml -f docker/engines/<engine>/compose.yml up -d`,
waits for Lakekeeper, the bootstrap container and the engine's healthcheck,
and runs `down -v` at the end. Set `SALTATION_TEST_KEEP_STACK=1` to leave the
stack up for poking at it.

On Linux, add `127.0.0.1 host.docker.internal` to `/etc/hosts` first, as for
the integration suite: Lakekeeper vends that storage endpoint to clients.

The Spark container downloads the Iceberg jars from Maven Central on every
start, so its first healthy state takes a few minutes.

### Ports

The base rig keeps its usual ports (Postgres 5432, rustfs 9000, Lakekeeper
8181), so the integration suite and an engine stack can't run at the same
time.

| Engine | Host port |
| --- | --- |
| Trino | 8080 (HTTP) |
| Spark | 15002 (Spark Connect) |
| StarRocks | 9030 (MySQL protocol), 8030 (FE HTTP) |
| DuckDB | none (in-process) |

## Adding an engine

1. **Overlay**: `docker/engines/<engine>/compose.yml` adds one service on the
   `saltation` network that depends on `lakekeeper` (healthy) and `bootstrap`
   (completed), pins its image with the date it was chosen, publishes a host
   port and has a healthcheck. Relative paths in an overlay resolve against
   `docker/`, the directory of the first compose file. Point the engine at
   `http://lakekeeper:8181/catalog`, warehouse `saltation`, and either at
   rustfs directly (`http://rustfs:9000`, path-style, the static keys) or at
   the vended `http://host.docker.internal:9000` with
   `extra_hosts: ["host.docker.internal:host-gateway"]`. Turn off any
   metadata cache: saltation changes tables between queries.
2. **Driver**: a class in `tests/engines/engines.py` that satisfies the
   `Engine` protocol (`ping`, `query`, `columns`, `table_ref`, `insert`,
   `close`, plus `capabilities` and `view_dialect`), importing its client
   inside `__init__`. Register it in `ENGINES` with its client module,
   overlay, service name and start timeout, and add the client with
   `uv add --group engines <package>`.
3. **CI**: add the name to the `engine` matrix in
   `.github/workflows/engines.yml`.
4. **Docs**: add a column to the tables above.

## Not included yet

- **ClickHouse** reads Iceberg REST catalogs through its `DataLakeCatalog`
  database engine, which its docs still mark as beta behind
  `allow_database_iceberg`. It is worth adding once that is stable.
- **Dremio** supports Iceberg REST catalogs, but its image is heavy and slow
  to start, and its catalog setup goes through its own REST API or UI. That is
  a lot of rig for coverage that mostly overlaps Trino.
- **Flink** uses the same Iceberg Java library as Spark, so it adds little
  read-path signal. Running it needs a JobManager, a TaskManager, a SQL
  gateway and the matching `iceberg-flink-runtime` jar. Add it if streaming
  writers become a target.
- **Daft** reads Iceberg through PyIceberg, the same library saltation
  writes with, so it would mostly test PyIceberg against itself.
