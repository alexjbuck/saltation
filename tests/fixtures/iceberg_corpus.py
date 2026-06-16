"""Authoritative corpus of Iceberg Spark SQL statements for coverage testing.

Each :class:`CorpusEntry` is one statement (or, rarely, a short multi-statement
script) with a stable ``id``, a ``category``, the ``sql`` text, an ``expected``
verdict (``"apply"`` for in-scope positives that must eventually parse AND
dispatch cleanly, ``"reject"`` for out-of-scope statements that must surface as
:class:`cambrian.errors.UnsupportedStatementError`), and a short ``note`` —
usually citing the authoritative source.

Sources (verified 2026-05-26):

* Iceberg Spark DDL — https://iceberg.apache.org/docs/latest/spark-ddl/
  (raw: github.com/apache/iceberg/blob/main/docs/docs/spark-ddl.md)
* Iceberg Spark Procedures — https://iceberg.apache.org/docs/latest/spark-procedures/
* Iceberg Spark Writes / Queries (time travel) —
  https://iceberg.apache.org/docs/latest/spark-writes/ and spark-queries.md

The module is intentionally import-light: a dataclass and a list, no heavy
imports, so tests can iterate over ``CORPUS`` without pulling in PyIceberg at
collection time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

__all__ = ["CORPUS", "CorpusEntry", "negatives", "positives"]

Expected = Literal["apply", "reject"]


@dataclass(frozen=True)
class CorpusEntry:
    """One corpus statement and its expected end-to-end verdict.

    ``expected`` is the *contract*, not the current behaviour: ``"apply"``
    entries are in cambrian's v1 scope and should parse and dispatch without
    error once the implementation is complete; ``"reject"`` entries are valid
    Iceberg/Spark SQL (or plausible syntax) deliberately out of scope, which
    must raise :class:`UnsupportedStatementError` rather than crash. The
    classification harness measures the gap between this contract and reality.
    """

    id: str
    category: str
    sql: str
    expected: Expected
    note: str


def positives() -> list[CorpusEntry]:
    """All in-scope entries (``expected == "apply"``)."""
    return [e for e in CORPUS if e.expected == "apply"]


def negatives() -> list[CorpusEntry]:
    """All out-of-scope entries (``expected == "reject"``)."""
    return [e for e in CORPUS if e.expected == "reject"]


# Source URL shorthands reused in notes.
_DDL = "iceberg.apache.org/docs/latest/spark-ddl"
_PROC = "iceberg.apache.org/docs/latest/spark-procedures"
_WRITES = "iceberg.apache.org/docs/latest/spark-writes"
_QUERIES = "iceberg.apache.org/docs/latest/spark-queries"


CORPUS: list[CorpusEntry] = [
    # =====================================================================
    # POSITIVE — Namespace
    # =====================================================================
    CorpusEntry(
        "ns_create",
        "namespace",
        "CREATE NAMESPACE foo",
        "apply",
        f"{_DDL}; stock Spark parses to Create kind=NAMESPACE.",
    ),
    CorpusEntry(
        "ns_create_dotted",
        "namespace",
        "CREATE NAMESPACE foo.bar",
        "apply",
        "Multi-part namespace.",
    ),
    CorpusEntry(
        "ns_create_if_not_exists",
        "namespace",
        "CREATE NAMESPACE IF NOT EXISTS foo",
        "apply",
        "IF NOT EXISTS variant; exists=True on Create.",
    ),
    CorpusEntry(
        "ns_create_with_properties",
        "namespace",
        "CREATE NAMESPACE foo WITH PROPERTIES ('owner' = 'eng')",
        "apply",
        f"{_DDL}; CREATE NAMESPACE ... WITH PROPERTIES is valid Spark.",
    ),
    CorpusEntry(
        "ns_drop",
        "namespace",
        "DROP NAMESPACE foo",
        "apply",
        "Drop kind=NAMESPACE.",
    ),
    CorpusEntry(
        "ns_drop_if_exists",
        "namespace",
        "DROP NAMESPACE IF EXISTS foo",
        "apply",
        "IF EXISTS guards the not-found path.",
    ),
    CorpusEntry(
        "ns_alter_set_properties",
        "namespace",
        "ALTER NAMESPACE foo SET PROPERTIES ('owner' = 'data')",
        "apply",
        f"{_DDL}; valid Iceberg-Spark namespace property update.",
    ),
    # =====================================================================
    # POSITIVE — CREATE TABLE: primitive types
    # =====================================================================
    CorpusEntry(
        "ct_simple",
        "create_table",
        "CREATE TABLE foo.t (id BIGINT, name STRING) USING iceberg",
        "apply",
        f"{_DDL}; the canonical minimal create.",
    ),
    CorpusEntry(
        "ct_if_not_exists",
        "create_table",
        "CREATE TABLE IF NOT EXISTS foo.t (id BIGINT) USING iceberg",
        "apply",
        "IF NOT EXISTS variant.",
    ),
    CorpusEntry(
        "ct_all_primitives",
        "create_table",
        (
            "CREATE TABLE foo.t ("
            "a BOOLEAN, b INT, c BIGINT, d FLOAT, e DOUBLE, f DECIMAL(10, 2), "
            "g DATE, h TIMESTAMP, i TIMESTAMP_NTZ, j STRING, k BINARY"
            ") USING iceberg"
        ),
        "apply",
        (
            f"{_DDL}; every Iceberg primitive. TIMESTAMP=tz, TIMESTAMP_NTZ=no-tz "
            "per Spark 3.4+ Iceberg mapping."
        ),
    ),
    CorpusEntry(
        "ct_timestamptz_alias",
        "create_table",
        "CREATE TABLE foo.t (ts TIMESTAMPTZ) USING iceberg",
        "apply",
        "TIMESTAMPTZ alias maps to Iceberg timestamptz.",
    ),
    CorpusEntry(
        "ct_decimal_precision_scale",
        "create_table",
        "CREATE TABLE foo.t (amount DECIMAL(38, 9)) USING iceberg",
        "apply",
        "DECIMAL(p,s) precision/scale extraction.",
    ),
    CorpusEntry(
        "ct_not_null",
        "create_table",
        "CREATE TABLE foo.t (id BIGINT NOT NULL, name STRING) USING iceberg",
        "apply",
        f"{_DDL}; NOT NULL → required Iceberg field.",
    ),
    CorpusEntry(
        "ct_column_comment",
        "create_table",
        "CREATE TABLE foo.t (id BIGINT COMMENT 'unique id', name STRING) USING iceberg",
        "apply",
        f"{_DDL}; per-column COMMENT.",
    ),
    CorpusEntry(
        "ct_table_comment",
        "create_table",
        "CREATE TABLE foo.t (id BIGINT) USING iceberg COMMENT 'table docs'",
        "apply",
        f"{_DDL}; table-level COMMENT clause.",
    ),
    CorpusEntry(
        "ct_location",
        "create_table",
        "CREATE TABLE foo.t (id BIGINT) USING iceberg LOCATION 's3://bucket/path'",
        "apply",
        f"{_DDL}; LOCATION clause.",
    ),
    CorpusEntry(
        "ct_tblproperties",
        "create_table",
        (
            "CREATE TABLE foo.t (id BIGINT) USING iceberg "
            "TBLPROPERTIES ('write.format.default' = 'parquet')"
        ),
        "apply",
        f"{_DDL}; TBLPROPERTIES at create time.",
    ),
    CorpusEntry(
        "ct_full",
        "create_table",
        (
            "CREATE TABLE foo.t (id BIGINT NOT NULL COMMENT 'pk', data STRING) "
            "USING iceberg PARTITIONED BY (bucket(16, id)) "
            "LOCATION 's3://bucket/t' COMMENT 'docs' "
            "TBLPROPERTIES ('k' = 'v')"
        ),
        "apply",
        f"{_DDL}; combined clauses.",
    ),
    # ----- nested types -----
    CorpusEntry(
        "ct_struct",
        "create_table_nested",
        "CREATE TABLE foo.t (point STRUCT<x: DOUBLE, y: DOUBLE>) USING iceberg",
        "apply",
        f"{_DDL}; STRUCT column.",
    ),
    CorpusEntry(
        "ct_array",
        "create_table_nested",
        "CREATE TABLE foo.t (tags ARRAY<STRING>) USING iceberg",
        "apply",
        f"{_DDL}; ARRAY column.",
    ),
    CorpusEntry(
        "ct_map",
        "create_table_nested",
        "CREATE TABLE foo.t (attrs MAP<STRING, INT>) USING iceberg",
        "apply",
        f"{_DDL}; MAP column.",
    ),
    CorpusEntry(
        "ct_nested_combo",
        "create_table_nested",
        (
            "CREATE TABLE foo.t ("
            "points ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>, "
            "lookup MAP<STRING, ARRAY<INT>>"
            ") USING iceberg"
        ),
        "apply",
        "Nested composite combinations (array of struct, map of array).",
    ),
    # ----- PARTITIONED BY transforms -----
    CorpusEntry(
        "ct_part_identity_col",
        "create_table_partition",
        "CREATE TABLE foo.t (id BIGINT, category STRING) USING iceberg PARTITIONED BY (category)",
        "apply",
        f"{_DDL}; bare-column identity partition.",
    ),
    CorpusEntry(
        "ct_part_bucket",
        "create_table_partition",
        "CREATE TABLE foo.t (id BIGINT) USING iceberg PARTITIONED BY (bucket(16, id))",
        "apply",
        f"{_DDL}; bucket(N, col).",
    ),
    CorpusEntry(
        "ct_part_truncate",
        "create_table_partition",
        "CREATE TABLE foo.t (data STRING) USING iceberg PARTITIONED BY (truncate(4, data))",
        "apply",
        f"{_DDL}; truncate(L, col).",
    ),
    CorpusEntry(
        "ct_part_years",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (years(ts))",
        "apply",
        f"{_DDL}; legacy years() transform.",
    ),
    CorpusEntry(
        "ct_part_year",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (year(ts))",
        "apply",
        f"{_DDL}; year() transform.",
    ),
    CorpusEntry(
        "ct_part_months",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (months(ts))",
        "apply",
        f"{_DDL}; legacy months() transform.",
    ),
    CorpusEntry(
        "ct_part_month",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (month(ts))",
        "apply",
        f"{_DDL}; month() transform.",
    ),
    CorpusEntry(
        "ct_part_days",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts))",
        "apply",
        f"{_DDL}; legacy days() transform.",
    ),
    CorpusEntry(
        "ct_part_day",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (day(ts))",
        "apply",
        f"{_DDL}; day() transform.",
    ),
    CorpusEntry(
        "ct_part_hours",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (hours(ts))",
        "apply",
        f"{_DDL}; legacy hours() transform.",
    ),
    CorpusEntry(
        "ct_part_hour",
        "create_table_partition",
        "CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (hour(ts))",
        "apply",
        f"{_DDL}; hour() transform.",
    ),
    CorpusEntry(
        "ct_part_multi",
        "create_table_partition",
        (
            "CREATE TABLE foo.t (id BIGINT, ts TIMESTAMP, category STRING) "
            "USING iceberg PARTITIONED BY (bucket(16, id), days(ts), category)"
        ),
        "apply",
        f"{_DDL}; multiple partition transforms in one clause.",
    ),
    # =====================================================================
    # POSITIVE — DROP TABLE / RENAME
    # =====================================================================
    CorpusEntry(
        "dt_simple",
        "drop_table",
        "DROP TABLE foo.t",
        "apply",
        f"{_DDL}; drop table.",
    ),
    CorpusEntry(
        "dt_if_exists",
        "drop_table",
        "DROP TABLE IF EXISTS foo.t",
        "apply",
        "IF EXISTS variant.",
    ),
    CorpusEntry(
        "dt_purge",
        "drop_table",
        "DROP TABLE foo.t PURGE",
        "apply",
        f"{_DDL}; PURGE removes data too.",
    ),
    CorpusEntry(
        "dt_if_exists_purge",
        "drop_table",
        "DROP TABLE IF EXISTS foo.t PURGE",
        "apply",
        "IF EXISTS + PURGE combination.",
    ),
    CorpusEntry(
        "rename_table",
        "rename_table",
        "ALTER TABLE foo.t RENAME TO foo.t2",
        "apply",
        f"{_DDL}; ALTER TABLE ... RENAME TO.",
    ),
    # =====================================================================
    # POSITIVE — Schema evolution: columns
    # =====================================================================
    CorpusEntry(
        "add_column_single",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN c INT",
        "apply",
        f"{_DDL}; singular ADD COLUMN.",
    ),
    CorpusEntry(
        "add_column_comment",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN c STRING COMMENT 'docs'",
        "apply",
        "ADD COLUMN with per-column COMMENT.",
    ),
    CorpusEntry(
        "add_column_not_null",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN c INT NOT NULL",
        "apply",
        "ADD COLUMN with NOT NULL.",
    ),
    CorpusEntry(
        "add_column_first",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN c INT FIRST",
        "apply",
        f"{_DDL}; FIRST positioning.",
    ),
    CorpusEntry(
        "add_column_after",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN c INT AFTER name",
        "apply",
        f"{_DDL}; AFTER positioning.",
    ),
    CorpusEntry(
        "add_column_nested",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMN point.z DOUBLE",
        "apply",
        f"{_DDL}; nested-field add (dotted path).",
    ),
    CorpusEntry(
        "add_columns_plural",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMNS (a INT, b STRING, c BIGINT)",
        "apply",
        f"{_DDL}; plural ADD COLUMNS list.",
    ),
    CorpusEntry(
        "add_columns_plural_comment",
        "schema_add",
        "ALTER TABLE foo.t ADD COLUMNS (a INT COMMENT 'x', b STRING)",
        "apply",
        "Plural ADD COLUMNS with comment.",
    ),
    CorpusEntry(
        "drop_column_single",
        "schema_drop",
        "ALTER TABLE foo.t DROP COLUMN c",
        "apply",
        f"{_DDL}; singular DROP COLUMN (Iceberg extension).",
    ),
    CorpusEntry(
        "drop_column_nested",
        "schema_drop",
        "ALTER TABLE foo.t DROP COLUMN point.z",
        "apply",
        f"{_DDL}; nested DROP COLUMN.",
    ),
    CorpusEntry(
        "drop_columns_plural",
        "schema_drop",
        "ALTER TABLE foo.t DROP COLUMNS (a, b)",
        "apply",
        "Plural DROP COLUMNS.",
    ),
    CorpusEntry(
        "rename_column",
        "schema_rename",
        "ALTER TABLE foo.t RENAME COLUMN data TO payload",
        "apply",
        f"{_DDL}; RENAME COLUMN.",
    ),
    CorpusEntry(
        "rename_column_nested",
        "schema_rename",
        "ALTER TABLE foo.t RENAME COLUMN location.lat TO latitude",
        "apply",
        f"{_DDL}; nested RENAME COLUMN.",
    ),
    CorpusEntry(
        "alter_column_type",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN measurement TYPE DOUBLE",
        "apply",
        f"{_DDL}; type widening.",
    ),
    CorpusEntry(
        "alter_column_comment",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN measurement COMMENT 'unit kb/s'",
        "apply",
        f"{_DDL}; comment-only ALTER COLUMN.",
    ),
    CorpusEntry(
        "alter_column_type_comment",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN measurement TYPE DOUBLE COMMENT 'unit'",
        "apply",
        f"{_DDL}; TYPE and COMMENT together.",
    ),
    CorpusEntry(
        "alter_column_first",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN c FIRST",
        "apply",
        f"{_DDL}; reorder to FIRST.",
    ),
    CorpusEntry(
        "alter_column_after",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN c AFTER other",
        "apply",
        f"{_DDL}; reorder AFTER.",
    ),
    CorpusEntry(
        "alter_column_set_not_null",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN id SET NOT NULL",
        "apply",
        "SET NOT NULL (requirement tightening).",
    ),
    CorpusEntry(
        "alter_column_drop_not_null",
        "schema_alter",
        "ALTER TABLE foo.t ALTER COLUMN id DROP NOT NULL",
        "apply",
        f"{_DDL}; DROP NOT NULL.",
    ),
    # =====================================================================
    # POSITIVE — Partition evolution
    # =====================================================================
    CorpusEntry(
        "apf_identity",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD category",
        "apply",
        f"{_DDL}; bare-column identity ADD PARTITION FIELD.",
    ),
    CorpusEntry(
        "apf_bucket",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id)",
        "apply",
        f"{_DDL}; bucket transform.",
    ),
    CorpusEntry(
        "apf_truncate",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD truncate(4, data)",
        "apply",
        f"{_DDL}; truncate transform.",
    ),
    CorpusEntry(
        "apf_year",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD year(ts)",
        "apply",
        f"{_DDL}; year transform.",
    ),
    CorpusEntry(
        "apf_day",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD day(ts)",
        "apply",
        "day transform.",
    ),
    CorpusEntry(
        "apf_hour",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD hour(ts)",
        "apply",
        "hour transform.",
    ),
    CorpusEntry(
        "apf_alias",
        "partition_evolution",
        "ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id) AS shard",
        "apply",
        f"{_DDL}; AS alias.",
    ),
    CorpusEntry(
        "dpf_identity",
        "partition_evolution",
        "ALTER TABLE foo.t DROP PARTITION FIELD category",
        "apply",
        f"{_DDL}; drop by name.",
    ),
    CorpusEntry(
        "dpf_transform",
        "partition_evolution",
        "ALTER TABLE foo.t DROP PARTITION FIELD bucket(16, id)",
        "apply",
        f"{_DDL}; drop by transform expr.",
    ),
    CorpusEntry(
        "dpf_alias_name",
        "partition_evolution",
        "ALTER TABLE foo.t DROP PARTITION FIELD shard",
        "apply",
        "Drop by alias name.",
    ),
    CorpusEntry(
        "rpf_basic",
        "partition_evolution",
        "ALTER TABLE foo.t REPLACE PARTITION FIELD ts_day WITH day(ts)",
        "apply",
        f"{_DDL}; replace partition field.",
    ),
    CorpusEntry(
        "rpf_alias",
        "partition_evolution",
        "ALTER TABLE foo.t REPLACE PARTITION FIELD ts_day WITH day(ts) AS day_of_ts",
        "apply",
        f"{_DDL}; replace with AS alias.",
    ),
    # =====================================================================
    # POSITIVE — Sort order
    # =====================================================================
    CorpusEntry(
        "wob_bare",
        "sort_order",
        "ALTER TABLE foo.t WRITE ORDERED BY category, id",
        "apply",
        f"{_DDL}; canonical WRITE ORDERED BY (no parens, comma list).",
    ),
    CorpusEntry(
        "wob_paren",
        "sort_order",
        "ALTER TABLE foo.t WRITE ORDERED BY (category, id)",
        "apply",
        "Parenthesised form (cambrian's existing tests use this).",
    ),
    CorpusEntry(
        "wob_asc_desc",
        "sort_order",
        "ALTER TABLE foo.t WRITE ORDERED BY category ASC, id DESC",
        "apply",
        f"{_DDL}; explicit directions.",
    ),
    CorpusEntry(
        "wob_nulls",
        "sort_order",
        "ALTER TABLE foo.t WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST",
        "apply",
        f"{_DDL}; NULLS FIRST/LAST.",
    ),
    CorpusEntry(
        "wob_transform",
        "sort_order",
        "ALTER TABLE foo.t WRITE ORDERED BY bucket(16, id)",
        "apply",
        "Transform-in-sort variant.",
    ),
    CorpusEntry(
        "wob_locally",
        "sort_order",
        "ALTER TABLE foo.t WRITE LOCALLY ORDERED BY category, id",
        "apply",
        f"{_DDL}; WRITE LOCALLY ORDERED BY.",
    ),
    CorpusEntry(
        "wob_distributed",
        "sort_order",
        "ALTER TABLE foo.t WRITE DISTRIBUTED BY PARTITION",
        "apply",
        f"{_DDL}; WRITE DISTRIBUTED BY PARTITION.",
    ),
    CorpusEntry(
        "wob_distributed_locally",
        "sort_order",
        "ALTER TABLE foo.t WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id",
        "apply",
        f"{_DDL}; distributed + locally ordered.",
    ),
    CorpusEntry(
        "wob_unordered",
        "sort_order",
        "ALTER TABLE foo.t WRITE UNORDERED",
        "apply",
        f"{_DDL}; WRITE UNORDERED.",
    ),
    # =====================================================================
    # POSITIVE — TBLPROPERTIES
    # =====================================================================
    CorpusEntry(
        "set_tblproperties",
        "properties",
        "ALTER TABLE foo.t SET TBLPROPERTIES ('read.split.target-size' = '268435456')",
        "apply",
        f"{_DDL}; SET TBLPROPERTIES.",
    ),
    CorpusEntry(
        "set_tblproperties_multi",
        "properties",
        "ALTER TABLE foo.t SET TBLPROPERTIES ('a' = '1', 'b' = '2')",
        "apply",
        "Multiple keys in SET.",
    ),
    CorpusEntry(
        "unset_tblproperties",
        "properties",
        "ALTER TABLE foo.t UNSET TBLPROPERTIES ('read.split.target-size')",
        "apply",
        f"{_DDL}; UNSET TBLPROPERTIES.",
    ),
    CorpusEntry(
        "unset_tblproperties_multi",
        "properties",
        "ALTER TABLE foo.t UNSET TBLPROPERTIES ('a', 'b', 'c')",
        "apply",
        "Multiple keys in UNSET.",
    ),
    # =====================================================================
    # POSITIVE — Identifier fields (V2)
    # =====================================================================
    CorpusEntry(
        "set_identifier_fields_single",
        "identifier_fields",
        "ALTER TABLE foo.t SET IDENTIFIER FIELDS id",
        "apply",
        f"{_DDL}; SET IDENTIFIER FIELDS (single).",
    ),
    CorpusEntry(
        "set_identifier_fields_multi",
        "identifier_fields",
        "ALTER TABLE foo.t SET IDENTIFIER FIELDS id, data",
        "apply",
        f"{_DDL}; SET IDENTIFIER FIELDS (multiple).",
    ),
    CorpusEntry(
        "drop_identifier_fields",
        "identifier_fields",
        "ALTER TABLE foo.t DROP IDENTIFIER FIELDS id",
        "apply",
        f"{_DDL}; DROP IDENTIFIER FIELDS.",
    ),
    # =====================================================================
    # POSITIVE — Data ops feasible in-process
    # =====================================================================
    CorpusEntry(
        "insert_values_single",
        "data_ops",
        "INSERT INTO foo.t VALUES (1, 'alice')",
        "apply",
        f"{_WRITES}; INSERT VALUES single row.",
    ),
    CorpusEntry(
        "insert_values_multi",
        "data_ops",
        "INSERT INTO foo.t VALUES (1, 'alice'), (2, 'bob')",
        "apply",
        f"{_WRITES}; INSERT VALUES multi-row.",
    ),
    CorpusEntry(
        "insert_values_null",
        "data_ops",
        "INSERT INTO foo.t VALUES (1, NULL)",
        "apply",
        "INSERT VALUES with NULL.",
    ),
    CorpusEntry(
        "delete_where",
        "data_ops",
        "DELETE FROM foo.t WHERE id = 1",
        "apply",
        f"{_WRITES}; DELETE FROM ... WHERE — feasible via PyIceberg delete().",
    ),
    CorpusEntry(
        "delete_where_predicate",
        "data_ops",
        "DELETE FROM foo.t WHERE category = 'x' AND id > 10",
        "apply",
        "DELETE with compound predicate.",
    ),
    # =====================================================================
    # NEGATIVE — out-of-scope writes
    # =====================================================================
    CorpusEntry(
        "neg_insert_select",
        "neg_data_ops",
        "INSERT INTO foo.t SELECT * FROM bar",
        "reject",
        f"{_WRITES}; INSERT ... SELECT out of scope (needs query engine).",
    ),
    CorpusEntry(
        "neg_update",
        "neg_data_ops",
        "UPDATE foo.t SET k = 1 WHERE id = 2",
        "reject",
        f"{_WRITES}; UPDATE out of scope.",
    ),
    CorpusEntry(
        "neg_merge",
        "neg_data_ops",
        (
            "MERGE INTO foo.t USING bar.s ON foo.t.id = bar.s.id "
            "WHEN MATCHED THEN UPDATE SET k = 1 "
            "WHEN NOT MATCHED THEN INSERT (id, k) VALUES (bar.s.id, 0)"
        ),
        "reject",
        f"{_WRITES}; MERGE out of scope.",
    ),
    # =====================================================================
    # NEGATIVE — branching / tagging
    # =====================================================================
    CorpusEntry(
        "neg_create_branch",
        "neg_branching",
        "ALTER TABLE foo.t CREATE BRANCH `audit-branch`",
        "reject",
        f"{_DDL}; branching out of scope (Nessie-style workflow excluded).",
    ),
    CorpusEntry(
        "neg_create_tag",
        "neg_branching",
        "ALTER TABLE foo.t CREATE TAG `historical-tag`",
        "reject",
        f"{_DDL}; tagging out of scope.",
    ),
    CorpusEntry(
        "neg_replace_branch",
        "neg_branching",
        "ALTER TABLE foo.t REPLACE BRANCH `audit-branch` AS OF VERSION 4567 RETAIN 60 DAYS",
        "reject",
        f"{_DDL}; replace branch out of scope.",
    ),
    CorpusEntry(
        "neg_drop_branch",
        "neg_branching",
        "ALTER TABLE foo.t DROP BRANCH `audit-branch`",
        "reject",
        f"{_DDL}; drop branch out of scope.",
    ),
    CorpusEntry(
        "neg_drop_tag",
        "neg_branching",
        "ALTER TABLE foo.t DROP TAG `historical-tag`",
        "reject",
        f"{_DDL}; drop tag out of scope.",
    ),
    # =====================================================================
    # NEGATIVE — stored procedures (CALL)
    # =====================================================================
    CorpusEntry(
        "neg_call_rewrite_data_files",
        "neg_procedures",
        "CALL cat.system.rewrite_data_files('db.t')",
        "reject",
        f"{_PROC}; table maintenance procedure out of scope.",
    ),
    CorpusEntry(
        "neg_call_expire_snapshots",
        "neg_procedures",
        (
            "CALL cat.system.expire_snapshots("
            "table => 'db.t', older_than => TIMESTAMP '2021-06-30 00:00:00.000')"
        ),
        "reject",
        f"{_PROC}; expire_snapshots out of scope.",
    ),
    CorpusEntry(
        "neg_call_rollback_to_snapshot",
        "neg_procedures",
        "CALL cat.system.rollback_to_snapshot('db.t', 1)",
        "reject",
        f"{_PROC}; rollback procedure out of scope.",
    ),
    CorpusEntry(
        "neg_call_rewrite_manifests",
        "neg_procedures",
        "CALL cat.system.rewrite_manifests('db.t')",
        "reject",
        f"{_PROC}; rewrite_manifests out of scope.",
    ),
    # =====================================================================
    # NEGATIVE — pseudo-syntax (NOT real Spark-Iceberg SQL)
    # =====================================================================
    CorpusEntry(
        "neg_optimize_write",
        "neg_pseudo",
        "ALTER TABLE foo.t OPTIMIZE WRITE",
        "reject",
        "Not real Iceberg-Spark SQL; from a user's example. Parses to Command.",
    ),
    CorpusEntry(
        "neg_expire_snapshots_pseudo",
        "neg_pseudo",
        "ALTER TABLE foo.t EXPIRE SNAPSHOTS BEFORE NOW()",
        "reject",
        "Not real Iceberg-Spark SQL; real path is CALL expire_snapshots. Parses to Command.",
    ),
    # =====================================================================
    # NEGATIVE — CTAS / REPLACE TABLE
    # =====================================================================
    CorpusEntry(
        "neg_ctas",
        "neg_create",
        "CREATE TABLE foo.t USING iceberg AS SELECT * FROM bar",
        "reject",
        f"{_DDL}; CTAS out of scope (needs query engine).",
    ),
    CorpusEntry(
        "neg_create_or_replace_table",
        "neg_create",
        "CREATE OR REPLACE TABLE foo.t (id INT) USING iceberg",
        "reject",
        f"{_DDL}; CREATE OR REPLACE TABLE out of scope (not idempotent-safe).",
    ),
    CorpusEntry(
        "neg_replace_table_as_select",
        "neg_create",
        "REPLACE TABLE foo.t USING iceberg AS SELECT * FROM bar",
        "reject",
        f"{_DDL}; REPLACE TABLE AS SELECT out of scope.",
    ),
    # =====================================================================
    # NEGATIVE — time-travel reads
    # =====================================================================
    CorpusEntry(
        "neg_time_travel_version",
        "neg_time_travel",
        "SELECT * FROM foo.t FOR VERSION AS OF 123",
        "reject",
        f"{_QUERIES}; time-travel read; cambrian applies DDL, not SELECT.",
    ),
    CorpusEntry(
        "neg_time_travel_timestamp",
        "neg_time_travel",
        "SELECT * FROM foo.t FOR TIMESTAMP AS OF '2021-01-01 00:00:00'",
        "reject",
        f"{_QUERIES}; timestamp time-travel read.",
    ),
]
