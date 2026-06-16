-- Iceberg SQL corpus: drop_table
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [dt_simple] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; drop table.
DROP TABLE foo.t;

-- [dt_if_exists] expected=apply :: IF EXISTS variant.
DROP TABLE IF EXISTS foo.t;

-- [dt_purge] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; PURGE removes data too.
DROP TABLE foo.t PURGE;

-- [dt_if_exists_purge] expected=apply :: IF EXISTS + PURGE combination.
DROP TABLE IF EXISTS foo.t PURGE;
