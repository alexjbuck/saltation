-- Iceberg SQL corpus: create_table_nested
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [ct_struct] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; STRUCT column.
CREATE TABLE foo.t (point STRUCT<x: DOUBLE, y: DOUBLE>) USING iceberg;

-- [ct_array] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; ARRAY column.
CREATE TABLE foo.t (tags ARRAY<STRING>) USING iceberg;

-- [ct_map] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; MAP column.
CREATE TABLE foo.t (attrs MAP<STRING, INT>) USING iceberg;

-- [ct_nested_combo] expected=apply :: Nested composite combinations (array of struct, map of array).
CREATE TABLE foo.t (points ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>, lookup MAP<STRING, ARRAY<INT>>) USING iceberg;
