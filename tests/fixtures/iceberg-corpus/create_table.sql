-- Iceberg SQL corpus: create_table
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [ct_simple] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; the canonical minimal create.
CREATE TABLE foo.t (id BIGINT, name STRING) USING iceberg;

-- [ct_if_not_exists] expected=apply :: IF NOT EXISTS variant.
CREATE TABLE IF NOT EXISTS foo.t (id BIGINT) USING iceberg;

-- [ct_all_primitives] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; every Iceberg primitive. TIMESTAMP=tz, TIMESTAMP_NTZ=no-tz per Spark 3.4+ Iceberg mapping.
CREATE TABLE foo.t (a BOOLEAN, b INT, c BIGINT, d FLOAT, e DOUBLE, f DECIMAL(10, 2), g DATE, h TIMESTAMP, i TIMESTAMP_NTZ, j STRING, k BINARY) USING iceberg;

-- [ct_timestamptz_alias] expected=apply :: TIMESTAMPTZ alias maps to Iceberg timestamptz.
CREATE TABLE foo.t (ts TIMESTAMPTZ) USING iceberg;

-- [ct_decimal_precision_scale] expected=apply :: DECIMAL(p,s) precision/scale extraction.
CREATE TABLE foo.t (amount DECIMAL(38, 9)) USING iceberg;

-- [ct_not_null] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; NOT NULL → required Iceberg field.
CREATE TABLE foo.t (id BIGINT NOT NULL, name STRING) USING iceberg;

-- [ct_column_comment] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; per-column COMMENT.
CREATE TABLE foo.t (id BIGINT COMMENT 'unique id', name STRING) USING iceberg;

-- [ct_table_comment] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; table-level COMMENT clause.
CREATE TABLE foo.t (id BIGINT) USING iceberg COMMENT 'table docs';

-- [ct_location] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; LOCATION clause.
CREATE TABLE foo.t (id BIGINT) USING iceberg LOCATION 's3://bucket/path';

-- [ct_tblproperties] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; TBLPROPERTIES at create time.
CREATE TABLE foo.t (id BIGINT) USING iceberg TBLPROPERTIES ('write.format.default' = 'parquet');

-- [ct_full] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; combined clauses.
CREATE TABLE foo.t (id BIGINT NOT NULL COMMENT 'pk', data STRING) USING iceberg PARTITIONED BY (bucket(16, id)) LOCATION 's3://bucket/t' COMMENT 'docs' TBLPROPERTIES ('k' = 'v');
