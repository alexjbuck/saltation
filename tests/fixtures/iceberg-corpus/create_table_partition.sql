-- Iceberg SQL corpus: create_table_partition
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [ct_part_identity_col] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; bare-column identity partition.
CREATE TABLE foo.t (id BIGINT, category STRING) USING iceberg PARTITIONED BY (category);

-- [ct_part_bucket] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; bucket(N, col).
CREATE TABLE foo.t (id BIGINT) USING iceberg PARTITIONED BY (bucket(16, id));

-- [ct_part_truncate] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; truncate(L, col).
CREATE TABLE foo.t (data STRING) USING iceberg PARTITIONED BY (truncate(4, data));

-- [ct_part_years] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; legacy years() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (years(ts));

-- [ct_part_year] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; year() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (year(ts));

-- [ct_part_months] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; legacy months() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (months(ts));

-- [ct_part_month] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; month() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (month(ts));

-- [ct_part_days] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; legacy days() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts));

-- [ct_part_day] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; day() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (day(ts));

-- [ct_part_hours] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; legacy hours() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (hours(ts));

-- [ct_part_hour] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; hour() transform.
CREATE TABLE foo.t (ts TIMESTAMP) USING iceberg PARTITIONED BY (hour(ts));

-- [ct_part_multi] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; multiple partition transforms in one clause.
CREATE TABLE foo.t (id BIGINT, ts TIMESTAMP, category STRING) USING iceberg PARTITIONED BY (bucket(16, id), days(ts), category);
