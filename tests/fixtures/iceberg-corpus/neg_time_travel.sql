-- Iceberg SQL corpus: neg_time_travel
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_time_travel_version] expected=reject :: iceberg.apache.org/docs/latest/spark-queries; time-travel read; cambrian applies DDL, not SELECT.
SELECT * FROM foo.t FOR VERSION AS OF 123;

-- [neg_time_travel_timestamp] expected=reject :: iceberg.apache.org/docs/latest/spark-queries; timestamp time-travel read.
SELECT * FROM foo.t FOR TIMESTAMP AS OF '2021-01-01 00:00:00';
