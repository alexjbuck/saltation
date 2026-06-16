-- Iceberg SQL corpus: identifier_fields
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [set_identifier_fields_single] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; SET IDENTIFIER FIELDS (single).
ALTER TABLE foo.t SET IDENTIFIER FIELDS id;

-- [set_identifier_fields_multi] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; SET IDENTIFIER FIELDS (multiple).
ALTER TABLE foo.t SET IDENTIFIER FIELDS id, data;

-- [drop_identifier_fields] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; DROP IDENTIFIER FIELDS.
ALTER TABLE foo.t DROP IDENTIFIER FIELDS id;
