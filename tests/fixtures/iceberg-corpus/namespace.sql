-- Iceberg SQL corpus: namespace
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [ns_create] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; stock Spark parses to Create kind=NAMESPACE.
CREATE NAMESPACE foo;

-- [ns_create_dotted] expected=apply :: Multi-part namespace.
CREATE NAMESPACE foo.bar;

-- [ns_create_if_not_exists] expected=apply :: IF NOT EXISTS variant; exists=True on Create.
CREATE NAMESPACE IF NOT EXISTS foo;

-- [ns_create_with_properties] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; CREATE NAMESPACE ... WITH PROPERTIES is valid Spark.
CREATE NAMESPACE foo WITH PROPERTIES ('owner' = 'eng');

-- [ns_drop] expected=apply :: Drop kind=NAMESPACE.
DROP NAMESPACE foo;

-- [ns_drop_if_exists] expected=apply :: IF EXISTS guards the not-found path.
DROP NAMESPACE IF EXISTS foo;

-- [ns_alter_set_properties] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; valid Iceberg-Spark namespace property update.
ALTER NAMESPACE foo SET PROPERTIES ('owner' = 'data');
