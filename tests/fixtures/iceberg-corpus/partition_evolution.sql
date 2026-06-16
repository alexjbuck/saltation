-- Iceberg SQL corpus: partition_evolution
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [apf_identity] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; bare-column identity ADD PARTITION FIELD.
ALTER TABLE foo.t ADD PARTITION FIELD category;

-- [apf_bucket] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; bucket transform.
ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id);

-- [apf_truncate] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; truncate transform.
ALTER TABLE foo.t ADD PARTITION FIELD truncate(4, data);

-- [apf_year] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; year transform.
ALTER TABLE foo.t ADD PARTITION FIELD year(ts);

-- [apf_day] expected=apply :: day transform.
ALTER TABLE foo.t ADD PARTITION FIELD day(ts);

-- [apf_hour] expected=apply :: hour transform.
ALTER TABLE foo.t ADD PARTITION FIELD hour(ts);

-- [apf_alias] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; AS alias.
ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id) AS shard;

-- [dpf_identity] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; drop by name.
ALTER TABLE foo.t DROP PARTITION FIELD category;

-- [dpf_transform] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; drop by transform expr.
ALTER TABLE foo.t DROP PARTITION FIELD bucket(16, id);

-- [dpf_alias_name] expected=apply :: Drop by alias name.
ALTER TABLE foo.t DROP PARTITION FIELD shard;

-- [rpf_basic] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; replace partition field.
ALTER TABLE foo.t REPLACE PARTITION FIELD ts_day WITH day(ts);

-- [rpf_alias] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; replace with AS alias.
ALTER TABLE foo.t REPLACE PARTITION FIELD ts_day WITH day(ts) AS day_of_ts;
