---
default: patch
---

# DROP and REPLACE PARTITION FIELD resolve the field like Spark

A transform call (`bucket(16, id)`) matches the field with that source column and transform, a bare column name means `identity(column)` whatever the field is called, and any other bare name is a partition field name. `REPLACE PARTITION FIELD` now removes and adds in one spec update and fails when the field to replace isn't in the spec, instead of adding the new field and reporting success.
