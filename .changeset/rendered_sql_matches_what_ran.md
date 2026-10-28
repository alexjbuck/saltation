---
default: patch
---

# Recorded SQL matches the statement that ran

Every cambrian extension (partition fields, write order and distribution, identifier fields, UNSET TBLPROPERTIES, ALTER COLUMN position, namespace properties) now renders back to SQL, so events and `--json` output show the real statement. Fixed renderings that lost or changed meaning: `ALTER COLUMN ... TYPE ... COMMENT ...` dropped the type, `TIME` columns rendered as `TIMESTAMP`, `truncate(W, col)` lost its column, and `CREATE NAMESPACE ... WITH PROPERTIES` rendered unparseable SQL.
