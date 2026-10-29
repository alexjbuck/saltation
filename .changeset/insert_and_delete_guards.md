---
default: patch
---

# INSERT into bucket/truncate-partitioned tables works; unsupported writes fail clearly

saltation now depends on `pyiceberg[pyiceberg-core]`, which PyIceberg needs to write to tables partitioned by `bucket` or `truncate`. INSERT and DELETE on a format v3 table, or on a table partitioned more than once on the same column, now fail with an explicit error instead of a raw PyIceberg exception (both are PyIceberg 0.12 limitations).
