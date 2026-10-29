---
default: patch
---

# View bodies in another dialect keep their own string rules

With `[views].source_dialect` set (for example to `trino`), saltation split scripts into statements using Spark's string rules, where a backslash escapes the next character. A Trino view body such as `REPLACE(path, '\', '/')` then failed to parse, or could be split in the wrong place. The view body is now read with its own dialect's quoting rules, and a script saltation can't tokenize at all is reported as a dispatch error instead of a raw tokenizer exception.
