---
default: patch
---

# Malformed input fails with saltation errors, not tracebacks

Malformed evolutions, includes and config now fail with saltation's own errors instead of tracebacks, so `apply`, `commit` and `watch` report them like any other error and `watch` keeps running.
