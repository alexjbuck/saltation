---
default: patch
---

# Replacing a view with an earlier body works on Lakekeeper

Replacing a view back to a body it had before (A → B → A) failed on Lakekeeper with a 503. saltation now points the view at the matching earlier version instead of re-adding it, which works on every catalog. Replacing a view without changing its columns also works on Java-based catalogs (Polaris, Gravitino).
