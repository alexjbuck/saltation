# cambrian CLI: JSON output schemas

Every mutating subcommand accepts `--json` and emits a single JSON document
on stdout. Errors go to stderr. The exit code is the source of truth for
success/failure; the JSON is for parsing the result.

The schemas below use a relaxed JSON-Schema notation: `string`, `int`,
`bool`, `list[T]`, `T | null` for optionality, `dict` for an unparsed
object.

Inside cambrian the canonical noun is **evolution** (matching Iceberg's
"schema evolution" / "partition evolution" vocabulary). The top-level
README uses "migration" framing for discoverability — the JSON field
names below all use `evolution_*`.

## Exit codes

| Code | Meaning                                            |
|------|----------------------------------------------------|
| 0    | success                                            |
| 1    | generic error (parse/dispatch failure, IO error)   |
| 2    | sidecar not initialised in the configured catalog  |
| 3    | sidecar is at a version newer than this binary    |
| 4    | external write detected (rollback / sync refused)  |

`cambrian init` and `cambrian status` are read-only; their exit codes
follow the same table.

## `cambrian apply [--json]`

```
{
  "mode":            "idempotent",
  "status":          "unchanged" | "applied" | "partial",
  "evolution_id":    "current",
  "evolution_hash":  string,           // sha256 of expanded current.sql
  "event_id":        string | null,    // null when no work was done
  "sources":         list[string],     // paths walked via --! include
  "applied_committed": list[{
    "evolution_id":   string,          // "NNNN_<slug>"
    "status":         "applied" | "unchanged" | "partial",
    "evolution_hash": string,
    "event_id":       string | null,
    "error":          string | null
  }],
  "statements": list[{
    "sql":             string,
    "notes":           string,
    "affected_tables": list[string],
    "error":           string | null
  }],
  "error": string | null
}
```

## `cambrian apply --reset [--json]` (alias: `cambrian redo`)

```
{
  "mode":              "reset",
  "status":            "applied" | "rolled-back" | "partial" | "no-change",
  "evolution_hash":    string | null,
  "rollback_event_id": string | null,
  "apply_event_id":    string | null,
  "sources":           list[string],
  "rollbacks": list[{
    "ident":             string,
    "rolled_back":       bool,
    "from_snapshot_id":  int | null,
    "to_snapshot_id":    int | null,
    "reason":            string
  }],
  "apply_result":      {<apply payload>} | null,
  "error":             string | null
}
```

`cambrian rollback --json` shares this schema.

## `cambrian commit -m <msg> [--json]`

```
{
  "evolution_id":     string,         // "NNNN_<slug>"
  "committed_path":   string,
  "evolution_hash":   string,
  "tag_ref":          string,         // "cambrian.committed.<n>.<slug>"
  "event_id":         string | null,
  "affected_tables":  list[string]
}
```

## `cambrian uncommit [--json]`

```
{
  "evolution_id":       string,
  "restored_path":      string,
  "restored_to_current": true,
  "event_id":           string | null,
  "rolled_back_tables": list[string],
  "skipped_tables":     list[string]
}
```

## `cambrian reset-to <evolution_id> [--json]`

```
{
  "mode":               "reset --to",
  "evolution_id":       string,
  "event_id":           string | null,
  "rolled_back_tables": list[string],
  "skipped_tables":     list[string]
}
```

## `cambrian sync [--json]` (alias: `cambrian download`)

```
{
  "dry_run":        bool,
  "written":        int,
  "overwritten":    int,
  "skipped":        int,
  "refused":        int,
  "discrepancies":  int,
  "files": list[{
    "evolution_id":  string,
    "path":          string,
    "status":        "written" | "overwritten" | "skipped" | "refused" | "discrepancy",
    "catalog_hash":  string | null,
    "local_hash":    string | null,
    "diff":          string | null,
    "note":          string | null
  }]
}
```

Sync emits exit code `4` when `refused > 0` so CI can fail loud on a
drifted checkout.

## `cambrian status [--json]` (read-only)

```
{
  "initialized":         bool,
  "sidecar_namespace":   string,
  "sidecar_version":     int,
  "is_version_ahead":    bool,
  "committed_count":     int,
  "committed_evolutions": list[{
    "evolution_id":  string,
    "event_id":      string,
    "event_ts":      string         // ISO-8601
  }],
  "current_applied": null | {
    "event_id":       string,
    "event_ts":       string,
    "evolution_hash": string
  }
}
```
