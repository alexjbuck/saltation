"""Recursive resolution of ``--! include`` directives and hashing of expanded SQL.

The include directive shape is a single line comment::

    --! include <path-or-glob>

Resolution rules (per plan §2.4):

- Paths are resolved relative to the **including** file, not cwd. So
  ``current.sql`` saying ``--! include current/*.sql`` looks in
  ``<dir-of-current.sql>/current/`` regardless of where the user invoked cambrian.
- Globs are expanded via :meth:`Path.glob` and the matches are sorted
  lexicographically — deterministic across operating systems and matches the
  semantics graphile-migrate users expect.
- Circular includes are detected via a visited-set keyed on the resolved
  absolute path. A cycle raises :class:`CircularIncludeError` with the chain.
- A directive that resolves to **zero** files (missing literal, empty glob)
  raises :class:`IncludeNotFoundError`. We intentionally don't silently
  expand to nothing: graphile-migrate's behavior is to fail.
- The expanded text is bracketed by synthetic comment markers
  ``-- cambrian:include-begin <relpath>`` and ``-- cambrian:include-end <relpath>``
  so that a line in the expanded text can be mapped back to its source file.
  The markers are deterministic so they don't perturb the hash.
- The hash is ``sha256`` of the final expanded text (markers included). Used
  by the idempotent runner to short-circuit re-applies when nothing changed.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from cambrian.errors import CircularIncludeError, IncludeNotFoundError

__all__ = ["ExpandedSql", "expand"]


# A line of the form ``--! include <stuff>``. Leading whitespace tolerated so
# nested-include directives can be indented for readability. The captured group
# is everything after ``include `` up to the end of the line, lstripped/rstripped.
_INCLUDE_RE = re.compile(r"^\s*--!\s*include\s+(\S.*?)\s*$")

_BEGIN_MARKER = "-- cambrian:include-begin"
_END_MARKER = "-- cambrian:include-end"


@dataclass(frozen=True, slots=True)
class ExpandedSql:
    """Result of recursively expanding all ``--! include`` directives.

    ``text`` is the flattened SQL with synthetic markers around each included
    fragment. ``hash`` is sha256(text) hex. ``sources`` is every file involved
    (the root + every transitively-included file), absolute paths, in the
    order they were first visited.
    """

    text: str
    hash: str
    sources: list[Path]


def expand(sql_path: Path) -> ExpandedSql:
    """Expand all ``--! include`` directives in *sql_path* recursively.

    Returns the flattened text, its sha256 hex digest, and the list of every
    source file that contributed to the result (in first-visit order).

    Raises:
        IncludeNotFoundError: A directive resolves to no file.
        CircularIncludeError: An include cycle is detected.
    """
    root = sql_path.resolve()
    visited_stack: list[Path] = []
    sources: list[Path] = []
    text = _expand_one(root, visited_stack, sources)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return ExpandedSql(text=text, hash=digest, sources=sources)


def _expand_one(path: Path, visited_stack: list[Path], sources: list[Path]) -> str:
    """Read *path* and inline every ``--! include`` directive it contains.

    ``visited_stack`` is the current chain of currently-being-expanded files,
    used for cycle detection. ``sources`` accumulates every file we've ever
    visited, dedup'd, in first-visit order — fed back to the caller for
    watchfile registration in M6.
    """
    if path in visited_stack:
        cycle = [*visited_stack, path]
        raise CircularIncludeError(cycle)

    visited_stack.append(path)
    if path not in sources:
        sources.append(path)

    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as err:
        # Only reachable if the root file is missing; recursive includes go
        # through ``_resolve_directive`` which raises a richer error before
        # reaching this point.
        raise IncludeNotFoundError(directive=str(path), resolved=path, source=path) from err

    out_lines: list[str] = []
    for line in raw.splitlines():
        match = _INCLUDE_RE.match(line)
        if not match:
            out_lines.append(line)
            continue
        directive = match.group(1)
        resolved_paths = _resolve_directive(directive, including_file=path)
        for child in resolved_paths:
            rel = _relpath_for_marker(child, path)
            out_lines.append(f"{_BEGIN_MARKER} {rel}")
            out_lines.append(_expand_one(child, visited_stack, sources))
            out_lines.append(f"{_END_MARKER} {rel}")

    visited_stack.pop()

    # Preserve trailing newline behavior of the source. ``splitlines()`` drops
    # the trailing separator; we add one back so the output is well-formed if
    # the source ended in a newline.
    body = "\n".join(out_lines)
    if raw.endswith("\n") and not body.endswith("\n"):
        body += "\n"
    return body


def _resolve_directive(directive: str, *, including_file: Path) -> list[Path]:
    """Resolve a directive token (literal path or glob) relative to *including_file*.

    Returns a deterministic, lexicographically sorted list of absolute paths.
    Raises :class:`IncludeNotFoundError` if the directive matches no files.
    """
    base_dir = including_file.parent
    # ``Path.glob`` treats absolute patterns specially; resolve relative
    # patterns by anchoring at ``base_dir``.
    target = Path(directive)
    if target.is_absolute():
        # An absolute path with glob characters is ambiguous on Path.glob;
        # we split the anchor and walk from there.
        if any(ch in directive for ch in "*?["):
            # Honour absolute globs by anchoring at the filesystem root and
            # treating the remainder as the pattern. This is a niche case
            # but cheap to support.
            anchor = Path(target.anchor)
            pattern = str(target.relative_to(anchor))
            matches = sorted(anchor.glob(pattern))
        else:
            matches = [target] if target.exists() else []
    elif any(ch in directive for ch in "*?["):
        matches = sorted(base_dir.glob(directive))
    else:
        candidate = (base_dir / directive).resolve()
        matches = [candidate] if candidate.exists() else []

    if not matches:
        raise IncludeNotFoundError(
            directive=directive,
            resolved=(base_dir / directive).resolve(),
            source=including_file,
        )

    return [p.resolve() for p in matches]


def _relpath_for_marker(target: Path, base: Path) -> str:
    """Best-effort relative path for the synthetic include markers.

    Falls back to the absolute path if *target* isn't beneath *base.parent*
    (Python's :meth:`Path.relative_to` raises in that case). The marker is for
    human-readable error mapping, not for control flow, so a fallback is fine.
    """
    try:
        return str(target.relative_to(base.parent))
    except ValueError:
        return str(target)
