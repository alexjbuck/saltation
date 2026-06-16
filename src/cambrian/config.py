"""Configuration models and TOML loading for cambrian.

The on-disk schema is ``cambrian.toml`` with four top-level tables:

- ``[catalog]``: PyIceberg catalog kwargs (passed straight through to
  :func:`pyiceberg.catalog.load_catalog`). ``type`` and ``uri`` are required;
  everything else is open-ended so we don't have to chase every future
  catalog flavor.
- ``[evolutions]``: location of the on-disk evolution scripts and the names
  of the sidecar namespace + table.
- ``[evolutions.sidecar_catalog]`` (optional): override the catalog used to
  store sidecar tables, otherwise the main ``[catalog]`` is reused.
- ``[dev]``: developer-loop knobs (mode/watch/debounce_ms), optional.

String values in TOML support ``${VAR}`` substitution against the process
environment at load time. This is intentionally restricted to strings (TOML
ints/bools are passed through as-is). Missing env vars raise
:class:`MissingEnvVarError` listing every unset name in one shot so CI runs
can see all that's broken in a single failure. To emit a literal ``${VAR}``
without interpolating, double the dollar: ``$${VAR}`` is rewritten to
``${VAR}`` after substitution (the standard make-style escape).
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from cambrian.errors import (
    ConfigNotFoundError,
    InvalidConfigError,
    MissingEnvVarError,
)

__all__ = [
    "CambrianConfig",
    "CatalogConfig",
    "DevConfig",
    "EvolutionsConfig",
    "load_config",
    "redacted_dump",
]


# ---------------------------------------------------------------------------
# Env interpolation
# ---------------------------------------------------------------------------

# Two alternatives matched in one pass so a literal ``$${VAR}`` is consumed
# as a unit and the leading ``$`` of ``${VAR}`` next to it can't be mistaken
# for an escape. Without the single-pass form, ``$${VAR}`` would
# first-pass-substitute the inner ``${VAR}``, defeating the escape.
_ENV_VAR_RE = re.compile(r"\$\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate_env(
    value: Any,
    missing: list[tuple[str, str]],
    path: tuple[str, ...] = (),
) -> Any:
    """Recursively substitute ``${VAR}`` references inside *value*.

    Strings are scanned; unset env vars are appended to *missing* as
    ``(varname, dotted-field-path)`` so the caller can report every unset
    name + the field it was referenced from. Non-string leaves are returned
    untouched. Lists and dicts are walked. A literal ``$${VAR}`` is rewritten
    to ``${VAR}`` without consulting the environment (make-style escape).
    """
    if isinstance(value, str):
        field_path = ".".join(path) or "<root>"

        def _sub(match: re.Match[str]) -> str:
            escaped, name = match.group(1), match.group(2)
            if escaped is not None:
                return f"${{{escaped}}}"
            assert name is not None
            env_value = os.environ.get(name)
            if env_value is None:
                missing.append((name, field_path))
                return match.group(0)
            return env_value

        return _ENV_VAR_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v, missing, (*path, str(k))) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(item, missing, (*path, f"[{i}]")) for i, item in enumerate(value)]
    return value


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class CatalogConfig(BaseModel):
    """PyIceberg catalog configuration.

    Fields besides ``type`` and ``uri`` pass through to
    :func:`pyiceberg.catalog.load_catalog` unchanged. We deliberately allow
    extras so each pyiceberg backend (REST/Glue/Hive/SQL/…) can supply its
    own kwargs without us mirroring them.
    """

    model_config = ConfigDict(extra="allow")

    type: str = Field(description="PyIceberg catalog backend, e.g. 'rest', 'sql', 'glue'.")
    uri: str = Field(description="Catalog endpoint URI.")


class _SidecarCatalogOverride(BaseModel):
    """Optional override catalog used for the sidecar tables only."""

    model_config = ConfigDict(extra="allow")

    type: str
    uri: str


class EvolutionsConfig(BaseModel):
    """Where evolutions live on disk and how the sidecar is named.

    The sidecar's table names (``events``, ``table_states``, ``version``) are
    fixed internal constants; only the *namespace* is user-configurable.
    """

    model_config = ConfigDict(extra="forbid")

    dir: str = Field(default="./evolutions", description="Directory holding .sql evolutions.")
    sidecar_namespace: str = Field(
        default="_cambrian",
        description="Namespace that holds the cambrian sidecar tables.",
    )
    sidecar_catalog: _SidecarCatalogOverride | None = Field(
        default=None,
        description="Optional override catalog for the sidecar (otherwise uses [catalog]).",
    )


class DevConfig(BaseModel):
    """Developer-loop settings used by ``cambrian dev``."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["idempotent", "reset"] = "idempotent"
    watch: bool = True
    debounce_ms: int = Field(default=500, ge=0)


class CambrianConfig(BaseModel):
    """Top-level cambrian config.

    ``extra="forbid"`` is intentional at the root so a typo like
    ``[evolution]`` (singular) fails loudly instead of being silently ignored.
    """

    model_config = ConfigDict(extra="forbid")

    catalog: CatalogConfig
    evolutions: EvolutionsConfig = Field(default_factory=EvolutionsConfig)
    dev: DevConfig = Field(default_factory=DevConfig)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_config(path: Path) -> CambrianConfig:
    """Load and validate a cambrian TOML config file.

    Performs, in order: file existence check, TOML parse, env interpolation,
    pydantic validation.

    Raises:
        ConfigNotFoundError: *path* does not exist.
        MissingEnvVarError: One or more ``${VAR}`` references are unset.
        InvalidConfigError: TOML is malformed or fails schema validation.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigNotFoundError(f"Config file not found: {path}")

    try:
        with path.open("rb") as fp:
            raw = tomllib.load(fp)
    except tomllib.TOMLDecodeError as exc:
        raise InvalidConfigError(f"Failed to parse TOML at {path}: {exc}") from exc

    missing: list[tuple[str, str]] = []
    interpolated = _interpolate_env(raw, missing)
    if missing:
        # Dedupe on (var, field) so a single reference doesn't print twice if
        # somehow it was visited more than once, but two distinct refs to the
        # same var from different fields are surfaced separately.
        unique = sorted(set(missing))
        joined = "; ".join(f"${{{var}}} (referenced from {field})" for var, field in unique)
        raise MissingEnvVarError(
            f"Config at {path} references unset environment variable(s): {joined}"
        )

    try:
        return CambrianConfig.model_validate(interpolated)
    except ValidationError as exc:
        raise InvalidConfigError(_format_validation_error(path, exc)) from exc


def _format_validation_error(path: Path, exc: ValidationError) -> str:
    """Turn a pydantic ValidationError into a multi-line, user-readable string."""
    lines = [f"Invalid config at {path}:"]
    valid_top_level = sorted(CambrianConfig.model_fields.keys())
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"]) or "<root>"
        msg = err["msg"]
        if err["type"] == "extra_forbidden" and len(err["loc"]) == 1:
            lines.append(
                f"  - unknown table [{loc}] (valid top-level tables: {', '.join(valid_top_level)})"
            )
        else:
            lines.append(f"  - {loc}: {msg}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------

# Substrings that strongly indicate a credential-shaped key.
_CREDENTIAL_SUBSTRINGS = ("token", "secret", "password", "credential")

# Explicit "key"-suffix patterns we treat as credentials. The plain substring
# "key" is too aggressive (warehouse_key, schema_key, partition_key, ...).
_KEY_SUFFIX_PATTERNS = (
    "api_key",
    "access_key",
    "secret_key",
    "private_key",
    "signing_key",
    "encryption_key",
    "client_key",
)

# Allowlist of keys whose lowercased name looks credential-ish but isn't.
_REDACTION_ALLOWLIST = frozenset({"warehouse_key", "schema_key"})

_REDACTED = "***"


def _is_credential_key(key: str) -> bool:
    lowered = key.lower()
    if lowered in _REDACTION_ALLOWLIST:
        return False
    if any(sub in lowered for sub in _CREDENTIAL_SUBSTRINGS):
        return True
    return any(pattern in lowered for pattern in _KEY_SUFFIX_PATTERNS)


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: (_REDACTED if _is_credential_key(str(k)) else _redact(v)) for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def redacted_dump(cfg: CambrianConfig) -> dict[str, Any]:
    """Return ``cfg.model_dump()`` with credential-shaped values masked as ``"***"``.

    Heuristic: any dict key whose lowercased name contains "token", "secret",
    "password", or "credential", *or* matches a known ``*_key`` credential
    pattern (api_key, access_key, secret_key, private_key, signing_key,
    encryption_key, client_key), has its value replaced. The substrings
    "warehouse_key" and "schema_key" are explicitly allowlisted so legitimate
    schema/table field names aren't mistakenly masked. The redaction is
    applied recursively so nested dicts (e.g. extras under ``[catalog]``)
    are covered too.
    """
    return _redact(cfg.model_dump())
