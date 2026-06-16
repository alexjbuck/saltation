"""Unit tests for ``cambrian.config``."""

from __future__ import annotations

from pathlib import Path

import pytest

from cambrian.config import (
    CambrianConfig,
    DevConfig,
    EvolutionsConfig,
    load_config,
    redacted_dump,
)
from cambrian.errors import (
    ConfigNotFoundError,
    InvalidConfigError,
    MissingEnvVarError,
)


def _write(tmp_path: Path, contents: str, name: str = "cambrian.toml") -> Path:
    path = tmp_path / name
    path.write_text(contents)
    return path


# ---------------------------------------------------------------------------
# Happy path / defaults
# ---------------------------------------------------------------------------


def test_load_full_config(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://bucket/"

[evolutions]
dir = "./my-evos"
sidecar_namespace = "_my_ns"

[dev]
mode = "reset"
watch = false
debounce_ms = 1000
""",
    )
    cfg = load_config(path)
    assert isinstance(cfg, CambrianConfig)
    assert cfg.catalog.type == "rest"
    assert cfg.catalog.uri == "http://localhost:8181"
    assert cfg.evolutions.dir == "./my-evos"
    assert cfg.evolutions.sidecar_namespace == "_my_ns"
    assert cfg.dev.mode == "reset"
    assert cfg.dev.watch is False
    assert cfg.dev.debounce_ms == 1000


def test_load_minimal_config_uses_defaults(tmp_path: Path) -> None:
    """Only [catalog] required; [evolutions] and [dev] take defaults."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
""",
    )
    cfg = load_config(path)
    # Evolutions defaults
    assert cfg.evolutions == EvolutionsConfig()
    assert cfg.evolutions.dir == "./evolutions"
    assert cfg.evolutions.sidecar_namespace == "_cambrian"
    assert cfg.evolutions.sidecar_catalog is None
    # Dev defaults
    assert cfg.dev == DevConfig()
    assert cfg.dev.mode == "idempotent"
    assert cfg.dev.watch is True
    assert cfg.dev.debounce_ms == 500


def test_evolutions_partial_overrides(tmp_path: Path) -> None:
    """Only some [evolutions] fields supplied; the rest fall back to defaults."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"

[evolutions]
dir = "./scripts"
""",
    )
    cfg = load_config(path)
    assert cfg.evolutions.dir == "./scripts"
    # Defaults preserved
    assert cfg.evolutions.sidecar_namespace == "_cambrian"


def test_sidecar_table_removed_from_schema(tmp_path: Path) -> None:
    """The legacy ``sidecar_table`` field was removed in M3; it's now rejected as unknown."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"

[evolutions]
sidecar_table = "evolution_state"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    assert "sidecar_table" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Missing file / required fields
# ---------------------------------------------------------------------------


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigNotFoundError, match="not found"):
        load_config(tmp_path / "nope.toml")


def test_missing_catalog_table_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[evolutions]
dir = "./e"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    assert "catalog" in str(excinfo.value).lower()


def test_missing_catalog_uri_raises_with_field_name(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    assert "uri" in str(excinfo.value)


def test_missing_catalog_type_raises_with_field_name(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
uri = "http://localhost:8181"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    assert "type" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Strict root / typo detection
# ---------------------------------------------------------------------------


def test_unknown_top_level_table_raises(tmp_path: Path) -> None:
    """Typo `[evolution]` (singular) should be rejected with a helpful list."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"

[evolution]
dir = "./e"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    message = str(excinfo.value)
    assert "evolution" in message
    # Valid top-level table names are listed in the error.
    for name in ("catalog", "evolutions", "dev"):
        assert name in message


def test_malformed_toml_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "this is not = valid toml [", name="bad.toml")
    with pytest.raises(InvalidConfigError, match="Failed to parse TOML"):
        load_config(path)


# ---------------------------------------------------------------------------
# Passthrough
# ---------------------------------------------------------------------------


def test_catalog_passthrough_keys(tmp_path: Path) -> None:
    """Arbitrary [catalog] keys flow through model_dump() unchanged."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "s3://bucket/"
token = "abc"
"some.dotted.key" = "value"
""",
    )
    cfg = load_config(path)
    dumped = cfg.catalog.model_dump()
    assert dumped["type"] == "rest"
    assert dumped["uri"] == "http://localhost:8181"
    assert dumped["warehouse"] == "s3://bucket/"
    assert dumped["token"] == "abc"
    assert dumped["some.dotted.key"] == "value"


# ---------------------------------------------------------------------------
# Env interpolation
# ---------------------------------------------------------------------------


def test_env_interpolation_substitutes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CAMBRIAN_CATALOG_URI", "http://from-env:9000")
    monkeypatch.setenv("CAMBRIAN_TOKEN", "shh")
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "${CAMBRIAN_CATALOG_URI}"
token = "prefix-${CAMBRIAN_TOKEN}-suffix"
""",
    )
    cfg = load_config(path)
    assert cfg.catalog.uri == "http://from-env:9000"
    assert cfg.catalog.model_dump()["token"] == "prefix-shh-suffix"


def test_env_interpolation_missing_raises_listing_all_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CAMBRIAN_MISSING_A", raising=False)
    monkeypatch.delenv("CAMBRIAN_MISSING_B", raising=False)
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "${CAMBRIAN_MISSING_A}"
token = "${CAMBRIAN_MISSING_B}"
""",
    )
    with pytest.raises(MissingEnvVarError) as excinfo:
        load_config(path)
    message = str(excinfo.value)
    assert "CAMBRIAN_MISSING_A" in message
    assert "CAMBRIAN_MISSING_B" in message


def test_env_interpolation_missing_var_names_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The error message must name BOTH the unset var and the field it was used in."""
    monkeypatch.delenv("CAMBRIAN_CATALOG_TOKEN", raising=False)
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
token = "${CAMBRIAN_CATALOG_TOKEN}"
""",
    )
    with pytest.raises(MissingEnvVarError) as excinfo:
        load_config(path)
    message = str(excinfo.value)
    assert "CAMBRIAN_CATALOG_TOKEN" in message
    assert "catalog.token" in message


def test_env_interpolation_literal_escape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``$${VAR}`` is rewritten to the literal ``${VAR}`` without interpolation."""
    monkeypatch.delenv("CAMBRIAN_NOT_SET", raising=False)
    monkeypatch.setenv("CAMBRIAN_TOKEN", "real")
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
warehouse = "literal-$${CAMBRIAN_NOT_SET}-tail"
token = "${CAMBRIAN_TOKEN}"
""",
    )
    cfg = load_config(path)
    dumped = cfg.catalog.model_dump()
    assert dumped["warehouse"] == "literal-${CAMBRIAN_NOT_SET}-tail"
    assert dumped["token"] == "real"


def test_env_interpolation_only_strings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-string TOML scalars are passed through untouched."""
    monkeypatch.setenv("CAMBRIAN_DIR", "./from-env-dir")
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"

[evolutions]
dir = "${CAMBRIAN_DIR}"

[dev]
debounce_ms = 250
watch = false
""",
    )
    cfg = load_config(path)
    assert cfg.evolutions.dir == "./from-env-dir"
    assert cfg.dev.debounce_ms == 250
    assert cfg.dev.watch is False


# ---------------------------------------------------------------------------
# Dev validation
# ---------------------------------------------------------------------------


def test_dev_mode_invalid_value_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"

[dev]
mode = "yolo"
""",
    )
    with pytest.raises(InvalidConfigError) as excinfo:
        load_config(path)
    assert "mode" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        "token",
        "auth_token",
        "secret",
        "client_secret",
        "password",
        "credential",
        "credentials",
        "api_key",
        "access_key",
        "private_key",
    ],
)
def test_redaction_masks_credential_keys(tmp_path: Path, key: str) -> None:
    path = _write(
        tmp_path,
        f"""
[catalog]
type = "rest"
uri = "http://localhost:8181"
{key} = "sensitive-value"
""",
    )
    cfg = load_config(path)
    dumped = redacted_dump(cfg)
    assert dumped["catalog"][key] == "***"


@pytest.mark.parametrize(
    "key,value",
    [
        ("warehouse", "s3://bucket/"),
        ("warehouse_key", "warehouse-id-123"),
        ("schema_key", "users"),
        ("region", "us-east-1"),
        ("table_name", "events"),
    ],
)
def test_redaction_does_not_mask_innocuous_keys(tmp_path: Path, key: str, value: str) -> None:
    path = _write(
        tmp_path,
        f"""
[catalog]
type = "rest"
uri = "http://localhost:8181"
{key} = "{value}"
""",
    )
    cfg = load_config(path)
    dumped = redacted_dump(cfg)
    assert dumped["catalog"][key] == value
    # And sanity-check that uri itself is never masked.
    assert dumped["catalog"]["uri"] == "http://localhost:8181"


def test_redaction_is_recursive(tmp_path: Path) -> None:
    """Nested dicts (e.g. sidecar_catalog override) are also redacted."""
    path = _write(
        tmp_path,
        """
[catalog]
type = "rest"
uri = "http://localhost:8181"
token = "outer-secret"

[evolutions.sidecar_catalog]
type = "rest"
uri = "http://sidecar:8181"
token = "nested-secret"
""",
    )
    cfg = load_config(path)
    dumped = redacted_dump(cfg)
    assert dumped["catalog"]["token"] == "***"
    assert dumped["evolutions"]["sidecar_catalog"]["token"] == "***"
    # Non-credential fields preserved at the nested level too.
    assert dumped["evolutions"]["sidecar_catalog"]["uri"] == "http://sidecar:8181"
