"""The catalog under test, selected by ``CAMBRIAN_TEST_CATALOG`` (default ``lakekeeper``).

Each rig is a docker-compose stack (catalog + rustfs + whatever the catalog
needs) with a one-shot ``bootstrap`` service, plus the PyIceberg properties
the host-side test process uses to reach it. Every integration test builds
its catalogs and configs from here so the suite runs unchanged per catalog.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyiceberg.catalog.rest import RestCatalog

from cambrian.config import CatalogConfig

__all__ = ["RIG", "Rig", "catalog_config", "catalog_properties", "catalog_toml", "rest_catalog"]

_DOCKER = Path(__file__).resolve().parents[2] / "docker"

# rustfs as the host sees it; every rig maps it to the same port.
_S3 = {
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "cambrian-access-key",
    "s3.secret-access-key": "cambrian-secret-key",
    "s3.region": "local",
    "s3.path-style-access": "true",
}


@dataclass(frozen=True)
class Rig:
    name: str
    compose_file: Path
    health_url: str
    # Where the catalog lets a client put an explicit table location.
    location_root: str
    properties: dict[str, Any] = field(default_factory=dict)


RIGS = {
    "lakekeeper": Rig(
        name="lakekeeper",
        compose_file=_DOCKER / "compose.yml",
        health_url="http://localhost:8181/health",
        location_root="s3://cambrian-warehouse/warehouse",
        properties={"uri": "http://localhost:8181/catalog", "warehouse": "cambrian", **_S3},
    ),
    "gravitino": Rig(
        name="gravitino",
        compose_file=_DOCKER / "gravitino" / "compose.yml",
        health_url="http://localhost:8181/iceberg/v1/config",
        location_root="s3://cambrian-warehouse/warehouse",
        # PyIceberg 0.12 sends "Authorization: Bearer None" without an auth
        # config; Gravitino rejects it where Lakekeeper ignores it.
        properties={"uri": "http://localhost:8181/iceberg", "auth": {"type": "noop"}, **_S3},
    ),
    "polaris": Rig(
        name="polaris",
        compose_file=_DOCKER / "polaris" / "compose.yml",
        health_url="http://localhost:8182/q/health",
        location_root="s3://cambrian-warehouse/polaris",
        properties={
            "uri": "http://localhost:8181/api/catalog",
            "warehouse": "cambrian",
            "credential": "root:cambrian-polaris-secret",
            "scope": "PRINCIPAL_ROLE:ALL",
            # The rig has no STS to vend credentials from; PyIceberg asks for
            # them by default and Polaris refuses the table load.
            "header.X-Iceberg-Access-Delegation": "",
            **_S3,
        },
    ),
}

RIG = RIGS[os.environ.get("CAMBRIAN_TEST_CATALOG", "lakekeeper")]


def catalog_properties() -> dict[str, Any]:
    return dict(RIG.properties)


def rest_catalog(name: str = "cambrian-test") -> RestCatalog:
    return RestCatalog(name=name, **catalog_properties())


def catalog_config() -> CatalogConfig:
    return CatalogConfig(type="rest", **catalog_properties())


def catalog_toml() -> str:
    """The ``[catalog]`` table of a cambrian.toml pointing at the rig."""
    lines = ["[catalog]", 'type = "rest"']
    lines += [f"{json.dumps(k)} = {_toml_value(v)}" for k, v in catalog_properties().items()]
    return "\n".join(lines) + "\n"


def _toml_value(value: Any) -> str:
    if isinstance(value, dict):
        pairs = ", ".join(f"{json.dumps(k)} = {_toml_value(v)}" for k, v in value.items())
        return "{ " + pairs + " }"
    return json.dumps(value)
