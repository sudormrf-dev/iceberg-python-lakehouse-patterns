"""Iceberg catalog patterns: REST, Glue, Hive, in-memory."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class CatalogType(str, Enum):
    REST = "rest"
    GLUE = "glue"
    HIVE = "hive"
    HADOOP = "hadoop"
    NESSIE = "nessie"
    IN_MEMORY = "in_memory"


@dataclass
class TableIdentifier:
    namespace: list[str]
    name: str

    def __str__(self) -> str:
        return ".".join([*self.namespace, self.name])

    def database(self) -> str:
        return self.namespace[-1] if self.namespace else ""

    @classmethod
    def from_string(cls, dotted: str) -> TableIdentifier:
        parts = dotted.split(".")
        return cls(namespace=parts[:-1], name=parts[-1])


@dataclass
class CatalogConfig:
    name: str
    catalog_type: CatalogType = CatalogType.REST
    uri: str = ""
    warehouse: str = ""
    credential: str = ""
    properties: dict[str, str] = field(default_factory=dict)

    def is_cloud(self) -> bool:
        return self.catalog_type in {CatalogType.GLUE, CatalogType.NESSIE}

    def requires_uri(self) -> bool:
        return self.catalog_type in {CatalogType.REST, CatalogType.HIVE, CatalogType.NESSIE}

    def to_pyiceberg_config(self) -> dict[str, str]:
        cfg: dict[str, str] = {"type": self.catalog_type.value}
        if self.uri:
            cfg["uri"] = self.uri
        if self.warehouse:
            cfg["warehouse"] = self.warehouse
        if self.credential:
            cfg["credential"] = self.credential
        cfg.update(self.properties)
        return cfg


class IcebergCatalog:
    """In-memory Iceberg catalog for testing and prototyping."""

    def __init__(self, config: CatalogConfig) -> None:
        self._config = config
        self._tables: dict[str, dict[str, str]] = {}
        self._namespaces: set[str] = set()

    @property
    def config(self) -> CatalogConfig:
        return self._config

    def create_namespace(self, namespace: str) -> None:
        self._namespaces.add(namespace)

    def namespace_exists(self, namespace: str) -> bool:
        return namespace in self._namespaces

    def list_namespaces(self) -> list[str]:
        return sorted(self._namespaces)

    def create_table(
        self, identifier: TableIdentifier, properties: dict[str, str] | None = None
    ) -> None:
        key = str(identifier)
        self._tables[key] = properties or {}
        if identifier.namespace:
            self._namespaces.add(".".join(identifier.namespace))

    def table_exists(self, identifier: TableIdentifier) -> bool:
        return str(identifier) in self._tables

    def drop_table(self, identifier: TableIdentifier) -> bool:
        key = str(identifier)
        if key in self._tables:
            del self._tables[key]
            return True
        return False

    def list_tables(self, namespace: str = "") -> list[str]:
        if namespace:
            return [k for k in self._tables if k.startswith(namespace + ".")]
        return list(self._tables.keys())

    def rename_table(self, src: TableIdentifier, dst: TableIdentifier) -> bool:
        src_key = str(src)
        if src_key not in self._tables:
            return False
        self._tables[str(dst)] = self._tables.pop(src_key)
        return True

    def table_properties(self, identifier: TableIdentifier) -> dict[str, str]:
        return dict(self._tables.get(str(identifier), {}))
