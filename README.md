# iceberg-python-lakehouse-patterns

Apache Iceberg Python lakehouse patterns: catalog management, schema evolution, partitioning strategies, and snapshot time-travel.

## Patterns

- **catalog** — `IcebergCatalog`, `TableIdentifier`, `CatalogConfig` (REST/Glue/Hive/Nessie)
- **partitioning** — `PartitionSpec`, `PartitionField` with all transforms, `SortOrder`
- **schema_evolution** — `SchemaEvolution`, `ColumnChange`, `IcebergType` promotions
- **snapshots** — `SnapshotLog`, time-travel, expiry, branch/tag refs

## Install

```bash
pip install -e ".[dev]"
pytest
```
