"""Full lakehouse demo using PyIceberg patterns (no JVM required).

Demonstrates a complete lakehouse workflow:
1. Catalog setup and namespace creation
2. Table definition with partitioning
3. Data ingestion via snapshots
4. Schema evolution (add column, rename)
5. Time travel to a prior snapshot
"""

from __future__ import annotations

from datetime import UTC, datetime

from patterns.catalog import CatalogConfig, CatalogType, IcebergCatalog, TableIdentifier
from patterns.partitioning import PartitionField, PartitionSpec, PartitionTransform, SortField, SortOrder
from patterns.schema_evolution import ChangeType, ColumnChange, IcebergColumn, IcebergType, SchemaEvolution
from patterns.snapshots import DataOperation, RefType, Snapshot, SnapshotLog, SnapshotRef

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_COUNTER = 0


def _next_snapshot_id() -> int:
    """Return a monotonically increasing snapshot ID."""
    global _SNAPSHOT_COUNTER  # noqa: PLW0603
    _SNAPSHOT_COUNTER += 1
    return _SNAPSHOT_COUNTER


def _separator(title: str) -> None:
    """Print a visual separator with a section title."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Step 1 — Catalog & namespace setup
# ---------------------------------------------------------------------------


def setup_catalog() -> IcebergCatalog:
    """Create an in-memory catalog and register namespaces."""
    _separator("STEP 1 — Catalog Setup")

    config = CatalogConfig(
        name="demo_catalog",
        catalog_type=CatalogType.IN_MEMORY,
        warehouse="s3://lakehouse-demo/warehouse",
        properties={"io-impl": "org.apache.iceberg.aws.s3.S3FileIO"},
    )
    catalog = IcebergCatalog(config)

    for ns in ("raw", "staging", "analytics"):
        catalog.create_namespace(ns)

    print(f"Catalog type  : {config.catalog_type.value}")
    print(f"Warehouse     : {config.warehouse}")
    print(f"Is cloud      : {config.is_cloud()}")
    print(f"Namespaces    : {catalog.list_namespaces()}")
    return catalog


# ---------------------------------------------------------------------------
# Step 2 — Table creation with partitioning
# ---------------------------------------------------------------------------


def create_orders_table(catalog: IcebergCatalog) -> tuple[TableIdentifier, PartitionSpec, SortOrder]:
    """Register the orders table with date + category partitioning."""
    _separator("STEP 2 — Table & Partitioning")

    identifier = TableIdentifier.from_string("analytics.orders")

    # Partition: day(order_date) + identity(category)
    spec = PartitionSpec(spec_id=1)
    spec.add_field(PartitionField(source_column="order_date", transform=PartitionTransform.DAY))
    spec.add_field(PartitionField(source_column="category", transform=PartitionTransform.IDENTITY))

    # Sort: order_date ASC, total_amount DESC
    sort = SortOrder(order_id=1)
    sort.add_field(SortField(source_column="order_date"))
    sort.add_field(SortField(source_column="total_amount"))

    catalog.create_table(
        identifier,
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": "134217728",
        },
    )

    print(f"Table         : {identifier}")
    print(f"Partition cols: {spec.partition_columns()}")
    print(f"Has time part : {spec.has_time_partition()}")
    print(f"Sort order    : {sort.to_sql()}")
    print(f"Spec dict     : {spec.to_dict()}")

    return identifier, spec, sort


# ---------------------------------------------------------------------------
# Step 3 — Initial schema + snapshot ingestion
# ---------------------------------------------------------------------------


def ingest_initial_data(catalog: IcebergCatalog, identifier: TableIdentifier) -> tuple[SchemaEvolution, SnapshotLog]:
    """Define v1 schema and simulate two append snapshots."""
    _separator("STEP 3 — Schema v1 + Data Ingestion")

    # Schema v1
    schema = SchemaEvolution(schema_id=1)
    for col in [
        IcebergColumn(field_id=1, name="order_id", col_type=IcebergType.LONG, required=True, doc="PK"),
        IcebergColumn(field_id=2, name="order_date", col_type=IcebergType.DATE, required=True),
        IcebergColumn(field_id=3, name="customer_id", col_type=IcebergType.LONG, required=True),
        IcebergColumn(field_id=4, name="category", col_type=IcebergType.STRING),
        IcebergColumn(field_id=5, name="total_amount", col_type=IcebergType.DOUBLE, required=True),
        IcebergColumn(field_id=6, name="status", col_type=IcebergType.STRING),
    ]:
        schema.add_column(col)

    print(f"Schema v1 columns: {schema.column_names()}")

    # Simulate batch 1 — 2024-01 orders
    log = SnapshotLog()
    snap1 = Snapshot(
        snapshot_id=_next_snapshot_id(),
        operation=DataOperation.APPEND,
        summary={"source": "batch-2024-01", "engine": "pyiceberg"},
        added_files=12,
        added_rows=45_000,
    )
    log.append_snapshot(snap1)
    print(f"Snapshot {snap1.snapshot_id}: +{snap1.added_rows:,} rows (batch-2024-01)")

    # Tag the initial load
    log.add_ref(SnapshotRef(name="initial_load", ref_type=RefType.TAG, snapshot_id=snap1.snapshot_id))

    # Simulate batch 2 — 2024-02 orders
    snap2 = Snapshot(
        snapshot_id=_next_snapshot_id(),
        operation=DataOperation.APPEND,
        parent_snapshot_id=snap1.snapshot_id,
        summary={"source": "batch-2024-02", "engine": "pyiceberg"},
        added_files=14,
        added_rows=51_000,
    )
    log.append_snapshot(snap2)
    print(f"Snapshot {snap2.snapshot_id}: +{snap2.added_rows:,} rows (batch-2024-02)")

    current = log.current_snapshot()
    assert current is not None
    print(f"Current snapshot  : {current.snapshot_id}")
    print(f"Total rows ingested: {log.total_added_rows():,}")
    print(f"Operations so far : {log.operations_summary()}")

    return schema, log


# ---------------------------------------------------------------------------
# Step 4 — Schema evolution
# ---------------------------------------------------------------------------


def evolve_schema(schema: SchemaEvolution, log: SnapshotLog) -> None:
    """Add a discount_pct column and rename status → order_status."""
    _separator("STEP 4 — Schema Evolution")

    before = schema.column_names()[:]

    # Add new optional column
    add_change = ColumnChange(
        change_type=ChangeType.ADD_COLUMN,
        column_name="",
        new_column=IcebergColumn(
            field_id=0,
            name="discount_pct",
            col_type=IcebergType.FLOAT,
            doc="Discount percentage applied to order",
        ),
    )
    schema.apply_change(add_change)
    print(f"ADD   : {add_change.description()}")

    # Rename status → order_status
    rename_change = ColumnChange(
        change_type=ChangeType.RENAME_COLUMN,
        column_name="status",
        new_name="order_status",
    )
    schema.apply_change(rename_change)
    print(f"RENAME: {rename_change.description()}")

    print(f"Schema before : {before}")
    print(f"Schema after  : {schema.column_names()}")
    print(f"Total changes : {schema.change_count()}")
    print(f"Unsafe changes: {len(schema.unsafe_changes())}")

    # Record a schema-change snapshot (overwrite meta only)
    snap_evolve = Snapshot(
        snapshot_id=_next_snapshot_id(),
        operation=DataOperation.OVERWRITE,
        parent_snapshot_id=log.current_snapshot().snapshot_id,  # type: ignore[union-attr]
        summary={"schema-change": "add discount_pct, rename status"},
    )
    log.append_snapshot(snap_evolve)
    print(f"Schema snapshot: {snap_evolve.snapshot_id} ({snap_evolve.operation.value})")


# ---------------------------------------------------------------------------
# Step 5 — Time travel
# ---------------------------------------------------------------------------


def time_travel(log: SnapshotLog) -> None:
    """Roll back to the initial_load tag and read that snapshot."""
    _separator("STEP 5 — Time Travel")

    ref = log.get_ref("initial_load")
    if ref is None:
        print("Ref 'initial_load' not found — skipping time travel.")
        return

    print(f"Tag 'initial_load' → snapshot_id={ref.snapshot_id}")

    historic = log.get_snapshot(ref.snapshot_id)
    if historic is None:
        print("Snapshot not found.")
        return

    print(f"Historic timestamp : {historic.timestamp.isoformat()}")
    print(f"Historic added rows: {historic.added_rows:,}")
    print(f"Is root snapshot   : {historic.is_root()}")

    # Rollback (in real PyIceberg: catalog.load_table().manage_snapshots().rollback_to(id).commit())
    ok = log.rollback_to(ref.snapshot_id)
    print(f"Rollback success   : {ok}")
    current = log.current_snapshot()
    assert current is not None
    print(f"Now reading from snapshot: {current.snapshot_id} ({current.operation.value})")

    # Restore to latest
    latest_id = log.history()[-1].snapshot_id
    log.rollback_to(latest_id)
    print(f"Restored to latest snapshot: {latest_id}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_demo() -> None:
    """Execute the full lakehouse demo end-to-end."""
    print("\nApache Iceberg Lakehouse Demo (PyIceberg, stdlib-only)")
    print(f"Started at: {datetime.now(UTC).isoformat()}")

    catalog = setup_catalog()
    identifier, spec, sort = create_orders_table(catalog)
    schema, log = ingest_initial_data(catalog, identifier)
    evolve_schema(schema, log)
    time_travel(log)

    _separator("SUMMARY")
    print(f"Catalog tables  : {catalog.list_tables()}")
    print(f"Total snapshots : {log.snapshot_count()}")
    print(f"Final columns   : {schema.column_names()}")
    print(f"Schema dict     : {schema.to_dict()}")


if __name__ == "__main__":
    run_demo()
