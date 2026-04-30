"""Change Data Capture (CDC) pipeline using Iceberg snapshot patterns.

Demonstrates:
1. Initial full snapshot of source data
2. Detecting inserts / updates / deletes between two source states
3. Applying changes via merge-on-read (append delete + append new)
4. Periodic compaction by replacing fragmented files with a single snapshot
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

from patterns.schema_evolution import (
    ChangeType,
    ColumnChange,
    IcebergColumn,
    IcebergType,
    SchemaEvolution,
)
from patterns.snapshots import DataOperation, RefType, Snapshot, SnapshotLog, SnapshotRef

# ---------------------------------------------------------------------------
# CDC primitives
# ---------------------------------------------------------------------------


class CDCEventType(str, Enum):
    """Kind of change detected in the source system."""

    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


@dataclass
class CDCEvent:
    """A single row-level change event."""

    event_type: CDCEventType
    primary_key: int
    before: dict[str, object] | None  # None for INSERT
    after: dict[str, object] | None  # None for DELETE
    source_ts: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class CDCBatch:
    """Collection of CDC events in one micro-batch."""

    batch_id: int
    events: list[CDCEvent] = field(default_factory=list)
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def inserts(self) -> list[CDCEvent]:
        return [e for e in self.events if e.event_type == CDCEventType.INSERT]

    def updates(self) -> list[CDCEvent]:
        return [e for e in self.events if e.event_type == CDCEventType.UPDATE]

    def deletes(self) -> list[CDCEvent]:
        return [e for e in self.events if e.event_type == CDCEventType.DELETE]

    def summary(self) -> dict[str, int]:
        return {
            "inserts": len(self.inserts()),
            "updates": len(self.updates()),
            "deletes": len(self.deletes()),
            "total": len(self.events),
        }


# ---------------------------------------------------------------------------
# Step 1 — Initial full snapshot
# ---------------------------------------------------------------------------

_SNAPSHOT_SEQ = 100


def _next_id() -> int:
    global _SNAPSHOT_SEQ  # noqa: PLW0603
    _SNAPSHOT_SEQ += 1
    return _SNAPSHOT_SEQ


def initial_snapshot(
    source_rows: dict[int, dict[str, object]],
) -> tuple[SnapshotLog, SchemaEvolution]:
    """Load the full source table as snapshot 0 of the Iceberg table."""
    print("\n--- Step 1: Initial Full Snapshot ---")

    schema = SchemaEvolution(schema_id=1)
    for col in [
        IcebergColumn(field_id=1, name="id", col_type=IcebergType.LONG, required=True),
        IcebergColumn(field_id=2, name="name", col_type=IcebergType.STRING, required=True),
        IcebergColumn(field_id=3, name="email", col_type=IcebergType.STRING),
        IcebergColumn(field_id=4, name="score", col_type=IcebergType.DOUBLE),
        IcebergColumn(
            field_id=5, name="updated_at", col_type=IcebergType.TIMESTAMPTZ, required=True
        ),
    ]:
        schema.add_column(col)

    log = SnapshotLog()
    snap = Snapshot(
        snapshot_id=_next_id(),
        operation=DataOperation.OVERWRITE,
        summary={"source": "full-load", "rows": str(len(source_rows))},
        added_files=4,
        added_rows=len(source_rows),
    )
    log.append_snapshot(snap)
    log.add_ref(SnapshotRef(name="full_load", ref_type=RefType.TAG, snapshot_id=snap.snapshot_id))

    print(f"  Snapshot id    : {snap.snapshot_id}")
    print(f"  Rows loaded    : {snap.added_rows}")
    print(f"  Schema columns : {schema.column_names()}")
    return log, schema


# ---------------------------------------------------------------------------
# Step 2 — Detect changes between two source states
# ---------------------------------------------------------------------------


def detect_changes(
    old_state: dict[int, dict[str, object]],
    new_state: dict[int, dict[str, object]],
    batch_id: int,
) -> CDCBatch:
    """Compare two state snapshots and produce a CDCBatch."""
    print("\n--- Step 2: Detect Changes ---")

    batch = CDCBatch(batch_id=batch_id)
    all_keys = set(old_state) | set(new_state)

    for pk in sorted(all_keys):
        old_row = old_state.get(pk)
        new_row = new_state.get(pk)

        if old_row is None and new_row is not None:
            batch.events.append(CDCEvent(CDCEventType.INSERT, pk, before=None, after=new_row))
        elif old_row is not None and new_row is None:
            batch.events.append(CDCEvent(CDCEventType.DELETE, pk, before=old_row, after=None))
        elif old_row != new_row:
            batch.events.append(CDCEvent(CDCEventType.UPDATE, pk, before=old_row, after=new_row))

    s = batch.summary()
    print(
        f"  Batch {batch_id}: {s['inserts']} inserts, {s['updates']} updates, {s['deletes']} deletes"
    )
    return batch


# ---------------------------------------------------------------------------
# Step 3 — Apply via merge-on-read (equality deletes + appends)
# ---------------------------------------------------------------------------


def apply_merge_on_read(batch: CDCBatch, log: SnapshotLog, schema: SchemaEvolution) -> None:
    """Apply CDC batch using Iceberg v2 merge-on-read strategy.

    Equality deletes mark stale rows; new versions are appended.
    Readers reconstruct the latest view at query time.
    """
    print("\n--- Step 3: Apply via Merge-on-Read ---")

    deleted_keys = [e.primary_key for e in batch.deletes()] + [
        e.primary_key for e in batch.updates()
    ]
    new_rows = [e.after for e in batch.inserts()] + [e.after for e in batch.updates()]

    # Add _cdc_ts column if not present yet
    if schema.get_column("_cdc_ts") is None:
        schema.apply_change(
            ColumnChange(
                change_type=ChangeType.ADD_COLUMN,
                column_name="",
                new_column=IcebergColumn(
                    field_id=0,
                    name="_cdc_ts",
                    col_type=IcebergType.TIMESTAMPTZ,
                    doc="Timestamp when CDC event was applied",
                ),
            )
        )
        print("  Schema: added _cdc_ts column")

    parent_id = log.current_snapshot().snapshot_id  # type: ignore[union-attr]

    if deleted_keys:
        # Equality delete file (marks rows as deleted without rewriting)
        delete_snap = Snapshot(
            snapshot_id=_next_id(),
            operation=DataOperation.DELETE,
            parent_snapshot_id=parent_id,
            summary={
                "equality-delete-keys": str(deleted_keys),
                "delete-count": str(len(deleted_keys)),
            },
            deleted_files=0,
            deleted_rows=len(deleted_keys),
        )
        log.append_snapshot(delete_snap)
        print(f"  Delete snapshot {delete_snap.snapshot_id}: marked {len(deleted_keys)} rows")
        parent_id = delete_snap.snapshot_id

    if new_rows:
        append_snap = Snapshot(
            snapshot_id=_next_id(),
            operation=DataOperation.APPEND,
            parent_snapshot_id=parent_id,
            summary={
                "source": f"cdc-batch-{batch.batch_id}",
                "new-rows": str(len(new_rows)),
            },
            added_files=1,
            added_rows=len(new_rows),
        )
        log.append_snapshot(append_snap)
        print(f"  Append snapshot {append_snap.snapshot_id}: +{len(new_rows)} rows")

    current = log.current_snapshot()
    assert current is not None
    print(f"  Net rows this batch: {batch.summary()['inserts'] - len(batch.deletes())}")
    print(f"  Current snapshot  : {current.snapshot_id}")


# ---------------------------------------------------------------------------
# Step 4 — Compaction
# ---------------------------------------------------------------------------


def compact_table(log: SnapshotLog, min_files_threshold: int = 3) -> None:
    """Replace fragmented small files with a single optimised snapshot.

    In real PyIceberg this is done via:
    ``table.rewrite_data_files(RewriteDataFilesAction()).execute()``
    """
    print("\n--- Step 4: Periodic Compaction ---")

    total_files = sum(s.added_files for s in log.history())
    print(f"  Files before compaction: {total_files}")

    if total_files < min_files_threshold:
        print("  Compaction skipped: file count below threshold")
        return

    parent_id = log.current_snapshot().snapshot_id  # type: ignore[union-attr]
    compact_snap = Snapshot(
        snapshot_id=_next_id(),
        operation=DataOperation.REPLACE,
        parent_snapshot_id=parent_id,
        summary={
            "rewrite-reason": "merge-on-read compaction",
            "files-rewritten": str(total_files),
            "files-added": "1",
        },
        added_files=1,
        deleted_files=total_files,
        added_rows=log.total_added_rows(),
    )
    log.append_snapshot(compact_snap)
    print(f"  Compaction snapshot: {compact_snap.snapshot_id}")
    print(f"  Replaced {total_files} files → 1 optimised file")
    print(f"  Total rows preserved: {compact_snap.added_rows:,}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_cdc_pipeline() -> None:
    """Execute the full CDC pipeline demo."""
    print("\nApache Iceberg CDC Pipeline Demo")
    print(f"Started at: {datetime.now(UTC).isoformat()}")

    # Source state at T0
    source_v1: dict[int, dict[str, object]] = {
        1: {
            "name": "Alice",
            "email": "alice@example.com",
            "score": 9.5,
            "updated_at": "2024-01-01T00:00:00Z",
        },
        2: {
            "name": "Bob",
            "email": "bob@example.com",
            "score": 8.0,
            "updated_at": "2024-01-01T00:00:00Z",
        },
        3: {
            "name": "Carol",
            "email": "carol@example.com",
            "score": 7.2,
            "updated_at": "2024-01-01T00:00:00Z",
        },
        4: {
            "name": "Dave",
            "email": "dave@example.com",
            "score": 6.8,
            "updated_at": "2024-01-01T00:00:00Z",
        },
    }

    log, schema = initial_snapshot(source_v1)

    # Source state at T1: Bob updated, Carol deleted, Eve inserted
    source_v2: dict[int, dict[str, object]] = {
        1: source_v1[1],
        2: {
            "name": "Bob",
            "email": "bob@newdomain.com",
            "score": 8.5,
            "updated_at": "2024-01-02T00:00:00Z",
        },
        4: source_v1[4],
        5: {
            "name": "Eve",
            "email": "eve@example.com",
            "score": 9.1,
            "updated_at": "2024-01-02T00:00:00Z",
        },
    }

    batch1 = detect_changes(source_v1, source_v2, batch_id=1)
    apply_merge_on_read(batch1, log, schema)

    # Source state at T2: Dave updated score
    source_v3: dict[int, dict[str, object]] = {
        **source_v2,
        4: {
            "name": "Dave",
            "email": "dave@example.com",
            "score": 7.9,
            "updated_at": "2024-01-03T00:00:00Z",
        },
    }

    batch2 = detect_changes(source_v2, source_v3, batch_id=2)
    apply_merge_on_read(batch2, log, schema)

    compact_table(log, min_files_threshold=2)

    print("\n--- Pipeline Summary ---")
    print(f"  Total snapshots   : {log.snapshot_count()}")
    print(f"  Snapshot history  : {[s.snapshot_id for s in log.history()]}")
    print(f"  Operations        : {log.operations_summary()}")
    print(f"  Final columns     : {schema.column_names()}")
    print(f"  Schema changes    : {schema.change_count()}")


if __name__ == "__main__":
    run_cdc_pipeline()
