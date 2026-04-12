"""Iceberg snapshot and time-travel patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class DataOperation(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    DELETE = "delete"
    REPLACE = "replace"


class RefType(str, Enum):
    BRANCH = "branch"
    TAG = "tag"


@dataclass
class Snapshot:
    snapshot_id: int
    operation: DataOperation
    parent_snapshot_id: int | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    summary: dict[str, str] = field(default_factory=dict)
    added_files: int = 0
    deleted_files: int = 0
    added_rows: int = 0
    deleted_rows: int = 0

    def is_root(self) -> bool:
        return self.parent_snapshot_id is None

    def net_rows(self) -> int:
        return self.added_rows - self.deleted_rows

    def to_dict(self) -> dict[str, object]:
        return {
            "snapshot-id": self.snapshot_id,
            "parent-snapshot-id": self.parent_snapshot_id,
            "timestamp-ms": int(self.timestamp.timestamp() * 1000),
            "operation": self.operation.value,
            "summary": self.summary,
        }


@dataclass
class SnapshotRef:
    name: str
    ref_type: RefType
    snapshot_id: int
    max_ref_age_ms: int | None = None
    min_snapshots_to_keep: int | None = None

    def is_branch(self) -> bool:
        return self.ref_type == RefType.BRANCH

    def is_tag(self) -> bool:
        return self.ref_type == RefType.TAG


class SnapshotLog:
    """Ordered log of Iceberg snapshots (history)."""

    def __init__(self) -> None:
        self._snapshots: list[Snapshot] = []
        self._refs: dict[str, SnapshotRef] = {}
        self._current_id: int | None = None

    def append_snapshot(self, snapshot: Snapshot) -> None:
        self._snapshots.append(snapshot)
        self._current_id = snapshot.snapshot_id

    def current_snapshot(self) -> Snapshot | None:
        if self._current_id is None:
            return None
        return next((s for s in self._snapshots if s.snapshot_id == self._current_id), None)

    def get_snapshot(self, snapshot_id: int) -> Snapshot | None:
        return next((s for s in self._snapshots if s.snapshot_id == snapshot_id), None)

    def add_ref(self, ref: SnapshotRef) -> None:
        self._refs[ref.name] = ref

    def get_ref(self, name: str) -> SnapshotRef | None:
        return self._refs.get(name)

    def history(self) -> list[Snapshot]:
        return list(self._snapshots)

    def snapshot_count(self) -> int:
        return len(self._snapshots)

    def snapshots_since(self, since: datetime) -> list[Snapshot]:
        return [s for s in self._snapshots if s.timestamp >= since]

    def expire_snapshots(self, older_than: datetime, keep_last: int = 1) -> list[int]:
        """Return snapshot ids to expire (not the last N)."""
        protected = {s.snapshot_id for s in self._snapshots[-keep_last:]}
        return [
            s.snapshot_id
            for s in self._snapshots
            if s.snapshot_id not in protected and s.timestamp < older_than
        ]

    def rollback_to(self, snapshot_id: int) -> bool:
        if self.get_snapshot(snapshot_id) is not None:
            self._current_id = snapshot_id
            return True
        return False

    def total_added_rows(self) -> int:
        return sum(s.added_rows for s in self._snapshots)

    def operations_summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for s in self._snapshots:
            counts[s.operation.value] = counts.get(s.operation.value, 0) + 1
        return counts
