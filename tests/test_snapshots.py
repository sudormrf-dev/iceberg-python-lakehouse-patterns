"""Tests for snapshots.py."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from patterns.snapshots import DataOperation, RefType, Snapshot, SnapshotLog, SnapshotRef


def _snap(id: int, op: DataOperation = DataOperation.APPEND, parent: int | None = None) -> Snapshot:
    return Snapshot(snapshot_id=id, operation=op, parent_snapshot_id=parent)


class TestSnapshot:
    def test_is_root(self):
        assert _snap(1).is_root() is True
        assert _snap(2, parent=1).is_root() is False

    def test_net_rows(self):
        s = Snapshot(1, DataOperation.APPEND, added_rows=100, deleted_rows=10)
        assert s.net_rows() == 90

    def test_to_dict(self):
        s = _snap(1)
        d = s.to_dict()
        assert d["snapshot-id"] == 1
        assert d["operation"] == "append"


class TestSnapshotLog:
    def setup_method(self):
        self.log = SnapshotLog()

    def test_empty(self):
        assert self.log.current_snapshot() is None
        assert self.log.snapshot_count() == 0

    def test_append_and_current(self):
        self.log.append_snapshot(_snap(1))
        assert self.log.current_snapshot().snapshot_id == 1  # type: ignore[union-attr]

    def test_get_snapshot(self):
        self.log.append_snapshot(_snap(42))
        assert self.log.get_snapshot(42) is not None

    def test_get_snapshot_missing(self):
        assert self.log.get_snapshot(99) is None

    def test_rollback(self):
        self.log.append_snapshot(_snap(1))
        self.log.append_snapshot(_snap(2))
        assert self.log.rollback_to(1) is True
        assert self.log.current_snapshot().snapshot_id == 1  # type: ignore[union-attr]

    def test_rollback_missing(self):
        assert self.log.rollback_to(99) is False

    def test_history(self):
        self.log.append_snapshot(_snap(1))
        self.log.append_snapshot(_snap(2))
        assert len(self.log.history()) == 2

    def test_add_and_get_ref(self):
        ref = SnapshotRef("main", RefType.BRANCH, 1)
        self.log.add_ref(ref)
        assert self.log.get_ref("main") is ref

    def test_get_ref_missing(self):
        assert self.log.get_ref("nope") is None

    def test_snapshots_since(self):
        now = datetime.now(UTC)
        old = Snapshot(1, DataOperation.APPEND, timestamp=now - timedelta(days=2))
        new = Snapshot(2, DataOperation.APPEND, timestamp=now)
        self.log.append_snapshot(old)
        self.log.append_snapshot(new)
        since = now - timedelta(days=1)
        result = self.log.snapshots_since(since)
        assert len(result) == 1
        assert result[0].snapshot_id == 2

    def test_expire_snapshots(self):
        now = datetime.now(UTC)
        old = Snapshot(1, DataOperation.APPEND, timestamp=now - timedelta(days=10))
        recent = Snapshot(2, DataOperation.APPEND, timestamp=now)
        self.log.append_snapshot(old)
        self.log.append_snapshot(recent)
        to_expire = self.log.expire_snapshots(now - timedelta(days=1), keep_last=1)
        assert 1 in to_expire
        assert 2 not in to_expire

    def test_total_added_rows(self):
        self.log.append_snapshot(Snapshot(1, DataOperation.APPEND, added_rows=100))
        self.log.append_snapshot(Snapshot(2, DataOperation.APPEND, added_rows=50))
        assert self.log.total_added_rows() == 150

    def test_operations_summary(self):
        self.log.append_snapshot(_snap(1, DataOperation.APPEND))
        self.log.append_snapshot(_snap(2, DataOperation.APPEND))
        self.log.append_snapshot(_snap(3, DataOperation.DELETE))
        s = self.log.operations_summary()
        assert s["append"] == 2
        assert s["delete"] == 1

    def test_ref_is_branch(self):
        ref = SnapshotRef("main", RefType.BRANCH, 1)
        assert ref.is_branch() is True
        assert ref.is_tag() is False
