"""Tests for partitioning.py."""

from __future__ import annotations

from patterns.partitioning import (
    NullOrder,
    PartitionField,
    PartitionSpec,
    PartitionTransform,
    SortDirection,
    SortField,
    SortOrder,
)


class TestPartitionField:
    def test_identity_spec_string(self):
        pf = PartitionField("user_id", PartitionTransform.IDENTITY)
        assert pf.to_spec_string() == "user_id"

    def test_year_spec_string(self):
        pf = PartitionField("ts", PartitionTransform.YEAR)
        assert pf.to_spec_string() == "year(ts)"

    def test_bucket_spec_string(self):
        pf = PartitionField("id", PartitionTransform.BUCKET, width=16)
        assert "bucket" in pf.to_spec_string()
        assert "16" in pf.to_spec_string()

    def test_truncate_spec_string(self):
        pf = PartitionField("name", PartitionTransform.TRUNCATE, width=10)
        assert "truncate" in pf.to_spec_string()

    def test_is_time_based(self):
        assert PartitionField("ts", PartitionTransform.MONTH).is_time_based() is True
        assert PartitionField("id", PartitionTransform.BUCKET, width=8).is_time_based() is False

    def test_auto_name(self):
        pf = PartitionField("ts", PartitionTransform.DAY)
        assert "ts" in pf.name

    def test_requires_width(self):
        assert PartitionTransform.BUCKET.requires_width() is True
        assert PartitionTransform.YEAR.requires_width() is False


class TestPartitionSpec:
    def test_unpartitioned(self):
        spec = PartitionSpec()
        assert spec.is_unpartitioned() is True

    def test_has_fields(self):
        spec = PartitionSpec()
        spec.add_field(PartitionField("ts", PartitionTransform.DAY))
        assert not spec.is_unpartitioned()

    def test_partition_columns(self):
        spec = PartitionSpec()
        spec.add_field(PartitionField("ts", PartitionTransform.DAY))
        spec.add_field(PartitionField("region", PartitionTransform.IDENTITY))
        assert "ts" in spec.partition_columns()

    def test_has_time_partition(self):
        spec = PartitionSpec()
        spec.add_field(PartitionField("ts", PartitionTransform.MONTH))
        assert spec.has_time_partition() is True

    def test_to_dict(self):
        spec = PartitionSpec(spec_id=1)
        spec.add_field(PartitionField("ts", PartitionTransform.DAY))
        d = spec.to_dict()
        assert d["spec-id"] == 1
        assert len(d["fields"]) == 1  # type: ignore[arg-type]

    def test_void_is_unpartitioned(self):
        spec = PartitionSpec()
        spec.add_field(PartitionField("x", PartitionTransform.VOID))
        assert spec.is_unpartitioned() is True


class TestSortOrder:
    def test_unsorted(self):
        so = SortOrder()
        assert so.is_unsorted() is True
        assert so.to_sql() == ""

    def test_to_sql(self):
        so = SortOrder()
        so.add_field(SortField("ts", SortDirection.DESC))
        assert "ORDER BY" in so.to_sql()
        assert "DESC" in so.to_sql()

    def test_multiple_fields(self):
        so = SortOrder()
        so.add_field(SortField("a"))
        so.add_field(SortField("b", SortDirection.DESC))
        assert "a" in so.to_sql()
        assert "b" in so.to_sql()

    def test_null_order_default(self):
        sf = SortField("x")
        assert sf.null_order == NullOrder.NULLS_FIRST
