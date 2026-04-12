"""Tests for schema_evolution.py."""

from __future__ import annotations

from patterns.schema_evolution import (
    ChangeType,
    ColumnChange,
    IcebergColumn,
    IcebergType,
    SchemaEvolution,
)


class TestIcebergType:
    def test_is_numeric(self):
        assert IcebergType.INT.is_numeric() is True
        assert IcebergType.STRING.is_numeric() is False

    def test_is_promoted_by(self):
        assert IcebergType.INT.is_promoted_by(IcebergType.LONG) is True
        assert IcebergType.INT.is_promoted_by(IcebergType.STRING) is False

    def test_float_to_double(self):
        assert IcebergType.FLOAT.is_promoted_by(IcebergType.DOUBLE) is True


class TestIcebergColumn:
    def test_is_optional(self):
        col = IcebergColumn(1, "name", IcebergType.STRING, required=False)
        assert col.is_optional() is True

    def test_to_dict(self):
        col = IcebergColumn(1, "age", IcebergType.INT, required=True)
        d = col.to_dict()
        assert d["id"] == 1
        assert d["name"] == "age"
        assert d["required"] is True

    def test_to_dict_with_doc(self):
        col = IcebergColumn(1, "x", IcebergType.LONG, doc="my doc")
        assert col.to_dict()["doc"] == "my doc"


class TestColumnChange:
    def test_add_column_is_safe(self):
        new_col = IcebergColumn(5, "tags", IcebergType.STRING)
        change = ColumnChange(ChangeType.ADD_COLUMN, "tags", new_column=new_col)
        assert change.is_safe() is True

    def test_drop_column_not_safe(self):
        change = ColumnChange(ChangeType.DROP_COLUMN, "old_col")
        assert change.is_safe() is False

    def test_make_optional_is_safe(self):
        change = ColumnChange(ChangeType.MAKE_OPTIONAL, "age")
        assert change.is_safe() is True

    def test_description_add(self):
        new_col = IcebergColumn(5, "score", IcebergType.DOUBLE)
        change = ColumnChange(ChangeType.ADD_COLUMN, "score", new_column=new_col)
        assert "ADD COLUMN" in change.description()

    def test_description_drop(self):
        change = ColumnChange(ChangeType.DROP_COLUMN, "old")
        assert "DROP COLUMN old" in change.description()

    def test_description_rename(self):
        change = ColumnChange(ChangeType.RENAME_COLUMN, "old", new_name="new")
        assert "RENAME" in change.description()


class TestSchemaEvolution:
    def setup_method(self):
        self.schema = SchemaEvolution(schema_id=1)
        self.schema.add_column(IcebergColumn(1, "id", IcebergType.LONG, required=True))
        self.schema.add_column(IcebergColumn(2, "name", IcebergType.STRING))

    def test_column_names(self):
        assert "id" in self.schema.column_names()
        assert "name" in self.schema.column_names()

    def test_get_column(self):
        col = self.schema.get_column("id")
        assert col is not None
        assert col.col_type == IcebergType.LONG

    def test_get_column_missing(self):
        assert self.schema.get_column("nope") is None

    def test_apply_add_column(self):
        new_col = IcebergColumn(3, "score", IcebergType.DOUBLE)
        change = ColumnChange(ChangeType.ADD_COLUMN, "score", new_column=new_col)
        assert self.schema.apply_change(change) is True
        assert "score" in self.schema.column_names()

    def test_apply_drop_column(self):
        change = ColumnChange(ChangeType.DROP_COLUMN, "name")
        assert self.schema.apply_change(change) is True
        assert "name" not in self.schema.column_names()

    def test_apply_rename_column(self):
        change = ColumnChange(ChangeType.RENAME_COLUMN, "name", new_name="full_name")
        assert self.schema.apply_change(change) is True
        assert "full_name" in self.schema.column_names()
        assert "name" not in self.schema.column_names()

    def test_apply_make_optional(self):
        change = ColumnChange(ChangeType.MAKE_OPTIONAL, "id")
        assert self.schema.apply_change(change) is True
        assert self.schema.get_column("id").is_optional() is True  # type: ignore[union-attr]

    def test_change_count(self):
        change = ColumnChange(ChangeType.DROP_COLUMN, "name")
        self.schema.apply_change(change)
        assert self.schema.change_count() == 1

    def test_to_dict(self):
        d = self.schema.to_dict()
        assert d["schema-id"] == 1
        assert len(d["fields"]) == 2  # type: ignore[arg-type]

    def test_auto_field_id(self):
        col = IcebergColumn(0, "new_col", IcebergType.STRING)
        self.schema.add_column(col)
        added = self.schema.get_column("new_col")
        assert added is not None
        assert added.field_id > 0
