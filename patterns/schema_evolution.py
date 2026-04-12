"""Iceberg schema evolution patterns."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class IcebergType(str, Enum):
    BOOLEAN = "boolean"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMPTZ = "timestamptz"
    STRING = "string"
    UUID = "uuid"
    FIXED = "fixed"
    BINARY = "binary"
    LIST = "list"
    MAP = "map"
    STRUCT = "struct"

    def is_numeric(self) -> bool:
        return self in {
            IcebergType.INT,
            IcebergType.LONG,
            IcebergType.FLOAT,
            IcebergType.DOUBLE,
            IcebergType.DECIMAL,
        }

    def is_promoted_by(self, target: IcebergType) -> bool:
        """Return True if self can be safely promoted to target."""
        promotions: dict[IcebergType, set[IcebergType]] = {
            IcebergType.INT: {IcebergType.LONG, IcebergType.FLOAT, IcebergType.DOUBLE},
            IcebergType.FLOAT: {IcebergType.DOUBLE},
            IcebergType.STRING: {IcebergType.BINARY},
        }
        return target in promotions.get(self, set())


@dataclass
class IcebergColumn:
    field_id: int
    name: str
    col_type: IcebergType
    required: bool = False
    doc: str = ""

    def is_optional(self) -> bool:
        return not self.required

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {
            "id": self.field_id,
            "name": self.name,
            "type": self.col_type.value,
            "required": self.required,
        }
        if self.doc:
            d["doc"] = self.doc
        return d


class ChangeType(str, Enum):
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    UPDATE_TYPE = "update_type"
    MAKE_OPTIONAL = "make_optional"
    MAKE_REQUIRED = "make_required"
    UPDATE_DOC = "update_doc"


@dataclass
class ColumnChange:
    change_type: ChangeType
    column_name: str
    new_name: str = ""
    new_type: IcebergType | None = None
    new_column: IcebergColumn | None = None
    doc: str = ""

    def is_safe(self) -> bool:
        """Return True if this change is backward-compatible."""
        safe = {
            ChangeType.ADD_COLUMN,
            ChangeType.MAKE_OPTIONAL,
            ChangeType.UPDATE_DOC,
            ChangeType.RENAME_COLUMN,
        }
        return self.change_type in safe or (
            self.change_type == ChangeType.UPDATE_TYPE and self.new_type is not None
        )

    def description(self) -> str:
        if self.change_type == ChangeType.ADD_COLUMN and self.new_column:
            return f"ADD COLUMN {self.new_column.name} {self.new_column.col_type.value}"
        if self.change_type == ChangeType.DROP_COLUMN:
            return f"DROP COLUMN {self.column_name}"
        if self.change_type == ChangeType.RENAME_COLUMN:
            return f"RENAME COLUMN {self.column_name} TO {self.new_name}"
        if self.change_type == ChangeType.UPDATE_TYPE and self.new_type:
            return f"UPDATE COLUMN {self.column_name} TYPE TO {self.new_type.value}"
        if self.change_type == ChangeType.MAKE_OPTIONAL:
            return f"MAKE COLUMN {self.column_name} OPTIONAL"
        if self.change_type == ChangeType.MAKE_REQUIRED:
            return f"MAKE COLUMN {self.column_name} REQUIRED"
        return f"{self.change_type.value} {self.column_name}"


class SchemaEvolution:
    """Tracks and validates schema changes for an Iceberg table."""

    def __init__(self, schema_id: int = 0) -> None:
        self._schema_id = schema_id
        self._columns: dict[str, IcebergColumn] = {}
        self._changes: list[ColumnChange] = []
        self._next_field_id: int = 1

    @property
    def schema_id(self) -> int:
        return self._schema_id

    def add_column(self, col: IcebergColumn) -> SchemaEvolution:
        if col.field_id <= 0:
            col = IcebergColumn(
                field_id=self._next_field_id,
                name=col.name,
                col_type=col.col_type,
                required=col.required,
                doc=col.doc,
            )
        self._columns[col.name] = col
        self._next_field_id = max(self._next_field_id, col.field_id) + 1
        return self

    def apply_change(self, change: ColumnChange) -> bool:
        if change.change_type == ChangeType.ADD_COLUMN and change.new_column:
            self.add_column(change.new_column)
            self._changes.append(change)
            return True
        if change.change_type == ChangeType.DROP_COLUMN and change.column_name in self._columns:
            del self._columns[change.column_name]
            self._changes.append(change)
            return True
        if change.change_type == ChangeType.RENAME_COLUMN and change.column_name in self._columns:
            col = self._columns.pop(change.column_name)
            self._columns[change.new_name] = IcebergColumn(
                field_id=col.field_id,
                name=change.new_name,
                col_type=col.col_type,
                required=col.required,
                doc=col.doc,
            )
            self._changes.append(change)
            return True
        if change.change_type == ChangeType.MAKE_OPTIONAL and change.column_name in self._columns:
            col = self._columns[change.column_name]
            self._columns[change.column_name] = IcebergColumn(
                field_id=col.field_id,
                name=col.name,
                col_type=col.col_type,
                required=False,
                doc=col.doc,
            )
            self._changes.append(change)
            return True
        return False

    def column_names(self) -> list[str]:
        return list(self._columns.keys())

    def get_column(self, name: str) -> IcebergColumn | None:
        return self._columns.get(name)

    def change_count(self) -> int:
        return len(self._changes)

    def unsafe_changes(self) -> list[ColumnChange]:
        return [c for c in self._changes if not c.is_safe()]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema-id": self._schema_id,
            "fields": [col.to_dict() for col in self._columns.values()],
        }
