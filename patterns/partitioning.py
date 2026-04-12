"""Iceberg partitioning and sort order patterns."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PartitionTransform(str, Enum):
    IDENTITY = "identity"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    BUCKET = "bucket"
    TRUNCATE = "truncate"
    VOID = "void"

    def requires_width(self) -> bool:
        return self in {PartitionTransform.BUCKET, PartitionTransform.TRUNCATE}


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class NullOrder(str, Enum):
    NULLS_FIRST = "nulls-first"
    NULLS_LAST = "nulls-last"


@dataclass
class PartitionField:
    source_column: str
    transform: PartitionTransform
    name: str = ""
    width: int | None = None

    def __post_init__(self) -> None:
        if not self.name:
            if self.transform.requires_width() and self.width:
                self.name = f"{self.source_column}_{self.transform.value}_{self.width}"
            else:
                self.name = f"{self.source_column}_{self.transform.value}"

    def to_spec_string(self) -> str:
        if self.transform == PartitionTransform.IDENTITY:
            return self.source_column
        if self.transform.requires_width() and self.width is not None:
            return f"{self.transform.value}({self.source_column}, {self.width})"
        return f"{self.transform.value}({self.source_column})"

    def is_time_based(self) -> bool:
        return self.transform in {
            PartitionTransform.YEAR,
            PartitionTransform.MONTH,
            PartitionTransform.DAY,
            PartitionTransform.HOUR,
        }


@dataclass
class PartitionSpec:
    spec_id: int = 0
    fields: list[PartitionField] = field(default_factory=list)

    def add_field(self, pf: PartitionField) -> PartitionSpec:
        self.fields.append(pf)
        return self

    def is_unpartitioned(self) -> bool:
        return len(self.fields) == 0 or all(
            f.transform == PartitionTransform.VOID for f in self.fields
        )

    def partition_columns(self) -> list[str]:
        return [f.source_column for f in self.fields if f.transform != PartitionTransform.VOID]

    def has_time_partition(self) -> bool:
        return any(f.is_time_based() for f in self.fields)

    def to_dict(self) -> dict[str, object]:
        return {
            "spec-id": self.spec_id,
            "fields": [
                {
                    "source-column": f.source_column,
                    "transform": f.to_spec_string(),
                    "name": f.name,
                }
                for f in self.fields
            ],
        }


@dataclass
class SortField:
    source_column: str
    direction: SortDirection = SortDirection.ASC
    null_order: NullOrder = NullOrder.NULLS_FIRST
    transform: PartitionTransform = PartitionTransform.IDENTITY

    def to_sql_fragment(self) -> str:
        return f"{self.source_column} {self.direction.value.upper()}"


@dataclass
class SortOrder:
    order_id: int = 0
    fields: list[SortField] = field(default_factory=list)

    def add_field(self, sf: SortField) -> SortOrder:
        self.fields.append(sf)
        return self

    def is_unsorted(self) -> bool:
        return len(self.fields) == 0

    def to_sql(self) -> str:
        if self.is_unsorted():
            return ""
        return "ORDER BY " + ", ".join(f.to_sql_fragment() for f in self.fields)
