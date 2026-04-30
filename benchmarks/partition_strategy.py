"""Partition strategy benchmark: compare scan cost across four strategies.

Strategies compared:
  - NO_PARTITION  : full table scan every time
  - DATE_DAY      : day(order_date) pruning
  - HASH_BUCKET   : bucket(customer_id, 16) — uniform distribution
  - MULTI_COLUMN  : day(order_date) + identity(category)

Simulation assumptions:
  - 365 days of data, 10 categories, 1M total rows
  - Each physical file holds ~1 000 rows
  - A typical query filters on: last 7 days AND one category
  - Hash-bucket queries do NOT prune by date (hash is opaque)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from patterns.partitioning import PartitionField, PartitionSpec, PartitionTransform

# ---------------------------------------------------------------------------
# Domain objects
# ---------------------------------------------------------------------------


class Strategy(StrEnum):
    """Available partitioning strategies."""

    NO_PARTITION = "no_partition"
    DATE_DAY = "date_day"
    HASH_BUCKET = "hash_bucket"
    MULTI_COLUMN = "multi_column"


@dataclass
class DatasetConfig:
    """Represents the shape of the dataset to benchmark against."""

    total_rows: int = 1_000_000
    days: int = 365
    categories: int = 10
    rows_per_file: int = 1_000

    @property
    def total_files(self) -> int:
        return math.ceil(self.total_rows / self.rows_per_file)

    @property
    def rows_per_day(self) -> int:
        return self.total_rows // self.days

    @property
    def files_per_day(self) -> int:
        return max(1, math.ceil(self.rows_per_day / self.rows_per_file))


@dataclass
class QueryFilter:
    """Represents a typical analytical query predicate."""

    day_range: int = 7  # last N days
    category: str | None = "electronics"  # None = no category filter
    bucket_id: int | None = None  # used only by hash strategy


@dataclass
class ScanResult:
    """Result of simulated query planning for one strategy."""

    strategy: Strategy
    spec: PartitionSpec
    files_scanned: int
    files_total: int
    rows_scanned: int
    rows_total: int

    @property
    def scan_ratio(self) -> float:
        return self.files_scanned / max(1, self.files_total)

    @property
    def pruning_efficiency(self) -> float:
        """Fraction of data pruned (1.0 = perfect, 0.0 = no pruning)."""
        return 1.0 - self.scan_ratio

    def cost_label(self) -> str:
        if self.scan_ratio >= 0.9:
            return "FULL SCAN"
        if self.scan_ratio >= 0.5:
            return "HEAVY"
        if self.scan_ratio >= 0.2:
            return "MODERATE"
        return "EFFICIENT"


# ---------------------------------------------------------------------------
# Strategy builders (PartitionSpec)
# ---------------------------------------------------------------------------


def _spec_no_partition() -> PartitionSpec:
    return PartitionSpec(spec_id=0)  # empty = unpartitioned


def _spec_date_day() -> PartitionSpec:
    spec = PartitionSpec(spec_id=1)
    spec.add_field(PartitionField(source_column="order_date", transform=PartitionTransform.DAY))
    return spec


def _spec_hash_bucket(buckets: int = 16) -> PartitionSpec:
    spec = PartitionSpec(spec_id=2)
    spec.add_field(
        PartitionField(
            source_column="customer_id", transform=PartitionTransform.BUCKET, width=buckets
        )
    )
    return spec


def _spec_multi_column() -> PartitionSpec:
    spec = PartitionSpec(spec_id=3)
    spec.add_field(PartitionField(source_column="order_date", transform=PartitionTransform.DAY))
    spec.add_field(PartitionField(source_column="category", transform=PartitionTransform.IDENTITY))
    return spec


# ---------------------------------------------------------------------------
# Query planning simulation
# ---------------------------------------------------------------------------


def _simulate_scan(
    strategy: Strategy,
    spec: PartitionSpec,
    dataset: DatasetConfig,
    query: QueryFilter,
) -> ScanResult:
    """Estimate the number of files and rows that must be scanned for the query."""
    total_files = dataset.total_files
    total_rows = dataset.total_rows

    if strategy == Strategy.NO_PARTITION:
        # No pruning possible: scan everything
        files_scanned = total_files
        rows_scanned = total_rows

    elif strategy == Strategy.DATE_DAY:
        # Prune to day_range partitions out of `days`
        day_fraction = query.day_range / dataset.days
        files_scanned = max(1, math.ceil(total_files * day_fraction))
        rows_scanned = max(1, math.ceil(total_rows * day_fraction))

    elif strategy == Strategy.HASH_BUCKET:
        # Hash bucket can only prune by bucket; it does NOT prune by date.
        # If bucket_id is given, we read 1/N-th of files; otherwise full scan.
        if query.bucket_id is not None:
            bucket_count = 16  # matches _spec_hash_bucket default
            files_scanned = max(1, math.ceil(total_files / bucket_count))
            rows_scanned = max(1, math.ceil(total_rows / bucket_count))
        else:
            files_scanned = total_files
            rows_scanned = total_rows

    elif strategy == Strategy.MULTI_COLUMN:
        # Prune by day_range AND category simultaneously
        day_fraction = query.day_range / dataset.days
        cat_fraction = 1 / dataset.categories if query.category else 1.0
        combined = day_fraction * cat_fraction
        files_scanned = max(1, math.ceil(total_files * combined))
        rows_scanned = max(1, math.ceil(total_rows * combined))

    else:
        files_scanned = total_files
        rows_scanned = total_rows

    return ScanResult(
        strategy=strategy,
        spec=spec,
        files_scanned=files_scanned,
        files_total=total_files,
        rows_scanned=rows_scanned,
        rows_total=total_rows,
    )


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------


def run_benchmark(
    dataset: DatasetConfig | None = None, query: QueryFilter | None = None
) -> list[ScanResult]:
    """Run the four strategies and return a list of ScanResult objects."""
    ds = dataset or DatasetConfig()
    q = query or QueryFilter()

    strategies: list[tuple[Strategy, PartitionSpec]] = [
        (Strategy.NO_PARTITION, _spec_no_partition()),
        (Strategy.DATE_DAY, _spec_date_day()),
        (Strategy.HASH_BUCKET, _spec_hash_bucket()),
        (Strategy.MULTI_COLUMN, _spec_multi_column()),
    ]

    return [_simulate_scan(s, spec, ds, q) for s, spec in strategies]


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

_COL_WIDTH = 16


def _row(cells: list[str]) -> str:
    return "| " + " | ".join(c.ljust(_COL_WIDTH) for c in cells) + " |"


def _divider(n_cols: int) -> str:
    return "+" + (("-" * (_COL_WIDTH + 2)) + "+") * n_cols


def print_report(results: list[ScanResult], dataset: DatasetConfig, query: QueryFilter) -> None:
    """Print a formatted comparison table to stdout."""
    headers = [
        "Strategy",
        "Files Scanned",
        "Rows Scanned",
        "Scan Ratio",
        "Efficiency",
        "Cost Label",
    ]
    n = len(headers)
    divider = _divider(n)

    print(
        f"\nDataset : {dataset.total_rows:,} rows | {dataset.days} days "
        f"| {dataset.categories} categories | {dataset.total_files:,} files"
    )
    print(f"Query   : last {query.day_range} days | category='{query.category}'")
    print()
    print(divider)
    print(_row(headers))
    print(divider)

    for r in results:
        row = [
            r.strategy.value,
            f"{r.files_scanned:,} / {r.files_total:,}",
            f"{r.rows_scanned:,}",
            f"{r.scan_ratio:.1%}",
            f"{r.pruning_efficiency:.1%}",
            r.cost_label(),
        ]
        print(_row(row))

    print(divider)

    # Best strategy
    best = min(results, key=lambda x: x.files_scanned)
    print(
        f"\nBest strategy : {best.strategy.value} "
        f"({best.files_scanned:,} files, {best.pruning_efficiency:.1%} pruning efficiency)"
    )


# ---------------------------------------------------------------------------
# Partition spec details
# ---------------------------------------------------------------------------


def print_spec_details(results: list[ScanResult]) -> None:
    """Print partition spec metadata for each strategy."""
    print("\n--- Partition Spec Details ---")
    for r in results:
        spec = r.spec
        is_unpartitioned = spec.is_unpartitioned()
        has_time = spec.has_time_partition()
        cols = spec.partition_columns() or ["(none)"]
        print(
            f"  [{r.strategy.value:<15}]  unpartitioned={is_unpartitioned!s:<5}  "
            f"time_part={has_time!s:<5}  cols={cols}"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run benchmark and print results."""
    print("\nIceberg Partition Strategy Benchmark")
    print("=" * 60)

    dataset = DatasetConfig(total_rows=1_000_000, days=365, categories=10, rows_per_file=1_000)
    query = QueryFilter(day_range=7, category="electronics")

    results = run_benchmark(dataset, query)
    print_report(results, dataset, query)
    print_spec_details(results)

    # Second scenario: no category filter
    print("\n--- Scenario 2: date filter only (no category) ---")
    query2 = QueryFilter(day_range=30, category=None)
    results2 = run_benchmark(dataset, query2)
    print_report(results2, dataset, query2)

    # Third scenario: hash bucket with explicit bucket_id
    print("\n--- Scenario 3: hash bucket with bucket_id=7 ---")
    query3 = QueryFilter(day_range=7, category=None, bucket_id=7)
    results3 = run_benchmark(dataset, query3)
    print_report(results3, dataset, query3)


if __name__ == "__main__":
    main()
