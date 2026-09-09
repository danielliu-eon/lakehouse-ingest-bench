"""Realized value shape of a corpus's columns, sampled while it is written.

A preset declares what each column's values should look like; nothing about
that declaration is self-enforcing, since a column can be declared skewed and
generated flat, or declared injective and generated constant, and the corpus
would still publish the declaration. So the generator samples the rows it
writes and publishes what it measured beside what was asked for, and gates the
corpus on the two agreeing.

The statistics are sampled on a corpus-wide row stride rather than per batch,
which is what makes a shard's sample a subset of the unsharded corpus's sample
and the merged figures identical either way.
"""

from __future__ import annotations

import bisect
import hashlib
import math
import struct
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import cast

from ingest_bench.corpus.columns import UNBOUNDED_CARDINALITY, VALUE_KINDS, ColumnDistribution
from ingest_bench.corpus.values import RowBlock, column_cdf_array, value_for_rank

# Distinct values are held as the k smallest value hashes, so k bounds the
# memory and also the cardinality the statistic can state exactly.
COLUMN_SKETCH_SIZE = 1024
COLUMN_STATS_TARGET_SAMPLES = 200_000
# An expectation this small is indistinguishable from sampling noise, so a
# column below it is not compared at all rather than compared loosely.
COLUMN_CARDINALITY_GATE_MIN_EXPECTED = 8.0


def _as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def column_value_bytes(value: object) -> bytes:
    """The bytes a sampled value is characterized by.

    Fixed-width big-endian encodings rather than the corpus's varints, so a
    column's byte histogram measures its values rather than how compactly Avro
    happened to hold them.
    """
    if isinstance(value, bytes):
        return value
    # A decoded timestamp-millis arrives as a datetime; the epoch integer is
    # the value the generator actually drew.
    if isinstance(value, datetime):
        return round(value.timestamp() * 1000).to_bytes(8, "big", signed=True)
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bool):
        return b"\x01" if value else b"\x00"
    if isinstance(value, int):
        return value.to_bytes(8, "big", signed=True)
    if isinstance(value, float):
        return struct.pack(">d", value)
    raise TypeError(f"column value of type {type(value).__name__} has no byte encoding")


@dataclass
class ColumnStats:
    """Realized value shape of one column, sampled while batches are written.

    Distinct values are held as a K-minimum-values sketch so the statistic
    stays exact for small value sets, bounded in memory for large ones, and —
    because a union of per-shard sketches has the same k smallest hashes as the
    whole corpus — identical whether the corpus was generated in one pass or
    merged from shards.
    """

    name: str
    sampled_rows: int = 0
    value_bytes: int = 0
    byte_counts: Counter[int] = field(default_factory=Counter)
    sketch: list[int] = field(default_factory=list)
    saturated: bool = False

    def observe(self, value: object) -> None:
        data = column_value_bytes(value)
        self.sampled_rows += 1
        self.value_bytes += len(data)
        self.byte_counts.update(data)
        digest = int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")
        position = bisect.bisect_left(self.sketch, digest)
        if position < len(self.sketch) and self.sketch[position] == digest:
            return
        if len(self.sketch) >= COLUMN_SKETCH_SIZE:
            self.saturated = True
            if position >= COLUMN_SKETCH_SIZE:
                return
            self.sketch.pop()
        self.sketch.insert(position, digest)

    def distinct_values(self) -> float:
        if not self.saturated:
            return float(len(self.sketch))
        return (COLUMN_SKETCH_SIZE - 1) * 2**64 / self.sketch[COLUMN_SKETCH_SIZE - 1]

    def value_byte_entropy_bits_per_byte(self) -> float:
        """Entropy of the sampled bytes, pooled across rows.

        This is a property of the bytes inside a value and is blind to values
        repeating across rows: a constant blob pools the same random bytes
        every row, so it scores as high here as a fresh blob per row would.
        It is what certifies that an injective column's bytes are themselves
        incompressible, never that a column is.
        """
        if self.value_bytes == 0:
            return 0.0
        total = float(self.value_bytes)
        return -sum((count / total) * math.log2(count / total) for count in self.byte_counts.values())

    def entropy_bits_per_byte(self) -> float:
        """Byte entropy discounted by the sample's rate of first-seen values.

        A value seen before carries no new information, so the byte entropy is
        weighted by the fraction of sampled rows that carried a value the
        column had not produced yet. That fraction is what separates a
        constant column from an incompressible one — the byte histogram alone
        cannot, since it is the same histogram either way.

        This ranks columns by repetition; it is not a codec's bits per byte.
        Once a bounded column has produced every value it has, the discount is
        its cardinality over the sample size, so the figure keeps falling as
        the sample grows while the column's real compressibility does not move.
        It is therefore comparable between columns of one corpus, or across
        corpora only at equal ``sampled_rows``.
        """
        if self.sampled_rows == 0:
            return 0.0
        return self.value_byte_entropy_bits_per_byte() * min(1.0, self.distinct_values() / self.sampled_rows)

    def mean_value_bytes(self) -> float:
        return self.value_bytes / self.sampled_rows if self.sampled_rows else 0.0

    def merge(self, other: ColumnStats) -> None:
        self.sampled_rows += other.sampled_rows
        self.value_bytes += other.value_bytes
        self.byte_counts.update(other.byte_counts)
        merged = sorted(set(self.sketch) | set(other.sketch))
        self.saturated = self.saturated or other.saturated or len(merged) > COLUMN_SKETCH_SIZE
        self.sketch = merged[:COLUMN_SKETCH_SIZE]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "sampled_rows": self.sampled_rows,
            "value_bytes": self.value_bytes,
            "byte_counts": {str(byte): count for byte, count in sorted(self.byte_counts.items())},
            "sketch": self.sketch,
            "saturated": self.saturated,
        }

    @staticmethod
    def from_dict(raw: dict[str, object]) -> ColumnStats:
        counts: Counter[int] = Counter()
        for byte, count in cast(dict[str, object], raw["byte_counts"]).items():
            counts[int(byte)] = _as_int_value(count)
        return ColumnStats(
            name=str(raw["name"]),
            sampled_rows=_as_int_value(raw["sampled_rows"]),
            value_bytes=_as_int_value(raw["value_bytes"]),
            byte_counts=counts,
            sketch=[_as_int_value(value) for value in cast(list[object], raw["sketch"])],
            saturated=bool(raw["saturated"]),
        )


def observe_block(block: RowBlock, column_stats: dict[str, ColumnStats], global_row_start: int, stride: int) -> None:
    """Sample the block's rows on the corpus-wide stride.

    ``global_row_start`` counts the row's position from the start of the corpus
    rather than of the batch, so the stride selects the same rows in a shard as
    in the unsharded corpus and the merged statistic is identical. The values
    sampled are the ones the generator drew, so a timestamp is characterized as
    the epoch milliseconds it is rather than as a decoder's rendering of them.
    """
    for index in range(-global_row_start % stride, block.rows, stride):
        for name, values in block.values.items():
            column_stats[name].observe(values[index])


def stats_stride(estimated_rows: int, target_samples: int = COLUMN_STATS_TARGET_SAMPLES) -> int:
    """Rows between column samples, so the sample size follows the target rather than the corpus.

    The estimate comes from the preset alone, which is what keeps a shard's
    stride equal to the unsharded corpus's stride: a stride derived from rows
    a shard actually wrote would select different rows in every shard and the
    merged sketch would no longer be the whole corpus's sketch.
    """
    return max(1, estimated_rows // target_samples)


def expected_distinct_values(column: ColumnDistribution, sampled_rows: int) -> float | None:
    """Distinct values a sample of ``sampled_rows`` should show, or None where nothing can be compared.

    Rounding and vocabulary reuse can map several ranks onto one value, so the
    expectation is over the value set the ranks realize rather than over the
    declared rank count. A column with no closed form, a value set wider than
    the sketch is exact for, or an expectation too small to distinguish from
    sampling noise yields None — so the deviation published beside it is None
    rather than 0.0, which would read as "this column matched".
    """
    if column.kind not in VALUE_KINDS or column.cardinality == UNBOUNDED_CARDINALITY or sampled_rows == 0:
        return None
    if column.cardinality > COLUMN_SKETCH_SIZE:
        return None
    cdf = column_cdf_array(column.cardinality, column.alpha)
    weights: dict[str, float] = defaultdict(float)
    for rank, cumulative in enumerate(cdf):
        value = value_for_rank(column, rank)
        if value is None:
            return None
        weights[repr(value)] += cumulative - (cdf[rank - 1] if rank else 0.0)
    # The rank weights are read off a numpy CDF, so the sum is a numpy scalar;
    # corpus.json is published to consumers that only know JSON numbers.
    expected = float(sum(1.0 - (1.0 - weight) ** sampled_rows for weight in weights.values()))
    return expected if expected >= COLUMN_CARDINALITY_GATE_MIN_EXPECTED else None


def column_stats_summary(
    column: ColumnDistribution, stats: ColumnStats, cardinality_deviation: float | None
) -> dict[str, object]:
    return {
        "kind": column.kind,
        "declared_cardinality": column.cardinality,
        "declared_alpha": column.alpha,
        "sampled_rows": stats.sampled_rows,
        "distinct_values": stats.distinct_values(),
        "distinct_exact": not stats.saturated,
        "entropy_bits_per_byte": stats.entropy_bits_per_byte(),
        "value_byte_entropy_bits_per_byte": stats.value_byte_entropy_bits_per_byte(),
        "mean_value_bytes": stats.mean_value_bytes(),
        "cardinality_checked": cardinality_deviation is not None,
        "cardinality_relative_deviation": cardinality_deviation,
    }
