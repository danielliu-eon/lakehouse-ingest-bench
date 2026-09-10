# SPDX-License-Identifier: Apache-2.0
"""Generate deterministic column values and encode row blocks.

Each cell depends only on ``(seed, batch, position, column)``, allowing
independent batch generation and reproducible shards. Encode columns in
blocks, then concatenate field encodings into Avro records.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import cast

import numpy as np

from ingest_bench.corpus.columns import (
    AVRO_TYPE_BY_KIND,
    ID_BLOCK,
    KIND_BLOB,
    KIND_BOOLEAN,
    KIND_CATEGORICAL,
    KIND_DECIMAL,
    KIND_INTEGER,
    KIND_RESERVED,
    KIND_TIMESTAMP,
    ROLE_PAYLOAD,
    UNBOUNDED_CARDINALITY,
    ColumnDistribution,
)

# ---------------------------------------------------------------------------
# Encoded value sizes
# ---------------------------------------------------------------------------

# Compute zigzag-varint lengths without encoding. Avro longs use up to ten
# bytes, with seven value bits per byte.
_VARINT_SHIFTS = tuple(range(7, 64, 7))
_UINT64_MASK = 0xFFFFFFFFFFFFFFFF


def zigzag(value: int) -> int:
    return (value << 1) ^ (value >> 63)


def zigzag_lengths(values: np.ndarray) -> np.ndarray:
    """Calculate Avro-long byte lengths using unsigned zigzag arithmetic."""
    words = np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)
    magnitude = (words << np.uint64(1)) ^ (np.uint64(0) - (words >> np.uint64(63)))
    lengths = np.ones(magnitude.shape, dtype=np.int64)
    for shift in _VARINT_SHIFTS:
        lengths += magnitude >= np.uint64(1 << shift)
    return lengths


# ---------------------------------------------------------------------------
# Avro cell encoding
# ---------------------------------------------------------------------------


def avro_long(value: int) -> bytes:
    magnitude = zigzag(value) & _UINT64_MASK
    out = bytearray()
    while True:
        byte = magnitude & 0x7F
        magnitude >>= 7
        if not magnitude:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def avro_bytes(data: bytes) -> bytes:
    return avro_long(len(data)) + data


def avro_cell(avro_type: object, value: object) -> bytes:
    """Encode one scalar cell; reject unions unsupported by the column encoders."""
    if isinstance(avro_type, list):
        raise ValueError(f"Avro union {avro_type!r} needs a branch index, which no column kind declares")
    if isinstance(avro_type, dict) or str(avro_type) in {"int", "long"}:
        return avro_long(cast(int, value))
    name = str(avro_type)
    if name == "double":
        return struct.pack("<d", cast(float, value))
    if name == "boolean":
        return b"\x01" if value else b"\x00"
    if name == "string":
        return avro_bytes(cast(str, value).encode())
    if name == "bytes":
        return avro_bytes(cast(bytes, value))
    raise ValueError(f"no Avro encoding for type {avro_type!r}")


@dataclass(frozen=True)
class Segment:
    """Per-row encoded byte runs and their lengths."""

    payload: np.ndarray
    lengths: np.ndarray


def segment_positions(lengths: np.ndarray) -> np.ndarray:
    """Return each byte's offset within its row's segment."""
    total = int(lengths.sum())
    starts: np.ndarray = np.repeat(np.cumsum(lengths) - lengths, lengths)
    positions: np.ndarray = np.arange(total, dtype=np.int64) - starts
    return positions


def varint_segment(values: np.ndarray, lengths: np.ndarray) -> Segment:
    """Encode zigzag varints as one contiguous byte segment.

    Each byte carries seven value bits, with continuation bits on all but the
    last byte of a value.
    """
    words = np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)
    zigzagged = (words << np.uint64(1)) ^ (np.uint64(0) - (words >> np.uint64(63)))
    within = segment_positions(lengths)
    shifts = np.uint64(7) * within.astype(np.uint64)
    payload = ((np.repeat(zigzagged, lengths) >> shifts) & np.uint64(0x7F)).astype(np.uint8)
    continuation = (within < np.repeat(lengths, lengths) - 1).astype(np.uint8)
    return Segment(payload | (continuation << np.uint8(7)), lengths)


def fixed_segment(payload: np.ndarray, rows: int, width: int) -> Segment:
    return Segment(payload, np.full(rows, width, dtype=np.int64))


def joined_segment(encoded: np.ndarray, lengths: np.ndarray) -> Segment:
    return Segment(np.frombuffer(b"".join(cast(list[bytes], encoded.tolist())), dtype=np.uint8), lengths)


def concatenated_records(segments: tuple[Segment, ...], rows: int) -> bytes:
    """Assemble column segments into row-major record bytes using their lengths."""
    lengths = np.stack([segment.lengths for segment in segments], axis=1).reshape(-1)
    ends = np.cumsum(lengths)
    starts = (ends - lengths).reshape(rows, len(segments))
    out = np.empty(int(ends[-1]) if ends.size else 0, dtype=np.uint8)
    for index, segment in enumerate(segments):
        out[np.repeat(starts[:, index], segment.lengths) + segment_positions(segment.lengths)] = segment.payload
    return out.tobytes()


# ---------------------------------------------------------------------------
# Counter-mode value draws
# ---------------------------------------------------------------------------

# Key Philox by (seed, batch, column) and seek by row, keeping values stable
# across block sizes. Round each row's word budget to four-word Philox blocks
# so row boundaries remain seekable.
_PHILOX_KEY_BYTES = 16
_PHILOX_WORDS_PER_BLOCK = 4
_WORD_BITS = 64
# A double carries 53 mantissa bits, so a uniform fraction is a word's top 53.
_MANTISSA_BITS = 53
_MANTISSA_SCALE = 2.0**-_MANTISSA_BITS

_ZIPF_CDF_CACHE: dict[tuple[int, float], np.ndarray] = {}
# Cache bounded value tables only while their cardinality fits the memory
# budget; compute larger distributions per rank.
COLUMN_VALUE_TABLE_MAX_RANKS = 1 << 18


def column_stream_key(seed: int, batch: int, name: str) -> int:
    digest = hashlib.blake2b(f"{seed}:{batch}:{name}".encode(), digest_size=_PHILOX_KEY_BYTES).digest()
    return int.from_bytes(digest, "little")


def column_words(seed: int, batch: int, name: str, row_start: int, rows: int, blocks_per_row: int) -> np.ndarray:
    """Return stream words for rows beginning at ``row_start``."""
    generator = np.random.Philox(key=column_stream_key(seed, batch, name))
    if row_start:
        generator.advance(row_start * blocks_per_row)
    return generator.random_raw(rows * blocks_per_row * _PHILOX_WORDS_PER_BLOCK)


def word_fractions(words: np.ndarray) -> np.ndarray:
    return (words >> np.uint64(_WORD_BITS - _MANTISSA_BITS)) * _MANTISSA_SCALE


def stream_bytes(words: np.ndarray, rows: int, words_per_row: int, width: int) -> np.ndarray:
    """Read ``rows`` by ``width`` bytes from a row-aligned word stream.

    Use little-endian words consistently across hosts.
    """
    matrix = words.astype("<u8", copy=False).view(np.uint8).reshape(rows, words_per_row * 8)
    return np.ascontiguousarray(matrix[:, :width])


def zipf_cdf(cardinality: int, alpha: float) -> np.ndarray:
    """CDF over ranks 1..cardinality with weight rank**-alpha; alpha 0 is uniform."""
    ranks = np.arange(1, cardinality + 1, dtype=np.float64)
    weights = ranks**-alpha
    cdf = np.cumsum(weights)
    cdf /= cdf[-1]
    cdf[-1] = 1.0
    return cdf


def column_cdf_array(cardinality: int, alpha: float) -> np.ndarray:
    key = (cardinality, alpha)
    cached = _ZIPF_CDF_CACHE.get(key)
    if cached is None:
        cached = zipf_cdf(cardinality, alpha)
        _ZIPF_CDF_CACHE[key] = cached
    return cached


def column_ranks(column: ColumnDistribution, words: np.ndarray) -> np.ndarray:
    """Select ranks according to the column's declared skew."""
    if column.cardinality <= 1:
        return np.zeros(words.size, dtype=np.int64)
    if column.alpha == 0.0:
        return (words % np.uint64(column.cardinality)).astype(np.int64)
    ranks = np.searchsorted(column_cdf_array(column.cardinality, column.alpha), word_fractions(words))
    return np.minimum(ranks, column.cardinality - 1).astype(np.int64)


def keystream(material: str, width: int) -> bytes:
    """Generate deterministic pseudorandom bytes for high-entropy values."""
    if width <= 0:
        return b""
    blocks = [
        hashlib.blake2b(f"{material}:{index}".encode(), digest_size=32).digest() for index in range((width + 31) // 32)
    ]
    return b"".join(blocks)[:width]


def categorical_label(column: ColumnDistribution, rank: int) -> str:
    vocabulary = column.vocabulary
    if rank < len(vocabulary):
        return vocabulary[rank]
    return f"{vocabulary[rank % len(vocabulary)]}-{rank // len(vocabulary):05d}"


def span_value(column: ColumnDistribution, rank: int) -> float:
    if column.cardinality <= 1:
        return column.minimum
    return column.minimum + (column.maximum - column.minimum) * rank / (column.cardinality - 1)


def value_for_rank(column: ColumnDistribution, rank: int) -> object | None:
    """Map a rank to a value for distributions determined by rank alone.

    Rounding and vocabulary reuse may map several ranks to one value.
    """
    if column.cardinality == UNBOUNDED_CARDINALITY:
        return None
    if column.kind == KIND_CATEGORICAL:
        return categorical_label(column, rank)
    if column.kind == KIND_INTEGER:
        return int(round(span_value(column, rank)))
    if column.kind == KIND_DECIMAL:
        return round(span_value(column, rank), 2)
    if column.kind == KIND_BOOLEAN:
        return rank == 0
    return None


def blob_byte_width(column: ColumnDistribution, payload_width: int) -> int:
    return payload_width if column.role == ROLE_PAYLOAD else column.width


def token_byte_width(column: ColumnDistribution) -> int:
    """Return the bytes needed for a hex token of the declared character width."""
    return column.width // 2 + 1


def column_word_blocks(column: ColumnDistribution, payload_width: int) -> int:
    """Return the Philox block budget per row for a column."""
    if column.cardinality != UNBOUNDED_CARDINALITY:
        return 1
    if column.kind == KIND_BLOB:
        width = blob_byte_width(column, payload_width)
    elif column.kind == KIND_CATEGORICAL:
        width = token_byte_width(column)
    else:
        return 1
    return max(1, -(-width // (8 * _PHILOX_WORDS_PER_BLOCK)))


def bounded_value(column: ColumnDistribution, seed: int, rank: int, payload_width: int) -> object:
    """Map a bounded rank to its value.

    Key blob bytes by rank so repeated ranks produce the same blob.
    """
    if column.kind == KIND_BLOB:
        return keystream(f"{seed}:{column.name}:{rank}", blob_byte_width(column, payload_width))
    value = value_for_rank(column, rank)
    if value is None:
        raise ValueError(f"column {column.name} has no bounded value for kind {column.kind!r}")
    return value


@dataclass(frozen=True)
class ValueTable:
    """Cached bounded values and their Avro encodings, indexed by rank."""

    values: np.ndarray
    encoded: np.ndarray
    encoded_sizes: np.ndarray


def build_value_table(values: list[object], avro_type: object) -> ValueTable:
    cells = [avro_cell(avro_type, value) for value in values]
    return ValueTable(
        np.array(values, dtype=object),
        np.array(cells, dtype=object),
        np.fromiter((len(cell) for cell in cells), dtype=np.int64, count=len(cells)),
    )


def value_table(column: ColumnDistribution, seed: int, payload_width: int) -> ValueTable:
    # Only blob contents depend on the seed or byte width. Calibration can reuse
    # every other table across its candidate payload widths.
    if column.kind != KIND_BLOB:
        seed, payload_width = 0, 0
    elif column.role != ROLE_PAYLOAD:
        payload_width = column.width
    return _value_table(column, seed, payload_width)


@lru_cache(maxsize=32)
def _value_table(column: ColumnDistribution, seed: int, payload_width: int) -> ValueTable:
    values = [bounded_value(column, seed, rank, payload_width) for rank in range(column.cardinality)]
    return build_value_table(values, AVRO_TYPE_BY_KIND[column.kind])


@dataclass(frozen=True)
class ColumnBlock:
    """Column values and row sizes with deferred Avro encoding.

    Calibration and statistics can inspect sizes and values without encoding.
    """

    values: list[object]
    encoded_sizes: np.ndarray
    encode: Callable[[], tuple[Segment, ...]]


def taken_column(table: ValueTable, ranks: np.ndarray) -> ColumnBlock:
    sizes = table.encoded_sizes[ranks]
    return ColumnBlock(
        cast(list[object], table.values[ranks].tolist()), sizes, lambda: (joined_segment(table.encoded[ranks], sizes),)
    )


def computed_column(values: list[object], avro_type: object) -> ColumnBlock:
    """Encode a column per row when no table or vectorized encoder applies."""
    cells = [avro_cell(avro_type, value) for value in values]
    sizes = np.fromiter((len(cell) for cell in cells), dtype=np.int64, count=len(cells))
    return ColumnBlock(values, sizes, lambda: (joined_segment(np.array(cells, dtype=object), sizes),))


def string_column(values: list[object]) -> ColumnBlock:
    """Encode single-byte strings in bulk, falling back for multibyte characters."""
    payload = "".join(cast(list[str], values)).encode()
    lengths = np.fromiter((len(cast(str, value)) for value in values), dtype=np.int64, count=len(values))
    if int(lengths.sum()) != len(payload):
        return computed_column(values, "string")
    prefixes = zigzag_lengths(lengths)
    return ColumnBlock(
        values,
        prefixes + lengths,
        lambda: (varint_segment(lengths, prefixes), Segment(np.frombuffer(payload, dtype=np.uint8), lengths)),
    )


def long_column(values: np.ndarray) -> ColumnBlock:
    lengths = zigzag_lengths(values)
    return ColumnBlock(cast(list[object], values.tolist()), lengths, lambda: (varint_segment(values, lengths),))


def prefixed_column(values: list[object], payload: np.ndarray, rows: int, width: int) -> ColumnBlock:
    """Encode fixed-width cells with separate shared-prefix and payload segments."""
    prefix = np.frombuffer(avro_long(width), dtype=np.uint8)
    return ColumnBlock(
        values,
        np.full(rows, prefix.size + width, dtype=np.int64),
        lambda: (fixed_segment(np.tile(prefix, rows), rows, prefix.size), fixed_segment(payload, rows, width)),
    )


def bounded_column(column: ColumnDistribution, seed: int, ranks: np.ndarray, payload_width: int) -> ColumnBlock:
    if column.cardinality <= COLUMN_VALUE_TABLE_MAX_RANKS:
        return taken_column(value_table(column, seed, payload_width), ranks)
    if column.kind == KIND_INTEGER:
        # Match `value_for_rank` rounding without materializing a large value table.
        span = (column.maximum - column.minimum) * ranks / (column.cardinality - 1)
        return long_column(np.rint(column.minimum + span).astype(np.int64))
    values = [bounded_value(column, seed, rank, payload_width) for rank in ranks.tolist()]
    if column.kind == KIND_CATEGORICAL:
        return string_column(values)
    return computed_column(values, AVRO_TYPE_BY_KIND[column.kind])


def unbounded_column(
    column: ColumnDistribution,
    words: np.ndarray,
    leading: np.ndarray,
    rows: int,
    words_per_row: int,
    payload_width: int,
) -> ColumnBlock:
    if column.kind == KIND_BLOB:
        width = blob_byte_width(column, payload_width)
        if width <= 0:
            return prefixed_column([b""] * rows, np.empty(0, dtype=np.uint8), rows, 0)
        payload = stream_bytes(words, rows, words_per_row, width)
        buffer = payload.tobytes()
        values = [buffer[index * width : (index + 1) * width] for index in range(rows)]
        return prefixed_column(cast(list[object], values), payload.reshape(-1), rows, width)
    if column.kind == KIND_CATEGORICAL:
        width = column.width
        byte_width = token_byte_width(column)
        hexed = stream_bytes(words, rows, words_per_row, byte_width).tobytes().hex()
        stride = 2 * byte_width
        tokens = [hexed[index * stride : index * stride + width] for index in range(rows)]
        # Hex characters are single-byte; trim each row to the declared width.
        payload = np.frombuffer(hexed.encode("ascii"), dtype=np.uint8).reshape(rows, stride)[:, :width]
        return prefixed_column(cast(list[object], tokens), np.ascontiguousarray(payload).reshape(-1), rows, width)
    if column.kind == KIND_INTEGER:
        low = int(column.minimum)
        span = max(1, int(column.maximum) - low + 1)
        return long_column((leading % np.uint64(span)).astype(np.int64) + low)
    if column.kind == KIND_DECIMAL:
        rounded = np.round(column.minimum + (column.maximum - column.minimum) * word_fractions(leading), 2)
        return ColumnBlock(
            cast(list[object], rounded.tolist()),
            np.full(rows, 8, dtype=np.int64),
            lambda: (fixed_segment(rounded.astype("<f8").view(np.uint8), rows, 8),),
        )
    raise ValueError(f"column {column.name} has no unbounded value generator for kind {column.kind!r}")


def timestamp_offsets(column: ColumnDistribution, words: np.ndarray, window_ms: int) -> np.ndarray:
    """Draw millisecond offsets within the batch arrival window."""
    if column.cardinality == UNBOUNDED_CARDINALITY:
        fractions = word_fractions(words)
    else:
        fractions = column_ranks(column, words).astype(np.float64) / column.cardinality
    return np.floor(fractions * window_ms).astype(np.int64)


def column_block(
    column: ColumnDistribution,
    seed: int,
    batch: int,
    row_start: int,
    rows: int,
    payload_width: int,
    batch_start_ms: int,
    batch_interval_ms: int,
) -> ColumnBlock:
    """Generate a column block from its independent counter stream."""
    blocks_per_row = column_word_blocks(column, payload_width)
    words_per_row = blocks_per_row * _PHILOX_WORDS_PER_BLOCK
    words = column_words(seed, batch, column.name, row_start, rows, blocks_per_row)
    # Use the first word of each row's block when only one variate is needed.
    leading = words[::words_per_row]
    if column.kind == KIND_TIMESTAMP:
        return long_column(timestamp_offsets(column, leading, batch_interval_ms) + batch_start_ms)
    if column.cardinality == UNBOUNDED_CARDINALITY:
        return unbounded_column(column, words, leading, rows, words_per_row, payload_width)
    return bounded_column(column, seed, column_ranks(column, leading), payload_width)


# ---------------------------------------------------------------------------
# Row blocks
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RowBlock:
    """Column-oriented values and Avro sizes for a contiguous batch row range."""

    rows: int
    values: dict[str, list[object]]
    encoders: tuple[Callable[[], tuple[Segment, ...]], ...]
    partition_keys: np.ndarray
    ids: np.ndarray
    encoded_sizes: np.ndarray

    def avro_records(self) -> list[dict[str, object]]:
        """Expose the column block as row dictionaries for scalar readers."""
        names = tuple(self.values)
        return [dict(zip(names, cells, strict=True)) for cells in zip(*self.values.values(), strict=True)]

    def avro_records_bytes(self) -> bytes:
        return concatenated_records(tuple(segment for encode in self.encoders for segment in encode()), self.rows)


def partition_label(key: int) -> str:
    return f"p{key:05d}"


def draw_partition_keys(seed: int, batch: int, row_start: int, rows: int, cdf: np.ndarray) -> np.ndarray:
    """Draw a batch's partition keys using the corpus Zipf distribution."""
    # Seek keys by row so changing block size preserves partition assignments.
    words = column_words(seed, batch, "partition_key", row_start, rows, 1)[::_PHILOX_WORDS_PER_BLOCK]
    return np.searchsorted(cdf, word_fractions(words), side="right").astype(np.int64)


# Grow the label cache only to the largest observed key.
_PARTITION_LABELS: ValueTable = build_value_table([], "string")


def partition_label_table(size: int) -> ValueTable:
    """Return cached ``p``-prefixed labels indexed by partition key."""
    global _PARTITION_LABELS
    if size > _PARTITION_LABELS.values.size:
        _PARTITION_LABELS = build_value_table(
            cast(list[object], [partition_label(key) for key in range(size)]), "string"
        )
    return _PARTITION_LABELS


def reserved_column_block(
    column: ColumnDistribution,
    ids: np.ndarray,
    keys: np.ndarray,
    labels: ValueTable,
    rows: int,
) -> ColumnBlock:
    """Compute reserved ID and partition-key fields from row position."""
    if column.name == "id":
        return long_column(ids)
    if column.name == "partition_key":
        return taken_column(labels, keys)
    raise ValueError(f"column {column.name} is not a reserved column")


def build_row_block(
    seed: int,
    batch: int,
    row_start: int,
    keys: np.ndarray,
    payload_width: int,
    batch_start_ms: int,
    batch_interval_ms: int,
    columns: tuple[ColumnDistribution, ...],
) -> RowBlock:
    """Generate rows ``[row_start, row_start + len(keys))`` for one batch.

    Event times remain within that batch's arrival window.
    """
    rows = int(keys.size)
    ids = batch * ID_BLOCK + np.arange(row_start, row_start + rows, dtype=np.int64)
    labels = partition_label_table(int(keys.max()) + 1 if rows else 1)
    values: dict[str, list[object]] = {}
    encoders: list[Callable[[], tuple[Segment, ...]]] = []
    sizes = np.zeros(rows, dtype=np.int64)
    for column in columns:
        if column.kind == KIND_RESERVED:
            encoded = reserved_column_block(column, ids, keys, labels, rows)
        else:
            encoded = column_block(
                column, seed, batch, row_start, rows, payload_width, batch_start_ms, batch_interval_ms
            )
        values[column.name] = encoded.values
        encoders.append(encoded.encode)
        sizes += encoded.encoded_sizes
    return RowBlock(rows, values, tuple(encoders), keys, ids, sizes)


def column_strings(block: RowBlock, name: str) -> list[str]:
    values = block.values[name]
    if not all(isinstance(value, str) for value in values):
        raise ValueError(f"column {name} is not a string column and cannot be a Kafka key")
    return cast(list[str], values)


# ---------------------------------------------------------------------------
# Row-size calibration
# ---------------------------------------------------------------------------

CALIBRATION_ROWS = 4096
CALIBRATION_INTERVAL_MS = 1000
# Calibrate at representative nonzero IDs and a modern epoch so varint
# widths resemble generated rows. Zero IDs and timestamps would leave
# too much of the row budget for payload.
CALIBRATION_BATCH = 1
CALIBRATION_EPOCH_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def realized_encoded_row_size(seed: int, payload_width: int, columns: tuple[ColumnDistribution, ...]) -> float:
    """Estimate mean encoded row size on a fixed calibration sample."""
    keys = np.arange(CALIBRATION_ROWS, dtype=np.int64)
    block = build_row_block(
        seed,
        CALIBRATION_BATCH,
        0,
        keys,
        payload_width,
        CALIBRATION_EPOCH_MS,
        CALIBRATION_INTERVAL_MS,
        columns,
    )
    return float(block.encoded_sizes.mean())


def calibrate_payload_width(seed: int, target: int, columns: tuple[ColumnDistribution, ...]) -> int:
    """Choose payload width to bring mean encoded row size closest to ``target``.

    Wider non-payload columns leave less room within the same row budget.
    """
    empty_mean = realized_encoded_row_size(seed, 0, columns)
    # Reject targets smaller than the non-payload fields can encode.
    if empty_mean > target:
        raise ValueError(
            f"the declared columns encode a mean row of {empty_mean:.2f} bytes with an empty payload, "
            f"which exceeds target_row_bytes {target}: widen the budget or narrow the columns"
        )
    # Payload length prefixes grow at varint boundaries. Measure nearby widths
    # after correcting the linear estimate to find the closest encoded size.
    estimate = max(0, int(target - empty_mean))
    corrected = max(0, estimate - int(round(realized_encoded_row_size(seed, estimate, columns) - target)))
    candidates = range(max(0, corrected - 1), corrected + 2)
    return min(candidates, key=lambda width: abs(realized_encoded_row_size(seed, width, columns) - target))
