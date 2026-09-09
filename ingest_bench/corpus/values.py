# SPDX-License-Identifier: Apache-2.0
"""Deterministic value streams, and the row blocks assembled out of them.

Every cell is a pure function of ``(seed, batch, position, column)``, so a row
is addressable without drawing the rows before it. That is what makes a
regenerated corpus byte-identical after a preemption, and what lets a shard
that produces one batch alone emit exactly the rows the unsharded run emits.

Values are drawn and encoded a column at a time: a block of rows is held as
columns, and an Avro record is the concatenation of its fields' encodings, so
the block's bytes come out of per-column byte runs instead of a row-at-a-time
encoder.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable
from dataclasses import dataclass
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

# An Avro long is a zigzag varint, so it spends one byte per seven bits of the
# zigzagged magnitude and never more than ten. Lengths are computed from the
# values rather than measured by encoding, because the partition byte shares
# sum a row's length over the whole corpus.
_VARINT_SHIFTS = tuple(range(7, 64, 7))
_UINT64_MASK = 0xFFFFFFFFFFFFFFFF


def zigzag(value: int) -> int:
    return (value << 1) ^ (value >> 63)


def zigzag_lengths(values: np.ndarray) -> np.ndarray:
    """Bytes each Avro long in ``values`` encodes to.

    The shift is taken on the unsigned view so a wide value wraps the way the
    encoder's two's-complement zigzag does rather than overflowing a signed
    accumulator.
    """
    words = np.ascontiguousarray(values, dtype=np.int64).view(np.uint64)
    magnitude = (words << np.uint64(1)) ^ (np.uint64(0) - (words >> np.uint64(63)))
    lengths = np.ones(magnitude.shape, dtype=np.int64)
    for shift in _VARINT_SHIFTS:
        lengths += magnitude >= np.uint64(1 << shift)
    return lengths


# ---------------------------------------------------------------------------
# Avro cell encoding
# ---------------------------------------------------------------------------

# A record is the concatenation of its fields' encodings and carries no framing
# of its own, which is what lets a block of rows be assembled from per-column
# byte runs instead of from a row-at-a-time encoder.


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
    """One Avro-encoded cell: the scalar the column encoders reproduce.

    No column kind maps to a union, so a union reaching here means the schema
    and the encoder disagree: the branch index would be missing and every row
    after it would decode as garbage.
    """
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
    """One run of bytes per row, in row order: the payload and each row's length.

    A cell whose prefix and payload vectorize separately contributes two
    segments, so every segment stays a single array expression.
    """

    payload: np.ndarray
    lengths: np.ndarray


def segment_positions(lengths: np.ndarray) -> np.ndarray:
    """The offset of each byte inside its own row's run."""
    total = int(lengths.sum())
    starts: np.ndarray = np.repeat(np.cumsum(lengths) - lengths, lengths)
    positions: np.ndarray = np.arange(total, dtype=np.int64) - starts
    return positions


def varint_segment(values: np.ndarray, lengths: np.ndarray) -> Segment:
    """The zigzag varints of ``values``, back to back.

    Byte j holds bits [7j, 7j+7) of the zigzagged value with the continuation
    bit set on every byte but the last, so the column is one expression over a
    (byte, value) index pair rather than a loop.
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
    """Pre-encoded cells taken by rank and concatenated."""
    return Segment(np.frombuffer(b"".join(cast(list[bytes], encoded.tolist())), dtype=np.uint8), lengths)


def concatenated_records(segments: tuple[Segment, ...], rows: int) -> bytes:
    """Every row's segments, in row order.

    The output offset of one segment of one row is the row-major running total
    of every length before it, and each segment's bytes are already in row
    order — so placing a whole column is a single scatter.
    """
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

# A column's variates come from a counter-based generator keyed on
# (seed, batch, column) and positioned at the row index, so a block of rows
# holds the same cells whatever block size drew it.
#
# Philox emits four 64-bit words per counter block and seeking counts whole
# blocks, so a row's word budget is rounded up to a block: the offset of a row
# in the stream is then a block count and a row is seekable exactly.
_PHILOX_KEY_BYTES = 16
_PHILOX_WORDS_PER_BLOCK = 4
_WORD_BITS = 64
# A double carries 53 mantissa bits, so a uniform fraction is a word's top 53.
_MANTISSA_BITS = 53
_MANTISSA_SCALE = 2.0**-_MANTISSA_BITS

_ZIPF_CDF_CACHE: dict[tuple[int, float], np.ndarray] = {}
# A rank-indexed value table is the cheapest way to turn ranks into values, and
# its memory is the cardinality rather than the block, so it is only built for
# a cardinality whose table stays small against the generator's budget. Above
# it the values are computed per drawn rank, which costs a Python call per row
# on a column the shipped schemas reach only at the top of the range.
COLUMN_VALUE_TABLE_MAX_RANKS = 1 << 18


def column_stream_key(seed: int, batch: int, name: str) -> int:
    digest = hashlib.blake2b(f"{seed}:{batch}:{name}".encode(), digest_size=_PHILOX_KEY_BYTES).digest()
    return int.from_bytes(digest, "little")


def column_words(seed: int, batch: int, name: str, row_start: int, rows: int, blocks_per_row: int) -> np.ndarray:
    """The words one column consumes for ``rows`` rows starting at ``row_start``."""
    generator = np.random.Philox(key=column_stream_key(seed, batch, name))
    if row_start:
        generator.advance(row_start * blocks_per_row)
    return generator.random_raw(rows * blocks_per_row * _PHILOX_WORDS_PER_BLOCK)


def word_fractions(words: np.ndarray) -> np.ndarray:
    return (words >> np.uint64(_WORD_BITS - _MANTISSA_BITS)) * _MANTISSA_SCALE


def stream_bytes(words: np.ndarray, rows: int, words_per_row: int, width: int) -> np.ndarray:
    """``rows`` x ``width`` bytes read out of a row-aligned word stream.

    The words are read little-endian whatever the host's order is, so the
    corpus is a function of the seed rather than of the machine that wrote it.
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
    """The value rank each word selects, under the column's declared skew."""
    if column.cardinality <= 1:
        return np.zeros(words.size, dtype=np.int64)
    if column.alpha == 0.0:
        return (words % np.uint64(column.cardinality)).astype(np.int64)
    ranks = np.searchsorted(column_cdf_array(column.cardinality, column.alpha), word_fractions(words))
    return np.minimum(ranks, column.cardinality - 1).astype(np.int64)


def keystream(material: str, width: int) -> bytes:
    """Deterministic pseudorandom bytes: full-entropy, so nothing downstream can compress them away."""
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
    """The value a rank maps to, where the rank alone determines it.

    Rounding and vocabulary reuse can map several ranks onto one value, so the
    realized cardinality gate has to compare against the value set rather than
    against the declared rank count.
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
    """The payload column carries the calibrated remainder; any other blob its declared width."""
    return payload_width if column.role == ROLE_PAYLOAD else column.width


def token_byte_width(column: ColumnDistribution) -> int:
    """Bytes behind an unbounded token, whose value is their hex truncated to the declared width."""
    return column.width // 2 + 1


def column_word_blocks(column: ColumnDistribution, payload_width: int) -> int:
    """Philox blocks one row of a column consumes, which fixes where a row sits."""
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
    """The value a bounded column's rank carries.

    A bounded blob's bytes are keyed on the rank rather than on the row, which
    is what makes its distinct value count the declared cardinality while its
    bytes stay incompressible inside a value.
    """
    if column.kind == KIND_BLOB:
        return keystream(f"{seed}:{column.name}:{rank}", blob_byte_width(column, payload_width))
    value = value_for_rank(column, rank)
    if value is None:
        raise ValueError(f"column {column.name} has no bounded value for kind {column.kind!r}")
    return value


@dataclass(frozen=True)
class ValueTable:
    """Rank-indexed values of a bounded column, beside the Avro cell each encodes to.

    Holding the cell beside the value makes a bounded column's block a take and
    a join; the sizes come from the cells, so a size cannot disagree with them.
    """

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


_VALUE_TABLE_CACHE: dict[tuple[ColumnDistribution, int, int], ValueTable] = {}


def value_table(column: ColumnDistribution, seed: int, payload_width: int) -> ValueTable:
    key = (column, seed, payload_width)
    cached = _VALUE_TABLE_CACHE.get(key)
    if cached is None:
        values = [bounded_value(column, seed, rank, payload_width) for rank in range(column.cardinality)]
        cached = build_value_table(values, AVRO_TYPE_BY_KIND[column.kind])
        _VALUE_TABLE_CACHE[key] = cached
    return cached


@dataclass(frozen=True)
class ColumnBlock:
    """One column of a row block: its values, its row sizes, and its Avro bytes on demand.

    The sizes are eager because the partition byte shares need them and because
    the row-size calibration reads nothing else; the encoding is deferred so
    calibration and any value-level reader never pay for bytes they discard.
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
    """A column whose values are neither tabulated nor vectorizable, encoded per row."""
    cells = [avro_cell(avro_type, value) for value in values]
    sizes = np.fromiter((len(cell) for cell in cells), dtype=np.int64, count=len(cells))
    return ColumnBlock(values, sizes, lambda: (joined_segment(np.array(cells, dtype=object), sizes),))


def string_column(values: list[object]) -> ColumnBlock:
    """A string column encoded a column at a time.

    utf-8 never spends fewer than one byte per character, so a byte total equal
    to the character total proves every cell is single-byte and the per-row
    lengths are the character counts — which is what lets the prefixes be one
    varint segment. A wider character falls back to per-cell encoding.
    """
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
    """A length-prefixed cell of one fixed width, whose prefix is the same on every row.

    The prefix and the payload are separate runs of bytes, so each stays a
    single array expression and the row assembly interleaves them.
    """
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
        # A rank spans [minimum, maximum] and is then rounded to a whole
        # number, which is the same expression and the same half-to-even rule
        # over an array as over a scalar — so this is `value_for_rank` without
        # a table to hold the ranks a wide column has.
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
        # A token is hex, so its characters are its bytes: the payload is the
        # hex buffer with each row's tail past the declared width dropped.
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
    """Jitter inside the batch's arrival window, so event time still tracks batch order.

    The fraction is scaled by the window rather than by a mean gap, which is
    what keeps every drawn offset inside the window the batch owns: no row of
    one batch can carry an event time that belongs to the next.
    """
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
    """One column of a row block, drawn from its own counter-mode stream."""
    blocks_per_row = column_word_blocks(column, payload_width)
    words_per_row = blocks_per_row * _PHILOX_WORDS_PER_BLOCK
    words = column_words(seed, batch, column.name, row_start, rows, blocks_per_row)
    # A column that reads one variate per row takes the first word of the row's
    # block, so the rest of the block is skipped rather than carried forward.
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
    """One contiguous run of a batch's rows, held as columns rather than rows.

    The Avro sizes travel with the values because the partition byte shares sum
    a row length over the whole corpus, and a pass to measure each row would
    cost more than writing the batch.
    """

    rows: int
    values: dict[str, list[object]]
    encoders: tuple[Callable[[], tuple[Segment, ...]], ...]
    partition_keys: np.ndarray
    ids: np.ndarray
    encoded_sizes: np.ndarray

    def avro_records(self) -> list[dict[str, object]]:
        """The block as row dicts, which is what a row-at-a-time reader compares against."""
        names = tuple(self.values)
        return [dict(zip(names, cells, strict=True)) for cells in zip(*self.values.values(), strict=True)]

    def avro_records_bytes(self) -> bytes:
        return concatenated_records(tuple(segment for encode in self.encoders for segment in encode()), self.rows)


def partition_label(key: int) -> str:
    return f"p{key:05d}"


def draw_partition_keys(seed: int, batch: int, row_start: int, rows: int, cdf: np.ndarray) -> np.ndarray:
    """Per-row partition keys of one batch, under the corpus's Zipf weights."""
    # The keys ride a counter stream positioned like a column's because
    # re-blocking must not move a key: a row's key has to come out the same
    # whether its batch was drawn in one block or in several, which is what
    # lets a shard agree with the unsharded run.
    words = column_words(seed, batch, "partition_key", row_start, rows, 1)[::_PHILOX_WORDS_PER_BLOCK]
    return np.searchsorted(cdf, word_fractions(words), side="right").astype(np.int64)


# One table, grown to cover the largest key any block has asked for, so its
# memory follows the keys a corpus realizes rather than the count it declares.
_PARTITION_LABELS: ValueTable = build_value_table([], "string")


def partition_label_table(size: int) -> ValueTable:
    """`p`-prefixed partition labels indexed by key, so a block's labels are a take."""
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
    """Identity and the partition key: computed from the block's position, never drawn."""
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
    """The rows ``[row_start, row_start + len(keys))`` of one batch.

    Event time is the batch's arrival window plus a per-row jitter inside it, so
    it tracks batch order however the timestamp column is distributed.
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
# A row's identity and its event time are zigzag varints, so each costs what
# its magnitude costs. An identity carries its batch's block, so it is five
# bytes for every batch past the first; an event time carries milliseconds
# since the Unix epoch, so it is six bytes from mid-1970 — 2^34 ms in, where
# the zigzagged value first needs a sixth seven-bit group — until 2039. At
# batch zero and epoch zero both collapse to widths no written row has, and
# the payload budget would absorb the difference under a calibrated name. So
# the sample sits past the first identity block, at a wall-clock epoch whose
# exact value is immaterial.
CALIBRATION_BATCH = 1
CALIBRATION_EPOCH_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def realized_encoded_row_size(seed: int, payload_width: int, columns: tuple[ColumnDistribution, ...]) -> float:
    """Mean Avro bytes a row of these columns encodes to, over a fixed sample.

    The sample is fixed rather than drawn from the corpus so two column sets
    are compared on the same rows, and the size is the block's own arithmetic
    so a row is never measured by one rule and written under another.
    """
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
    """Payload width that lands the mean encoded row on ``target``.

    Column entropy is folded into the same byte budget rather than added on
    top: a schema whose other columns are wider simply leaves less payload,
    so the target row size stays comparable across schemas.
    """
    empty_mean = realized_encoded_row_size(seed, 0, columns)
    # An empty payload is the narrowest row the declared columns can encode, so
    # a target below it cannot be met by trimming: clamping the width to zero
    # would leave the corpus silently over budget under a calibrated name.
    if empty_mean > target:
        raise ValueError(
            f"the declared columns encode a mean row of {empty_mean:.2f} bytes with an empty payload, "
            f"which exceeds target_row_bytes {target}: widen the budget or narrow the columns"
        )
    # A payload cell is length-prefixed and the prefix is itself a varint, so a
    # row does not grow byte for byte with the payload: crossing a prefix
    # boundary puts the optimum a byte or two below where the linear estimate
    # says it is, and how far depends on the width a schema lands at. One
    # measurement recovers that offset, and the three widths around the
    # correction are then scored on measured rows — so the answer holds under
    # the encoder for a schema the tool has never seen, rather than under an
    # estimate calibrated against the ones it has.
    estimate = max(0, int(target - empty_mean))
    corrected = max(0, estimate - int(round(realized_encoded_row_size(seed, estimate, columns) - target)))
    candidates = range(max(0, corrected - 1), corrected + 2)
    return min(candidates, key=lambda width: abs(realized_encoded_row_size(seed, width, columns) - target))
