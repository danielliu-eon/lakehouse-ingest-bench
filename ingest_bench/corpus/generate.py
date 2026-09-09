"""Generate a corpus: batch files, key sidecars, manifest, partition truth, column stats, corpus.json.

The manifest is the frozen scoring input. Every figure in it is re-derived
from the stored bytes by `verify_batch` before the corpus is published, so the
scorer trusts the manifest without trusting the encoder that wrote it.
"""

from __future__ import annotations

import io
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import cast

import fastavro
import numpy as np

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import frames
from ingest_bench.corpus import values as v
from ingest_bench.corpus.preset import Preset, corpus_dir_name, corpus_hash, effective_dict
from ingest_bench.corpus.stats import (
    ColumnStats,
    column_stats_summary,
    expected_distinct_values,
    observe_block,
    stats_stride,
)

GENERATOR_VERSION = "5"
PARTITION_SHARE_MAX_DEVIATION = 0.05
# The share gate's floor is the coldest key's expected row count, not the
# corpus's total rows: the noise on a key's realized share falls with the rows
# behind that key alone, so the coldest key is what decides whether the
# comparison measures the generator or the seed. A flat row floor cannot serve
# both a wide key space and a narrow one — at any given corpus size it would
# gate 512 keys on sampling noise while leaving 8 keys unchecked. Ten thousand
# rows put the relative noise near 1%, which a 5% gate clears.
PARTITION_SHARE_GATE_MIN_COLD_ROWS = 10_000
MEAN_ROW_MAX_DEVIATION = 0.02
CARDINALITY_MAX_DEVIATION = 0.10
# A blob column asked for a fresh value per row is the corpus's incompressibility
# axis, so bytes that a codec could fold away mean the axis is not there — however
# the column is declared.
UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE = 7.5


@dataclass
class BatchRecord:
    """One batch's scoring ground truth, and where its bytes live.

    Exactness is scored against ``rows`` and ``checksum``, so the record is
    what the scorer compares the table to; the sha256 is what ties those
    figures to the bytes a producer will actually send.
    """

    batch: int
    offset_ms: int
    rows: int
    id_min: int
    id_max: int
    checksum: int
    encoded_bytes: int
    stored_bytes: int
    sha256: str
    uri: str
    key_uris: dict[str, str]

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, line: str) -> BatchRecord:
        raw = json.loads(line)
        return cls(**raw)


@dataclass
class BatchFill:
    """One batch's encoded rows, beside the truth accumulated while filling it."""

    records: bytes
    sizes: np.ndarray
    ids: np.ndarray
    keys: np.ndarray
    key_values: dict[str, list[str]]
    partition_counts: np.ndarray
    partition_sum_mod: np.ndarray
    partition_encoded: np.ndarray


def epoch_ms(preset: Preset) -> int:
    """The instant the corpus's first batch arrives, in milliseconds since the Unix epoch.

    A timestamp without an offset is refused rather than resolved in local time.
    The offset does not enter the corpus hash, so a naive epoch would let two
    machines in different zones write different event times under one corpus
    hash — each internally consistent, neither reproducing the other.
    """
    moment = datetime.fromisoformat(preset.corpus_epoch.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"corpus_epoch {preset.corpus_epoch!r} needs an explicit UTC offset or a trailing Z")
    return round(moment.timestamp() * 1000)


def fill_batch(
    seed: int,
    batch: int,
    preset: Preset,
    payload_width: int,
    cdf: np.ndarray,
    row_block: int,
    column_stats: dict[str, ColumnStats] | None,
    stride: int,
    rows_per_batch_estimate: int,
) -> BatchFill:
    """Whole row blocks until the batch holds its byte budget.

    The budget is met by overshooting rather than by trimming a block, so a
    batch's rows are always the rows the block generator produces at those
    positions and a regenerated batch is byte-identical.
    """
    start_ms = epoch_ms(preset) + batch * preset.batch_interval_ms
    chunks: list[bytes] = []
    sizes: list[np.ndarray] = []
    ids: list[np.ndarray] = []
    keys: list[np.ndarray] = []
    key_values: dict[str, list[str]] = {name: [] for name in preset.kafka_key_columns}
    counts = np.zeros(preset.partition_count, dtype=np.int64)
    sums = np.zeros(preset.partition_count, dtype=np.int64)
    encoded = np.zeros(preset.partition_count, dtype=np.int64)
    rows = 0
    total = 0
    while total < preset.batch_bytes:
        block_keys = v.draw_partition_keys(seed, batch, rows, row_block, cdf)
        block = v.build_row_block(
            seed, batch, rows, block_keys, payload_width, start_ms, preset.batch_interval_ms, preset.columns
        )
        chunks.append(block.avro_records_bytes())
        sizes.append(block.encoded_sizes)
        ids.append(block.ids)
        keys.append(block_keys)
        for name in key_values:
            key_values[name].extend(v.column_strings(block, name))
        counts += np.bincount(block_keys, minlength=preset.partition_count)
        # Residues are below P and a block holds at most row_block of them, so the
        # partial sums stay exact in int64 before the reduction.
        sums += np.bincount(block_keys, weights=block.ids % c.P, minlength=preset.partition_count).astype(np.int64)
        sums %= c.P
        encoded += np.bincount(block_keys, weights=block.encoded_sizes, minlength=preset.partition_count).astype(
            np.int64
        )
        if column_stats is not None:
            observe_block(block, column_stats, batch * rows_per_batch_estimate + rows, stride)
        rows += block.rows
        total += int(block.encoded_sizes.sum())
        # A batch owns one identity block, so a batch that outgrew it would hand
        # the next batch's identities out twice and exactness would score them
        # against a corpus that never existed.
        if rows >= c.ID_BLOCK:
            raise ValueError(f"batch {batch} exceeded its id block")
    return BatchFill(
        b"".join(chunks),
        np.concatenate(sizes),
        np.concatenate(ids),
        np.concatenate(keys),
        key_values,
        counts,
        sums,
        encoded,
    )


def closed_form_checksum(id_min: int, id_max: int, rows: int) -> int:
    return ((id_min + id_max) * rows // 2) % c.P


def verify_batch(
    record: BatchRecord,
    data: bytes,
    key_data: dict[str, bytes],
    columns: tuple[c.ColumnDistribution, ...],
    fill_partition_counts: dict[int, int],
) -> None:
    """Re-derive a batch's whole record from its stored bytes.

    Everything the scorer will compare a table to is recomputed here by a
    reader that shares nothing with the encoder: the frames are decoded under
    the published schema, so a manifest figure can only be right if a consumer
    reading the same bytes the same way would agree with it.
    """
    label = f"batch {record.batch}"
    if frames.sha256_hex(data) != record.sha256:
        raise AssertionError(f"{label}: stored bytes do not match the manifest sha256")
    schema = fastavro.parse_schema(c.avro_schema(columns))
    key_frames = {name: list(frames.iter_frames(frames.decompress(blob))) for name, blob in key_data.items()}
    # A producer reads a sidecar in lockstep with the batch, so a sidecar of the
    # wrong length is a corpus fault whatever its contents. Counting the frames
    # up front is also what makes the row walk below a comparison rather than an
    # unchecked index: a short sidecar would otherwise fail on the index and a
    # long one would never be looked at past the last row.
    for name, framed in key_frames.items():
        if len(framed) != record.rows:
            raise AssertionError(f"{label}: sidecar {name} holds {len(framed)} frames for {record.rows} rows")
    expected_id = record.id_min
    total = 0
    counts: dict[int, int] = {}
    decoded = 0
    for frame in frames.iter_frames(frames.decompress(data)):
        row = cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
        # The published schema types `id` as an Avro long, so a decoded row
        # carries an int; a row that somehow did not would fail the next check.
        row_id = cast(int, row["id"])
        if row_id != expected_id:
            raise AssertionError(f"{label}: id {row_id} where {expected_id} was expected")
        expected_id += 1
        total = (total + row_id) % c.P
        key = int(str(row["partition_key"])[1:])
        counts[key] = counts.get(key, 0) + 1
        for name, framed in key_frames.items():
            if framed[decoded].decode("utf-8") != str(row[name]):
                raise AssertionError(f"{label}: sidecar {name} disagrees with row {decoded}")
        decoded += 1
    if decoded != record.rows or expected_id != record.id_max + 1:
        raise AssertionError(f"{label}: decoded {decoded} rows, manifest says {record.rows}")
    if total != record.checksum:
        raise AssertionError(f"{label}: checksum mismatch")
    if fill_partition_counts and counts != fill_partition_counts:
        raise AssertionError(f"{label}: per-key counts differ from the generator's tally")


def generate(
    preset: Preset,
    out_uri: str,
    *,
    seed: int = 1,
    shard_index: int = 0,
    shard_count: int = 1,
    zstd_level: int = 3,
    row_block: int = 1024,
) -> dict[str, object]:
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must satisfy 0 <= shard_index < shard_count")
    corpus_uri = uri.join(out_uri, corpus_dir_name(preset))
    payload_width = v.calibrate_payload_width(seed, preset.target_row_bytes, preset.columns)
    calibrated_mean = v.realized_encoded_row_size(seed, payload_width, preset.columns)
    rows_per_batch_estimate = max(1, int(preset.batch_bytes / calibrated_mean))
    stride = stats_stride(rows_per_batch_estimate * preset.batch_count)
    cdf = v.zipf_cdf(preset.partition_count, preset.alpha)
    column_stats = {column.name: ColumnStats(column.name) for column in preset.columns}
    records: list[BatchRecord] = []
    partition_rows = np.zeros(preset.partition_count, dtype=np.int64)
    partition_sum = np.zeros(preset.partition_count, dtype=np.int64)
    partition_encoded = np.zeros(preset.partition_count, dtype=np.int64)
    for batch in range(shard_index, preset.batch_count, shard_count):
        fill = fill_batch(
            seed, batch, preset, payload_width, cdf, row_block, column_stats, stride, rows_per_batch_estimate
        )
        stored = frames.compress(frames.frame_stream(fill.records, fill.sizes), zstd_level)
        rel = f"batches/{frames.batch_file_name(batch)}"
        uri.write_bytes(uri.join(corpus_uri, rel), stored)
        key_uris: dict[str, str] = {}
        key_blobs: dict[str, bytes] = {}
        for name, key_column_values in fill.key_values.items():
            blob = frames.compress(frames.string_frames(key_column_values), zstd_level)
            key_rel = f"batches/{frames.key_file_name(batch, name)}"
            uri.write_bytes(uri.join(corpus_uri, key_rel), blob)
            key_uris[name] = key_rel
            key_blobs[name] = blob
        rows = int(fill.ids.size)
        record = BatchRecord(
            batch=batch,
            offset_ms=batch * preset.batch_interval_ms,
            rows=rows,
            id_min=int(fill.ids[0]),
            id_max=int(fill.ids[-1]),
            checksum=closed_form_checksum(int(fill.ids[0]), int(fill.ids[-1]), rows),
            encoded_bytes=int(fill.sizes.sum()),
            stored_bytes=len(stored),
            sha256=frames.sha256_hex(stored),
            uri=rel,
            key_uris=key_uris,
        )
        # The closed form assumes the batch's identities are dense, and the
        # accumulated residues do not; they agree only if they are.
        if int(fill.partition_sum_mod.sum() % c.P) != record.checksum:
            raise AssertionError(f"batch {batch}: closed-form checksum disagrees with the accumulated residues")
        counts = {key: int(count) for key, count in enumerate(fill.partition_counts.tolist()) if count}
        verify_batch(record, stored, key_blobs, preset.columns, counts)
        records.append(record)
        partition_rows += fill.partition_counts
        partition_sum = (partition_sum + fill.partition_sum_mod) % c.P
        partition_encoded += fill.partition_encoded
    manifest_text = "".join(record.to_json() + "\n" for record in records)
    uri.write_text(uri.join(corpus_uri, "manifest.jsonl"), manifest_text)
    uri.write_text(uri.join(corpus_uri, "schema.avsc"), json.dumps(c.avro_schema(preset.columns), indent=2))
    truth = {
        v.partition_label(key): {
            "rows": int(partition_rows[key]),
            "sum_mod": int(partition_sum[key]),
            "encoded_bytes": int(partition_encoded[key]),
        }
        for key in range(preset.partition_count)
    }
    uri.write_text(uri.join(corpus_uri, "partition_truth.json"), json.dumps(truth, indent=2))
    uri.write_text(uri.join(corpus_uri, "column_stats.json"), dump_column_stats(column_stats))
    meta = finalize_corpus_json(
        preset,
        seed,
        records,
        truth,
        column_stats,
        stride,
        payload_width,
        row_block,
        zstd_level,
        shard_index,
        shard_count,
    )
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(meta, indent=2, sort_keys=True))
    return meta


def dump_column_stats(column_stats: dict[str, ColumnStats]) -> str:
    """The sampled column statistics in the form a merge can re-derive them from.

    `corpus.json` publishes derived figures — a distinct-value estimate, an
    entropy — and those cannot be re-merged: a union of shard sketches is the
    corpus's sketch, while a union of per-shard estimates is nothing. So the
    sampler's own state travels beside them, which is what lets a merge apply
    the corpus-wide gates a shard cannot judge from its own batches.

    Written compactly rather than indented like its neighbours: a sketch is a
    list of opaque digests, so there is nothing in here for a reader.
    """
    document = {name: stats.to_dict() for name, stats in column_stats.items()}
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def load_column_stats(document: str) -> dict[str, ColumnStats]:
    raw = cast(dict[str, object], json.loads(document))
    return {name: ColumnStats.from_dict(cast(dict[str, object], entry)) for name, entry in raw.items()}


def partition_weights(preset: Preset) -> np.ndarray:
    """The byte share each partition key is asked for, normalized over the key space."""
    weights = np.arange(1, preset.partition_count + 1, dtype=np.float64) ** -preset.alpha
    return weights / weights.sum()


def partition_share_deviation(preset: Preset, truth: dict[str, dict[str, int]]) -> float | None:
    """How far the realized byte share of the worst key is from its Zipf weight.

    Bytes rather than rows, because what a skewed key costs an engine is the
    data it has to write for it, and a row-share match would hide a key whose
    rows are systematically narrower.
    """
    total = sum(entry["encoded_bytes"] for entry in truth.values())
    if total == 0:
        return None
    weights = partition_weights(preset)
    realized = (
        np.array(
            [truth[v.partition_label(key)]["encoded_bytes"] for key in range(preset.partition_count)], dtype=np.float64
        )
        / total
    )
    return float(np.max(np.abs(realized - weights) / weights))


def finalize_corpus_json(
    preset: Preset,
    seed: int,
    records: list[BatchRecord],
    truth: dict[str, dict[str, int]],
    column_stats: dict[str, ColumnStats],
    stride: int,
    payload_width: int,
    row_block: int,
    zstd_level: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, object]:
    """Everything a consumer needs to know about the corpus, and the gates it passed.

    The gates run here rather than in a checker of their own because a corpus
    that failed one must not exist to be picked up: a run scored against a
    corpus whose skew or row width is not what it publishes reports a number
    about a workload nobody asked for. A shard cannot judge a corpus-wide
    statistic from its own batches, so the gates hold for the unsharded run,
    and merging every shard is what applies them to a sharded one.
    """
    row_count = sum(record.rows for record in records)
    encoded = sum(record.encoded_bytes for record in records)
    mean_row = encoded / row_count if row_count else 0.0
    mean_dev = (mean_row - preset.target_row_bytes) / preset.target_row_bytes
    if shard_count == 1 and abs(mean_dev) > MEAN_ROW_MAX_DEVIATION:
        raise ValueError(
            f"mean encoded row {mean_row:.2f} deviates {mean_dev:+.2%} from target {preset.target_row_bytes}"
        )
    share_dev = partition_share_deviation(preset, truth) if shard_count == 1 else None
    cold_rows = row_count * float(partition_weights(preset).min())
    share_gate_enforced = share_dev is not None and preset.alpha > 0 and cold_rows >= PARTITION_SHARE_GATE_MIN_COLD_ROWS
    if share_dev is not None and share_gate_enforced and share_dev > PARTITION_SHARE_MAX_DEVIATION:
        raise ValueError(f"partition byte share deviates {share_dev:.2%} from the Zipf weights")
    # Under no skew the shares carry no information, so what is left to check is
    # that the key space is covered at all.
    if shard_count == 1 and any(entry["rows"] == 0 for entry in truth.values()):
        raise ValueError("a partition key received no rows")
    sampled = max((stats.sampled_rows for stats in column_stats.values()), default=0)
    cardinality_devs: dict[str, float] = {}
    summaries: dict[str, object] = {}
    for column in preset.columns:
        stats = column_stats[column.name]
        expected = expected_distinct_values(column, sampled)
        dev = None if expected is None else abs(stats.distinct_values() - expected) / expected
        if dev is not None:
            cardinality_devs[column.name] = dev
        summaries[column.name] = column_stats_summary(column, stats, dev)
    max_card_dev = max(cardinality_devs.values(), default=None)
    if shard_count == 1 and max_card_dev is not None and max_card_dev > CARDINALITY_MAX_DEVIATION:
        raise ValueError(f"realized column cardinality deviates {max_card_dev:.2%} from the declaration")
    # The floor over the unbounded blobs rather than a per-column check, because
    # one compressible payload is enough to cost the corpus the axis; a schema
    # with no unbounded blob has nothing to judge and yields None.
    entropies = [
        column_stats[column.name].value_byte_entropy_bits_per_byte()
        for column in preset.columns
        if column.kind == c.KIND_BLOB and column.cardinality == c.UNBOUNDED_CARDINALITY
    ]
    blob_entropy = min(entropies) if entropies else None
    if shard_count == 1 and blob_entropy is not None and blob_entropy < UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE:
        raise ValueError(
            f"unbounded blob column entropy {blob_entropy:.2f} bits/byte is below "
            f"{UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE}"
        )
    manifest_sha = frames.sha256_hex("".join(record.to_json() + "\n" for record in records).encode())
    return {
        "name": preset.name,
        "corpus_hash": corpus_hash(preset),
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "effective_preset": effective_dict(preset),
        "schema": c.avro_schema(preset.columns),
        "schema_name": preset.schema_name,
        "column_roles": {
            role: c.role_column(preset.columns, role).name
            for role in (c.ROLE_EVENT_TIME, c.ROLE_ENTITY, c.ROLE_SUM_MEASURE, c.ROLE_PAYLOAD)
        },
        "iceberg_types": {
            field["name"]: c.iceberg_type_name(field["type"]) for field in c.schema_fields(preset.columns)
        },
        "key_columns": list(preset.kafka_key_columns),
        "p": c.P,
        "id_block": c.ID_BLOCK,
        "batch_count": preset.batch_count,
        "batch_interval_ms": preset.batch_interval_ms,
        "offered_bytes_per_s": preset.offered_bytes_per_s,
        "duration_s": preset.duration_s,
        "row_count": row_count,
        "encoded_bytes": encoded,
        "stored_bytes": sum(record.stored_bytes for record in records),
        "rows_per_s": row_count / preset.duration_s if shard_count == 1 else None,
        "mean_encoded_row_size": mean_row,
        "mean_encoded_row_size_relative_deviation": mean_dev,
        "payload_width": payload_width,
        "row_block": row_block,
        "partition_rows": {key: entry["rows"] for key, entry in truth.items()},
        "partition_sum_mod": {key: entry["sum_mod"] for key, entry in truth.items()},
        "partition_encoded_bytes": {key: entry["encoded_bytes"] for key, entry in truth.items()},
        "partition_byte_share_max_relative_deviation": share_dev,
        "partition_byte_share_gate_enforced": share_gate_enforced,
        "unbounded_blob_min_entropy_bits_per_byte": blob_entropy,
        "column_stats": summaries,
        "column_stats_stride": stride,
        "column_cardinality_max_relative_deviation": max_card_dev,
        "column_cardinality_checked_columns": len(cardinality_devs),
        "zstd_level": zstd_level,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "manifest_sha256": manifest_sha,
    }
