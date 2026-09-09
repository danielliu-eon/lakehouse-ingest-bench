# SPDX-License-Identifier: Apache-2.0
"""Merge shard corpora into one corpus: metadata beside the per-shard data.

Generating a large corpus is split across machines, and a shard sees only the
batches it owns — so it can publish neither a corpus-wide figure nor the gates
that decide whether the corpus may exist. This is where the whole is assembled
and judged: the merged `corpus.json` is written only if the whole passes what a
single-pass corpus has to pass, so a run can never be scored against a corpus
that only looked right one shard at a time.

The batch files stay where they were written. Copying them would double the
bytes of the largest artifact the benchmark produces to no end, so the merged
manifest addresses each batch in the shard that wrote it.
"""

from __future__ import annotations

import json
from typing import cast

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import values as v
from ingest_bench.corpus.generate import (
    BatchRecord,
    dump_column_stats,
    finalize_corpus_json,
    load_column_stats,
)
from ingest_bench.corpus.preset import Preset, corpus_dir_name, corpus_hash, preset_from_effective
from ingest_bench.corpus.stats import ColumnStats

# What every shard of one corpus must agree on: the workload, the value stream
# that filled it, and the encoding of the bytes on disk. A disagreement here
# means the shards were generated from different runs and their batches do not
# belong to one corpus, whatever their indices say.
AGREED_KEYS = ("corpus_hash", "seed", "generator_version", "payload_width", "row_block", "zstd_level")

TRUTH_FIELDS = ("rows", "sum_mod", "encoded_bytes")


def _as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def _read_partition_truth(shard_uri: str) -> dict[str, dict[str, int]]:
    raw = cast(dict[str, object], json.loads(uri.read_text(uri.join(shard_uri, "partition_truth.json"))))
    return {
        str(label): {field: _as_int_value(cast(dict[str, object], entry)[field]) for field in TRUTH_FIELDS}
        for label, entry in raw.items()
    }


def _sum_partition_truth(shard_uris: list[str], preset: Preset) -> dict[str, dict[str, int]]:
    """The corpus's partition truth, over the key space the preset declares.

    Summing over the declared labels rather than over the labels a shard
    happens to publish is what makes a shard generated for a different key
    space fail here instead of contributing a partial column of the truth.
    """
    labels = [v.partition_label(key) for key in range(preset.partition_count)]
    truth = {label: {"rows": 0, "sum_mod": 0, "encoded_bytes": 0} for label in labels}
    for shard_uri in shard_uris:
        shard_truth = _read_partition_truth(shard_uri)
        if sorted(shard_truth) != sorted(labels):
            raise ValueError(f"{shard_uri} publishes {len(shard_truth)} partition keys, not {len(labels)}")
        for label in labels:
            total, entry = truth[label], shard_truth[label]
            total["rows"] += entry["rows"]
            total["encoded_bytes"] += entry["encoded_bytes"]
            # The residues are what make the identity sum splittable at all:
            # they add across shards the way they add across batches.
            total["sum_mod"] = (total["sum_mod"] + entry["sum_mod"]) % c.P
    return truth


def _merge_column_stats(shard_uris: list[str], preset: Preset) -> dict[str, ColumnStats]:
    """The corpus's sampled column statistics, unioned over the shards.

    The sampler strides over corpus-wide row positions, so a shard samples the
    rows the unsharded corpus would have sampled from the batches it owns —
    which is what makes this union the whole corpus's sample rather than an
    approximation of it.
    """
    merged = {column.name: ColumnStats(column.name) for column in preset.columns}
    for shard_uri in shard_uris:
        shard_stats = load_column_stats(uri.read_text(uri.join(shard_uri, "column_stats.json")))
        for name, stats in merged.items():
            stats.merge(shard_stats[name])
    return merged


def merge(shard_uris: list[str], out_uri: str) -> dict[str, object]:
    """Assemble the shards under ``out_uri`` and return the merged `corpus.json`."""
    if not shard_uris:
        raise ValueError("merging needs at least one shard corpus")
    shards = [cast(dict[str, object], json.loads(uri.read_text(uri.join(s, "corpus.json")))) for s in shard_uris]
    first = shards[0]
    for shard in shards[1:]:
        for key in AGREED_KEYS:
            if shard[key] != first[key]:
                raise ValueError(f"shards disagree on {key}: {shard[key]!r} != {first[key]!r}")
    indices = sorted(_as_int_value(shard["shard_index"]) for shard in shards)
    if indices != list(range(_as_int_value(first["shard_count"]))):
        raise ValueError(f"expected shards 0..{_as_int_value(first['shard_count']) - 1}, got {indices}")
    preset = preset_from_effective(cast(dict[str, object], first["effective_preset"]), str(first["schema_name"]))
    if corpus_hash(preset) != str(first["corpus_hash"]):
        raise ValueError(f"the shards publish corpus hash {first['corpus_hash']!r}, and their preset hashes to another")

    records: list[BatchRecord] = []
    for shard_uri in shard_uris:
        for line in uri.read_text(uri.join(shard_uri, "manifest.jsonl")).splitlines():
            record = BatchRecord.from_json(line)
            record.uri = uri.join(shard_uri, record.uri)
            record.key_uris = {name: uri.join(shard_uri, reference) for name, reference in record.key_uris.items()}
            records.append(record)
    records.sort(key=lambda record: record.batch)
    if [record.batch for record in records] != list(range(preset.batch_count)):
        raise ValueError(
            f"the shards hold {len(records)} batches covering {len({r.batch for r in records})} distinct indices, "
            f"and the preset asks for batches 0..{preset.batch_count - 1}"
        )

    truth = _sum_partition_truth(shard_uris, preset)
    column_stats = _merge_column_stats(shard_uris, preset)
    # The gates run before anything is written, so a corpus that failed one
    # never exists to be picked up by a run.
    meta = finalize_corpus_json(
        preset,
        _as_int_value(first["seed"]),
        records,
        truth,
        column_stats,
        _as_int_value(first["column_stats_stride"]),
        _as_int_value(first["payload_width"]),
        _as_int_value(first["row_block"]),
        _as_int_value(first["zstd_level"]),
        shard_index=0,
        shard_count=1,
    )
    corpus_uri = uri.join(out_uri, corpus_dir_name(preset))
    uri.write_text(uri.join(corpus_uri, "manifest.jsonl"), "".join(record.to_json() + "\n" for record in records))
    uri.write_text(uri.join(corpus_uri, "schema.avsc"), uri.read_text(uri.join(shard_uris[0], "schema.avsc")))
    uri.write_text(uri.join(corpus_uri, "partition_truth.json"), json.dumps(truth, indent=2))
    uri.write_text(uri.join(corpus_uri, "column_stats.json"), dump_column_stats(column_stats))
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(meta, indent=2, sort_keys=True))
    return meta
