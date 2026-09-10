# SPDX-License-Identifier: Apache-2.0
"""Read published corpus metadata and batch manifests.

Consumers use the recorded corpus facts instead of re-deriving them from a
preset that may have changed since generation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast

from ingest_bench import uri
from ingest_bench.corpus.generate import GENERATOR_VERSION, BatchRecord


def _as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def _as_float_value(value: object) -> float:
    return float(cast(int | float | str, value))


def _as_string_map(value: object) -> dict[str, str]:
    return {str(key): str(entry) for key, entry in cast(dict[str, object], value).items()}


@dataclass(frozen=True)
class CorpusMetadata:
    """Published corpus facts and their source URI."""

    uri: str
    name: str
    corpus_hash: str
    schema: dict[str, object]
    schema_name: str
    column_roles: dict[str, str]
    iceberg_types: dict[str, str]
    key_columns: tuple[str, ...]
    p: int
    id_block: int
    batch_count: int
    batch_interval_ms: int
    row_count: int
    offered_bytes_per_s: int
    duration_s: int
    rows_per_s: float
    generator_version: str
    corpus_epoch: str

    def field_names(self) -> list[str]:
        """The corpus columns in schema order, which is the order a row encodes in."""
        return [str(cast(dict[str, object], field)["name"]) for field in cast(list[object], self.schema["fields"])]

    def avro_schema_json(self) -> str:
        return json.dumps(self.schema)

    def role(self, name: str) -> str:
        """The column holding the ``name`` role, so a query names a role rather than a column."""
        return self.column_roles[name]


def read(corpus_uri: str) -> CorpusMetadata:
    """Read a complete corpus; reject shards until they have been merged."""
    raw = cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_uri, "corpus.json"))))
    try:
        version = str(raw["generator_version"])
        if version != GENERATOR_VERSION:
            raise ValueError(
                f"{corpus_uri} was written by generator version {version}, and this build speaks "
                f"version {GENERATOR_VERSION}; regenerate it with `gen-corpus`, which names a corpus "
                "after its preset and so writes this same path"
            )
        shard_count = _as_int_value(raw["shard_count"])
        if shard_count != 1:
            raise ValueError(
                f"{corpus_uri} is shard {_as_int_value(raw['shard_index'])} of {shard_count}, not a whole corpus; "
                "merge the shards first"
            )
        effective = cast(dict[str, object], raw["effective_preset"])
        return CorpusMetadata(
            uri=corpus_uri,
            name=str(raw["name"]),
            corpus_hash=str(raw["corpus_hash"]),
            schema=cast(dict[str, object], raw["schema"]),
            schema_name=str(raw["schema_name"]),
            column_roles=_as_string_map(raw["column_roles"]),
            iceberg_types=_as_string_map(raw["iceberg_types"]),
            key_columns=tuple(str(name) for name in cast(list[object], raw["key_columns"])),
            p=_as_int_value(raw["p"]),
            id_block=_as_int_value(raw["id_block"]),
            batch_count=_as_int_value(raw["batch_count"]),
            batch_interval_ms=_as_int_value(raw["batch_interval_ms"]),
            row_count=_as_int_value(raw["row_count"]),
            offered_bytes_per_s=_as_int_value(raw["offered_bytes_per_s"]),
            duration_s=_as_int_value(raw["duration_s"]),
            rows_per_s=_as_float_value(raw["rows_per_s"]),
            generator_version=version,
            corpus_epoch=str(effective["corpus_epoch"]),
        )
    except KeyError as err:
        raise ValueError(f"{corpus_uri}/corpus.json is missing key {err.args[0]!r}") from err


def _absolute(corpus_uri: str, reference: str) -> str:
    """Resolve relative manifest paths, preserving existing absolute shard URIs."""
    if reference.startswith("/") or "://" in reference:
        return reference
    return uri.join(corpus_uri, reference)


def read_manifest(corpus_uri: str) -> list[BatchRecord]:
    """Read batches in send order with absolute paths.

    Reject gaps and duplicates before they can affect engine scoring.
    """
    meta = read(corpus_uri)
    records = [
        BatchRecord.from_json(line)
        for line in uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
        if line.strip()
    ]
    records.sort(key=lambda record: record.batch)
    held = [record.batch for record in records]
    if held != list(range(meta.batch_count)):
        raise ValueError(
            f"{corpus_uri} publishes batches 0..{meta.batch_count - 1}, and its manifest holds {len(held)} "
            f"records covering {len(set(held))} distinct batches"
        )
    for record in records:
        record.uri = _absolute(corpus_uri, record.uri)
        record.key_uris = {name: _absolute(corpus_uri, reference) for name, reference in record.key_uris.items()}
    return records
