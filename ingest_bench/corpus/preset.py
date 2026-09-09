# SPDX-License-Identifier: Apache-2.0
"""A preset is the workload definition: a schema plus the shape of the stream.

Everything downstream renders from the corpus this produces, so the preset is
hashed in full and the hash names the corpus directory: two shapes can never
share a directory, and `corpus.json` is the only authority for what was built.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.corpus import columns as c

KEYS = (
    "schema",
    "offered_bytes_per_s",
    "duration_s",
    "partition_count",
    "alpha",
    "target_row_bytes",
    "batch_interval_ms",
    "corpus_epoch",
    "kafka_key_columns",
    "column_overrides",
)

_QUANTITY = re.compile(r"^\s*(\d+)\s*([KMGT]?i?B)?\s*$")
_DECIMAL = {"B": 1, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12}
_BINARY = {"KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def parse_quantity(text: str | int) -> int:
    if isinstance(text, int):
        return text
    match = _QUANTITY.match(text)
    if not match:
        raise ValueError(f"not a byte quantity: {text!r}")
    number, unit = int(match.group(1)), match.group(2) or "B"
    scale = _DECIMAL.get(unit) or _BINARY.get(unit)
    if scale is None:
        raise ValueError(f"unknown unit in {text!r}")
    return number * scale


@dataclass(frozen=True)
class Preset:
    name: str
    schema_name: str
    offered_bytes_per_s: int
    duration_s: int
    partition_count: int
    alpha: float
    target_row_bytes: int
    batch_interval_ms: int
    corpus_epoch: str
    kafka_key_columns: tuple[str, ...]
    column_overrides: dict[str, dict[str, object]]
    columns: tuple[c.ColumnDistribution, ...]

    @property
    def batch_count(self) -> int:
        total_ms = self.duration_s * 1000
        if total_ms % self.batch_interval_ms:
            raise ValueError("duration_s * 1000 must be a multiple of batch_interval_ms")
        return total_ms // self.batch_interval_ms

    @property
    def batch_bytes(self) -> int:
        return self.offered_bytes_per_s * self.batch_interval_ms // 1000


def _as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def _as_float_value(value: object) -> float:
    return float(cast(int | float | str, value))


def _apply_override(raw: dict[str, object], assignment: str) -> None:
    key, sep, value = assignment.partition("=")
    if not sep:
        raise ValueError(f"--set expects key=value, got {assignment!r}")
    parsed: object = yaml.safe_load(value)
    if key.startswith("column_overrides."):
        _, column, attribute = key.split(".", 2)
        overrides = cast(dict[str, dict[str, object]], raw.setdefault("column_overrides", {}))
        overrides.setdefault(column, {})[attribute] = parsed
        return
    if key not in KEYS:
        raise ValueError(f"unknown preset key {key!r}; known keys: {', '.join(KEYS)}")
    raw[key] = parsed


def resolve_preset_path(source: str, workloads_dir: Path) -> Path:
    path = Path(source)
    if path.suffix in (".yaml", ".yml") and path.exists():
        return path
    candidate = workloads_dir / "presets" / f"{source}.yaml"
    if not candidate.exists():
        raise FileNotFoundError(f"no preset {source!r}: not a file and {candidate} does not exist")
    return candidate


def load_preset(source: str, *, workloads_dir: Path, overrides: Sequence[str] = ()) -> Preset:
    path = resolve_preset_path(source, workloads_dir)
    raw = cast(dict[str, object], yaml.safe_load(path.read_text()))
    unknown = sorted(set(raw) - set(KEYS))
    if unknown:
        raise ValueError(f"{path}: unknown preset key(s): {', '.join(unknown)}")
    for assignment in overrides:
        _apply_override(raw, assignment)
    missing = [key for key in KEYS if key not in raw]
    if missing:
        raise ValueError(f"{path}: missing preset key(s): {', '.join(missing)}")
    schema_name = str(raw["schema"])
    columns = c.load_schema(workloads_dir / "schemas" / f"{schema_name}.json")
    column_overrides = cast(dict[str, dict[str, object]], raw["column_overrides"])
    columns = c.apply_column_overrides(columns, column_overrides)
    key_columns = tuple(str(k) for k in cast(list[object], raw["kafka_key_columns"]))
    by_name = {column.name: column for column in columns}
    bad = [k for k in key_columns if k not in by_name]
    if bad:
        raise ValueError(f"kafka_key_columns name unknown column(s): {', '.join(bad)}")
    # A Kafka key is UTF-8 text, so a categorical column is the only kind that can
    # supply one. `partition_key` is the sole exception: it is a reserved string
    # column, computed rather than drawn, so it declares no value kind of its own.
    # `id` is reserved too but is a long, which is why the reserved kind cannot
    # stand in for the check.
    non_string = [k for k in key_columns if k != "partition_key" and by_name[k].kind != c.KIND_CATEGORICAL]
    if non_string:
        raise ValueError(f"kafka_key_columns must be string columns: {', '.join(non_string)}")
    preset = Preset(
        name=path.stem,
        schema_name=schema_name,
        offered_bytes_per_s=parse_quantity(cast(str | int, raw["offered_bytes_per_s"])),
        duration_s=int(cast(int, raw["duration_s"])),
        partition_count=int(cast(int, raw["partition_count"])),
        alpha=float(cast(float, raw["alpha"])),
        target_row_bytes=int(cast(int, raw["target_row_bytes"])),
        batch_interval_ms=int(cast(int, raw["batch_interval_ms"])),
        corpus_epoch=str(raw["corpus_epoch"]),
        kafka_key_columns=key_columns,
        column_overrides=column_overrides,
        columns=columns,
    )
    if (
        preset.partition_count < 1
        or preset.duration_s < 1
        or preset.batch_interval_ms < 1
        or preset.offered_bytes_per_s < 1
    ):
        raise ValueError("partition_count, duration_s, batch_interval_ms and offered_bytes_per_s must be positive")
    preset.batch_count  # noqa: B018 - validates divisibility at load time
    # The Iceberg partition column is written from a sidecar keyed by the Kafka
    # key, so the key columns always have to include it for that sidecar to exist.
    if "partition_key" not in preset.kafka_key_columns:
        preset = replace(preset, kafka_key_columns=(*preset.kafka_key_columns, "partition_key"))
    return preset


def effective_dict(preset: Preset) -> dict[str, object]:
    data = asdict(preset)
    data["columns"] = [asdict(column) for column in preset.columns]
    return data


def canonical_json(data: dict[str, object]) -> str:
    """The one rendering a preset is compared and hashed by, so key order cannot change either."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def corpus_hash(preset: Preset) -> str:
    return hashlib.sha256(canonical_json(effective_dict(preset)).encode()).hexdigest()[:8]


def preset_from_effective(effective: dict[str, object], schema_name: str) -> Preset:
    """The preset a corpus was generated from, rebuilt from its published effective form.

    Merging shard corpora has to gate the whole the way a single pass would,
    and the gates read the preset rather than the corpus, so the preset has to
    come back out of `corpus.json` — a schema file on the merging machine
    would be a second source of truth for a corpus that already exists.

    The rebuilt preset is required to reproduce the effective form it was read
    from: anything the round trip dropped would gate the merged corpus against
    a workload nobody generated, and the corpus hash would no longer name it.
    """
    if str(effective["schema_name"]) != schema_name:
        raise ValueError(f"the effective preset names schema {effective['schema_name']!r}, not {schema_name!r}")
    declared = [cast(dict[str, object], entry) for entry in cast(list[object], effective["columns"])]
    preset = Preset(
        name=str(effective["name"]),
        schema_name=schema_name,
        offered_bytes_per_s=_as_int_value(effective["offered_bytes_per_s"]),
        duration_s=_as_int_value(effective["duration_s"]),
        partition_count=_as_int_value(effective["partition_count"]),
        alpha=_as_float_value(effective["alpha"]),
        target_row_bytes=_as_int_value(effective["target_row_bytes"]),
        batch_interval_ms=_as_int_value(effective["batch_interval_ms"]),
        corpus_epoch=str(effective["corpus_epoch"]),
        kafka_key_columns=tuple(str(name) for name in cast(list[object], effective["kafka_key_columns"])),
        column_overrides=cast(dict[str, dict[str, object]], effective["column_overrides"]),
        # The reserved columns lead every schema and carry no declaration, so
        # `build_columns` is what puts them back rather than the published form.
        columns=c.build_columns(
            tuple(c.column_from_dict(entry) for entry in declared if str(entry["kind"]) != c.KIND_RESERVED)
        ),
    )
    if canonical_json(effective_dict(preset)) != canonical_json(effective):
        raise ValueError(
            f"the preset rebuilt for {preset.name} does not reproduce the effective preset it was read from"
        )
    return preset


def corpus_dir_name_for(name: str, hash_value: str) -> str:
    """The corpus directory rule, for a caller holding only what `corpus.json` published."""
    return f"{name}-{hash_value}"


def corpus_dir_name(preset: Preset) -> str:
    return corpus_dir_name_for(preset.name, corpus_hash(preset))
