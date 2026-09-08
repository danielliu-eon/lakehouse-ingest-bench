"""What the producer actually published, batch by batch.

Freshness is measured from when a batch finished being published, not from when
it was due: a producer that fell behind must not be scored as engine lag. The
publish log is that record, written as it goes so a run that dies still says
how far it got, and read back by the scorer as the offered side of every
figure it computes.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from ingest_bench import uri

LOG_NAME_PREFIX = "publish_log-"
LOG_NAME_SUFFIX = ".jsonl"

_DONE_KEY = "done"


@dataclass(frozen=True)
class PublishRecord:
    """One batch's publish outcome, as the scorer reads it.

    ``last_ack_ms`` is the batch's emit time: the batch is not offered until
    its final row is acknowledged, so committing anything of it earlier would
    be committing rows the broker had not yet confirmed.
    """

    batch: int
    scheduled_ms: int
    first_ack_ms: int
    last_ack_ms: int
    rows: int
    bytes: int
    errors: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def _record_from_json(raw: dict[str, object]) -> PublishRecord:
    return PublishRecord(
        batch=int(cast(int, raw["batch"])),
        scheduled_ms=int(cast(int, raw["scheduled_ms"])),
        first_ack_ms=int(cast(int, raw["first_ack_ms"])),
        last_ack_ms=int(cast(int, raw["last_ack_ms"])),
        rows=int(cast(int, raw["rows"])),
        bytes=int(cast(int, raw["bytes"])),
        errors=int(cast(int, raw["errors"])),
    )


def _parse(text: str, source: str) -> list[PublishRecord]:
    """Every batch record in ``text``, with the trailer left out.

    The trailer says the shard finished rather than what it published, so it is
    not a record; a reader that turned it into one would count a batch that
    does not exist.
    """
    records = []
    for line in text.splitlines():
        if not line.strip():
            continue
        raw = cast(dict[str, object], json.loads(line))
        if _DONE_KEY in raw:
            continue
        try:
            records.append(_record_from_json(raw))
        except KeyError as err:
            raise ValueError(f"{source} holds a line missing key {err.args[0]!r}: {line}") from err
    return records


def append(path: Path, record: PublishRecord) -> None:
    """Add one record, flushed, so a killed producer still published its history."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record.to_json() + "\n")
        handle.flush()


def append_done(path: Path, shard: int, batches: int) -> None:
    """Mark the shard finished, after its last batch.

    A shard that stops early and one that has nothing left to send both go
    quiet, and the scorer has to tell them apart: without the trailer a run
    that died at half its batches would look like a run still in flight.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({_DONE_KEY: True, "shard": shard, "batches": batches}, sort_keys=True) + "\n")
        handle.flush()


def read(path: Path) -> list[PublishRecord]:
    return _parse(path.read_text(encoding="utf-8"), str(path))


def shard_done(path: Path) -> bool:
    """Whether the shard that owns ``path`` published everything it selected.

    An absent log is not a finished one: the scorer polls while shards run, and
    a log that has not been written or uploaded yet reads as still in flight.
    """
    if not path.exists():
        return False
    for line in reversed(path.read_text(encoding="utf-8").splitlines()):
        if line.strip():
            return _DONE_KEY in cast(dict[str, object], json.loads(line))
    return False


def log_names(uri_prefix: str) -> list[str]:
    return [
        name for name in uri.listdir(uri_prefix) if name.startswith(LOG_NAME_PREFIX) and name.endswith(LOG_NAME_SUFFIX)
    ]


def read_all(uri_prefix: str) -> list[PublishRecord]:
    """Every shard's records under ``uri_prefix``, merged into one send order.

    A batch belongs to exactly one shard, so the same batch appearing twice
    means two producers were sent the same work — the offered figures would
    double-count it, and every rate derived from them would be wrong. That is
    refused rather than deduplicated.
    """
    # A prefix with nothing under it is the normal state before the first
    # upload lands, not a missing input.
    if not uri.exists(uri_prefix):
        return []
    records: list[PublishRecord] = []
    owner: dict[int, str] = {}
    for name in log_names(uri_prefix):
        log_uri = uri.join(uri_prefix, name)
        for record in _parse(uri.read_text(log_uri), log_uri):
            if record.batch in owner:
                raise ValueError(
                    f"batch {record.batch} is duplicate: published in both {owner[record.batch]} and {name}"
                )
            owner[record.batch] = name
            records.append(record)
    records.sort(key=lambda record: record.batch)
    return records


def behind_ms(records: list[PublishRecord]) -> int:
    """How far the worst batch's first acknowledgement fell behind its due time.

    This is the producer's own lag, and the scorer's gate against reporting it
    as the engine's: a run where the producer could not keep up says nothing
    about how fresh the engine kept the table.
    """
    return max((record.first_ack_ms - record.scheduled_ms for record in records), default=0)


def emit_times(records: list[PublishRecord]) -> dict[int, int]:
    return {record.batch: record.last_ack_ms for record in records}
