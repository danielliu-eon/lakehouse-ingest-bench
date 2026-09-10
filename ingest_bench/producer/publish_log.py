# SPDX-License-Identifier: Apache-2.0
"""Record producer acknowledgements and completion, batch by batch.

The scorer uses these logs to distinguish producer delay from engine lag.
Incremental writes preserve progress if the producer stops unexpectedly.
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
    """One batch's delivery outcome.

    Use the last acknowledgement as the batch emit time for scoring.
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
    """Parse batch records, excluding completion trailers."""
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
    """Append and flush a batch record to preserve incremental progress."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record.to_json() + "\n")
        handle.flush()


def append_done(path: Path, shard: int, batches: int) -> None:
    """Append a completion trailer after the shard's final batch.

    The trailer distinguishes completion from a producer that stopped early.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({_DONE_KEY: True, "shard": shard, "batches": batches}, sort_keys=True) + "\n")
        handle.flush()


def read(path: Path) -> list[PublishRecord]:
    return _parse(path.read_text(encoding="utf-8"), str(path))


def _trailed(text: str) -> bool:
    """Whether a log's last line is the trailer rather than a batch."""
    for line in reversed(text.splitlines()):
        if line.strip():
            return _DONE_KEY in cast(dict[str, object], json.loads(line))
    return False


def shard_done(path: Path) -> bool:
    """Return whether the log has a completion trailer; absent logs are unfinished."""
    if not path.exists():
        return False
    return _trailed(path.read_text(encoding="utf-8"))


def log_names(uri_prefix: str) -> list[str]:
    return [
        name for name in uri.listdir(uri_prefix) if name.startswith(LOG_NAME_PREFIX) and name.endswith(LOG_NAME_SUFFIX)
    ]


def shard_index(name: str) -> int:
    """Extract the shard index from a log filename, rejecting malformed indices."""
    raw = name[len(LOG_NAME_PREFIX) : -len(LOG_NAME_SUFFIX)]
    if not raw.isdigit():
        raise ValueError(f"publish log {name!r} does not name a shard index")
    return int(raw)


def shards_done(uri_prefix: str) -> set[int]:
    """Return shard indices whose logs contain completion trailers."""
    if not uri.exists(uri_prefix):
        return set()
    return {shard_index(name) for name in log_names(uri_prefix) if _trailed(uri.read_text(uri.join(uri_prefix, name)))}


def read_all(uri_prefix: str) -> list[PublishRecord]:
    """Read all shard logs in batch order.

    Reject duplicate batches rather than silently altering the offered totals.
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
    """Return the maximum delay from scheduled time to first acknowledgement."""
    return max((record.first_ack_ms - record.scheduled_ms for record in records), default=0)


def emit_times(records: list[PublishRecord]) -> dict[int, int]:
    return {record.batch: record.last_ack_ms for record in records}
