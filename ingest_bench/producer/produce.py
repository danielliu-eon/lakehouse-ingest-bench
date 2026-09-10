# SPDX-License-Identifier: Apache-2.0
"""Publish corpus batches to Kafka on their recorded schedule.

Send encoded corpus values without decoding and record broker acknowledgements.
Abort on delivery errors: retrying a partially delivered batch could duplicate
rows or leave gaps that the scorer would attribute to the engine.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TextIO

from ingest_bench import kafka_auth, uri
from ingest_bench.clock import Clock
from ingest_bench.corpus import frames, metadata
from ingest_bench.producer import pacing, publish_log
from ingest_bench.specs.model import refuse_compression_props

PROGRESS_INTERVAL_MS = 60_000
UPLOAD_INTERVAL_MS = 5_000

# librdkafka's delivery reports are served from `poll`, and nothing else frees
# the send queue, so a long batch has to yield to it before the queue fills.
POLL_EVERY_ROWS = 4096

# Match the configured message timeout when draining the queue.
FLUSH_TIMEOUT_S = 120.0


class FrameProducer(Protocol):
    """Producer interface used for publishing and test doubles.

    Keep ``on_delivery`` keyword-only: librdkafka's fourth positional argument
    is the partition.
    """

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        *,
        on_delivery: Callable[[object | None, object], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class BatchOutcome:
    first_ack_ms: int
    last_ack_ms: int
    rows: int
    bytes: int
    errors: int


@dataclass(frozen=True)
class ProduceArgs:
    corpus_uri: str
    bootstrap: str
    topic: str
    epoch_ms: int
    speed: float
    shard: int
    shards: int
    seconds: int | None
    key_column: str | None
    # Prebuilt value header; empty for raw Avro.
    value_prefix: bytes
    publish_log_path: Path
    behind_max_ms: int
    upload_prefix: str | None
    # The codec the batches are compressed with, which the run's spec chose and
    # its `facts.json` published: a consumer is configured against it.
    compression: str
    # Resolved site properties, applied over defaults through `kafka_auth`.
    kafka_props: dict[str, str]


def default_producer_config(bootstrap: str, compression: str) -> dict[str, object]:
    """Configure idempotent delivery with full acknowledgements and large buffers.

    Use the caller's compression codec. Buffering absorbs brief broker stalls;
    acknowledgement timing records resulting producer delay.
    """
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "batch.size": 1048576,
        "compression.type": compression,
        "queue.buffering.max.messages": 1000000,
        "queue.buffering.max.kbytes": 1048576,
        "message.timeout.ms": 120000,
    }


def produce_batch(
    producer: FrameProducer,
    topic: str,
    frames_iter: Iterable[bytes],
    keys: Iterable[bytes] | None,
    clock: Clock,
    queue_full_backoff_s: float = 0.005,
    value_prefix: bytes = b"",
) -> BatchOutcome:
    """Send one batch and wait for all delivery reports.

    Flush before the next batch so acknowledgement boundaries remain distinct.
    Include ``value_prefix`` in each value and in the reported byte count.
    """
    state = {"acked": 0, "errors": 0, "first": 0, "last": 0}

    def on_delivery(err: object | None, _msg: object) -> None:
        now = clock.now_ms()
        if err is not None:
            state["errors"] += 1
            return
        state["acked"] += 1
        state["first"] = state["first"] or now
        state["last"] = now

    rows = 0
    total = 0
    key_iter = iter(keys) if keys is not None else None
    for frame in frames_iter:
        key: bytes | None = None
        if key_iter is not None:
            key = next(key_iter, None)
            if key is None:
                raise ValueError(
                    f"the key sidecar ran out at frame {rows}; it holds fewer keys than the batch has rows"
                )
        value = value_prefix + frame if value_prefix else frame
        while True:
            try:
                producer.produce(topic, value, key, on_delivery=on_delivery)
                break
            except BufferError:
                # librdkafka's queue is bounded; draining delivery reports frees it.
                producer.poll(queue_full_backoff_s)
                clock.sleep(queue_full_backoff_s)
        rows += 1
        total += len(value)
        if rows % POLL_EVERY_ROWS == 0:
            producer.poll(0)
    if key_iter is not None and next(key_iter, None) is not None:
        raise ValueError(f"the key sidecar holds more keys than the batch's {rows} rows")
    producer.flush(FLUSH_TIMEOUT_S)
    if state["acked"] + state["errors"] != rows:
        raise RuntimeError(f"{rows} frames sent but {state['acked'] + state['errors']} delivery reports received")
    return BatchOutcome(state["first"], state["last"], rows, total, state["errors"])


def _batch_frames(reference: str) -> Iterator[bytes]:
    return frames.iter_frames(frames.decompress(uri.read_bytes(reference)))


def _upload(upload_prefix: str, path: Path) -> None:
    uri.write_bytes(uri.join(upload_prefix, "producer", path.name), path.read_bytes())


def run(
    args: ProduceArgs,
    producer_factory: Callable[[dict[str, object]], FrameProducer],
    clock: Clock,
    log: TextIO,
) -> int:
    meta = metadata.read(args.corpus_uri)
    if args.key_column is not None and args.key_column not in meta.key_columns:
        raise ValueError(
            f"--key-column {args.key_column!r} is not in the corpus's kafka_key_columns {meta.key_columns}; "
            "regenerate the corpus with it or choose another key"
        )
    selected = pacing.select_batches(metadata.read_manifest(args.corpus_uri), args.shard, args.shards, args.seconds)
    # The second gate on the codec: these properties win the merge below, and
    # they reach here from a command line as well as from a site.
    refuse_compression_props(args.kafka_props, "--kafka-prop")
    producer = producer_factory(
        kafka_auth.librdkafka_config({**default_producer_config(args.bootstrap, args.compression), **args.kafka_props})
    )
    records: list[publish_log.PublishRecord] = []
    offered = 0
    published = 0
    errors = 0
    next_progress_ms = 0
    next_upload_ms = 0
    reported_behind = False

    for batch_record in selected:
        due_ms = pacing.scheduled_ms(args.epoch_ms, batch_record.offset_ms, args.speed)
        delay_ms = due_ms - clock.now_ms()
        if delay_ms > 0:
            clock.sleep(delay_ms / 1000)
        keys = None if args.key_column is None else _batch_frames(batch_record.key_uris[args.key_column])
        outcome = produce_batch(
            producer, args.topic, _batch_frames(batch_record.uri), keys, clock, value_prefix=args.value_prefix
        )
        record = publish_log.PublishRecord(
            batch=batch_record.batch,
            scheduled_ms=due_ms,
            first_ack_ms=outcome.first_ack_ms,
            last_ack_ms=outcome.last_ack_ms,
            rows=outcome.rows,
            bytes=outcome.bytes,
            errors=outcome.errors,
        )
        publish_log.append(args.publish_log_path, record)
        records.append(record)
        offered += outcome.rows
        published += outcome.rows - outcome.errors
        errors += outcome.errors

        now_ms = clock.now_ms()
        behind = publish_log.behind_ms(records)
        if now_ms >= next_progress_ms:
            print(
                f"PROGRESS offered={offered} published={published} behind_ms={behind} errors={errors}",
                file=log,
                flush=True,
            )
            next_progress_ms = now_ms + PROGRESS_INTERVAL_MS
        # Falling behind does not stop the run; whether it invalidates one is the
        # scorer's call, made from the same publish log this line is derived from.
        if not reported_behind and behind > args.behind_max_ms:
            print(f"BEHIND behind_ms={behind} behind_max_ms={args.behind_max_ms}", file=log, flush=True)
            reported_behind = True
        if args.upload_prefix is not None and now_ms >= next_upload_ms:
            _upload(args.upload_prefix, args.publish_log_path)
            next_upload_ms = now_ms + UPLOAD_INTERVAL_MS
        if outcome.errors:
            print(f"PRODUCE FAILED batch={batch_record.batch} errors={outcome.errors}", file=log, flush=True)
            if args.upload_prefix is not None:
                _upload(args.upload_prefix, args.publish_log_path)
            return 1

    publish_log.append_done(args.publish_log_path, args.shard, len(records))
    if args.upload_prefix is not None:
        _upload(args.upload_prefix, args.publish_log_path)
    print(
        f"PRODUCE DONE batches={len(records)} rows={offered} behind_ms={publish_log.behind_ms(records)}",
        file=log,
        flush=True,
    )
    return 0
