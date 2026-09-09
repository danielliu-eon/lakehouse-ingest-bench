# SPDX-License-Identifier: Apache-2.0
"""Send a corpus's batches into Kafka, each at the moment it is due.

The producer is the offered side of the benchmark, so its only job is to be
uninteresting: it sends exactly the bytes the corpus published, at the times
the corpus published, and records when the broker acknowledged them. It
decodes nothing — a batch file is already a sequence of message values — so
what an engine reads is what the manifest is scored against.

A delivery error ends the run. There is no resume: a batch half in the topic
cannot be re-sent without either duplicating rows the engine already read or
leaving a hole, and both would be scored as the engine's exactness rather than
the producer's.
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

# The producer waits at most this long for the queue to drain, matching
# `message.timeout.ms`: a frame that has not been acknowledged by then has
# permanently failed, and waiting longer only delays saying so.
FLUSH_TIMEOUT_S = 120.0


class FrameProducer(Protocol):
    """The part of a Kafka producer this module uses, so a fake can stand in.

    The delivery callback is keyword-only because a real producer's fourth
    positional argument is the partition: passed positionally, the callback
    would be taken for one and never be called.
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
    # Prepended to every value, and empty for the corpus's own raw Avro. The
    # header rather than the encoding's name because the loop has nothing else
    # to decide: which five bytes a `confluent` run carries is settled once, by
    # the caller that knows the schema id, and a run with no header is the same
    # loop with nothing to prepend.
    value_prefix: bytes
    publish_log_path: Path
    behind_max_ms: int
    upload_prefix: str | None
    # The codec the batches are compressed with, which the run's spec chose and
    # its `facts.json` published: a consumer is configured against it.
    compression: str
    # librdkafka client properties from the site, with any environment
    # indirection already resolved. Applied over the defaults below and then
    # built through `kafka_auth`, so a site that needs authentication — MSK's
    # IAM mechanism included — needs no new knob here.
    kafka_props: dict[str, str]


def default_producer_config(bootstrap: str, compression: str) -> dict[str, object]:
    """Durable, ordered, idempotent delivery, batched hard enough to saturate a link.

    `acks=all` with idempotence is what makes an acknowledgement mean the row is
    in the topic once, which is the claim every offered figure rests on. The
    queue is sized to hold about a second of the largest offered rate so a brief
    broker stall shows up as producer lag rather than as a full queue.

    The codec is the caller's because it is the run's: `compression` is one of
    librdkafka's `compression.type` values and is passed through as it stands.
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
    """Send one batch and wait for every frame in it to be acknowledged.

    The batch is not offered until its last acknowledgement arrives, so this
    blocks on `flush` rather than letting the next batch overlap: an overlapping
    batch would make the offered timeline the producer's queue depth instead of
    the corpus's schedule.

    ``value_prefix`` is prepended to each frame and counted in the bytes
    reported: a frame is already the value's Avro binary, so a header is the
    only thing between the corpus's bytes and the wire, and what was offered is
    what the broker was actually sent.
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
