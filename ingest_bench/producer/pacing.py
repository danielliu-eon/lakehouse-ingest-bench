# SPDX-License-Identifier: Apache-2.0
"""When each batch is due, and which shard owns it.

The corpus fixes the offered rate: a batch carries a fixed interval's worth of
bytes and declares the offset from the run's start at which it is due. Pacing
is therefore arithmetic on that offset rather than a rate limiter — every shard
of every run computes the same due time from the same epoch, so two engines
compared against one corpus were offered the same bytes at the same moments.
"""

from __future__ import annotations

from ingest_bench.corpus.generate import BatchRecord


def scheduled_ms(epoch_ms: int, offset_ms: int, speed: float) -> int:
    """When the batch at ``offset_ms`` is due, under a ``speed``-times replay.

    Speed divides the corpus's own timeline: at 6 an hour of offered traffic is
    sent in ten minutes, which is how a long run is priced without generating a
    long corpus.
    """
    if speed <= 0:
        raise ValueError(f"speed must be positive, got {speed}")
    return epoch_ms + round(offset_ms / speed)


def select_batches(records: list[BatchRecord], shard: int, shards: int, seconds: int | None) -> list[BatchRecord]:
    """The batches this shard sends, in send order.

    Shards interleave rather than split the corpus into contiguous halves: each
    takes every ``shards``-th batch, so every shard is busy across the whole run
    and the offered rate stays flat when one of them is slow to start.
    """
    if shards < 1 or not 0 <= shard < shards:
        raise ValueError(f"shard must satisfy 0 <= shard < shards, got shard={shard} shards={shards}")
    limit_ms = None if seconds is None else seconds * 1000
    return [
        record
        for record in records
        if record.batch % shards == shard and (limit_ms is None or record.offset_ms < limit_ms)
    ]
