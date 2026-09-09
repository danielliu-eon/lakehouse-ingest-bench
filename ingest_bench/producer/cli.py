"""Command line for one producer shard.

A run's producers are shards of one command: they share the corpus, the topic
and the epoch, and differ only in `--shard`. The epoch is passed rather than
taken from the local clock so every shard, and the scorer, measure from the
same instant — a shard that chose its own start would offer its batches at
times no other shard agreed with.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from ingest_bench.catalog import parse_key_values
from ingest_bench.clock import SystemClock
from ingest_bench.producer.produce import FrameProducer, ProduceArgs, run
from ingest_bench.specs.env import resolve_env_placeholders


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="produce", description="Send a corpus into a Kafka topic on schedule.")
    parser.add_argument("--corpus", required=True, metavar="URI", help="the corpus directory to send")
    parser.add_argument("--bootstrap", required=True, metavar="HOST:PORT", help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, shared by every shard and by the scorer",
    )
    parser.add_argument("--speed", type=float, default=1.0, help="replay the corpus's timeline this many times faster")
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--seconds", type=int, help="send only the batches due in the first SECONDS of the corpus")
    parser.add_argument("--key-column", metavar="NAME", help="a corpus key column to send as the Kafka message key")
    parser.add_argument("--publish-log", required=True, metavar="PATH", help="where to write this shard's publish log")
    parser.add_argument(
        "--behind-max-ms", type=int, default=5000, help="report once the producer falls this far behind"
    )
    parser.add_argument(
        "--upload-prefix", metavar="URI", help="copy the publish log under this prefix as it is written"
    )
    parser.add_argument(
        "--kafka-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a librdkafka client property (security.protocol=..., sasl.username=...), repeatable. Applied over "
        "the producer's own defaults; a ${env:NAME} value is read from the environment of this process. "
        "aws.region is taken here too, to sign an Amazon MSK IAM token with",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    args = ProduceArgs(
        corpus_uri=str(parsed.corpus),
        bootstrap=str(parsed.bootstrap),
        topic=str(parsed.topic),
        epoch_ms=round(float(parsed.epoch) * 1000),
        speed=float(parsed.speed),
        shard=int(parsed.shard),
        shards=int(parsed.shards),
        seconds=None if parsed.seconds is None else int(parsed.seconds),
        key_column=None if parsed.key_column is None else str(parsed.key_column),
        publish_log_path=Path(str(parsed.publish_log)),
        behind_max_ms=int(parsed.behind_max_ms),
        upload_prefix=None if parsed.upload_prefix is None else str(parsed.upload_prefix),
        kafka_props=resolve_env_placeholders(
            parse_key_values([str(prop) for prop in parsed.kafka_prop], "--kafka-prop")
        ),
    )
    return run(args, _confluent_producer, SystemClock(), sys.stdout)


def _confluent_producer(config: dict[str, object]) -> FrameProducer:
    # Imported here so the module can be loaded, and its arguments parsed, on a
    # machine without librdkafka.
    from confluent_kafka import Producer

    producer: FrameProducer = Producer(config)
    return producer


if __name__ == "__main__":
    raise SystemExit(main())
