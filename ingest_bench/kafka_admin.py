# SPDX-License-Identifier: Apache-2.0
"""Create and drop the topic a run publishes into, and count the cluster's brokers.

A run owns its topic: it is created before the producer starts and dropped
after the score is written, so one run's retained bytes cannot be read as
another's. Creation is refused rather than made idempotent — a topic that
already exists holds records from an earlier run, and appending to it would
put rows in the table that no manifest accounts for.

Dropping is also a command of its own, because a managed broker is often
reachable only from inside its own network: the harness image runs `drop-topic`
there while a teardown script runs on an operator's machine.

Every call takes the librdkafka client properties a run's site declares, with
any indirection already resolved. Authentication is not implemented here: the
properties reach the client through `kafka_auth`, which passes all but its own
keys through verbatim, so a cluster this harness has never heard of is
reachable by configuration alone.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from typing import cast

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from ingest_bench import kafka_auth
from ingest_bench.catalog import parse_key_values
from ingest_bench.specs.env import resolve_env_placeholders

REQUEST_TIMEOUT_S = 30.0

# Two days is longer than any run plus the time an operator needs to look at
# what a failed one left behind, and an unbounded byte cap keeps a topic from
# dropping the head of a stream the scorer still expects to be readable.
DEFAULT_TOPIC_CONFIG: dict[str, str] = {"retention.ms": "172800000", "retention.bytes": "-1"}

# Topic creation and deletion return once the controller has acted, and the
# broker a later metadata request lands on may not have caught up yet. Every
# call here therefore waits for the cluster to agree before returning, so a
# caller that creates a topic and then produces into it cannot race it.
_VISIBILITY_TIMEOUT_S = 30.0
_VISIBILITY_POLL_S = 0.25

# librdkafka serves an OAUTHBEARER token callback only from a client's `poll`,
# and an admin client's own requests never poll: `list_topics` on a client that
# has no token yet waits out its whole timeout and reports a SASL
# authentication error. Polling here is what makes the token exist before the
# first request. A client library that already served the callback while it
# constructed the client leaves this loop with nothing to do.
_TOKEN_POLL_S = 0.1
_TOKEN_POLL_ATTEMPTS = 20


def _client(bootstrap: str, client: dict[str, str]) -> AdminClient:
    served = False

    def token_served() -> None:
        nonlocal served
        served = True

    config = kafka_auth.librdkafka_config({"bootstrap.servers": bootstrap, **client}, on_token=token_served)
    # The admin client's declared configuration holds only scalars, while
    # librdkafka's token callback is a callable. The cast is over a mapping this
    # module built, so nothing unchecked reaches the client.
    admin = AdminClient(cast("dict[str, str | int | float | bool]", config))
    if "oauth_cb" in config:
        # A token that never arrives is left to the request that follows: the
        # broker's own authentication error names more than a refusal here could.
        for _ in range(_TOKEN_POLL_ATTEMPTS):
            if served:
                break
            admin.poll(_TOKEN_POLL_S)
    return admin


def _error_code(err: KafkaException) -> int:
    return int(err.args[0].code())


def _exists(admin: AdminClient, name: str) -> bool:
    return name in admin.list_topics(timeout=REQUEST_TIMEOUT_S).topics


def _await_visibility(admin: AdminClient, name: str, present: bool) -> None:
    deadline = time.monotonic() + _VISIBILITY_TIMEOUT_S
    while _exists(admin, name) != present:
        if time.monotonic() >= deadline:
            state = "appear" if present else "disappear"
            raise TimeoutError(
                f"topic {name!r} did not {state} in cluster metadata within {_VISIBILITY_TIMEOUT_S:.0f}s"
            )
        time.sleep(_VISIBILITY_POLL_S)


def topic_exists(bootstrap: str, name: str, client: dict[str, str]) -> bool:
    return _exists(_client(bootstrap, client), name)


def broker_count(bootstrap: str, client: dict[str, str]) -> int:
    """How many brokers the cluster's metadata names.

    This is what a run's replication factor is chosen from, so it is read from
    the cluster rather than guessed at from a hostname: a name says nothing
    about how many brokers answer to it, and a factor above the count is
    refused by the broker at topic creation.
    """
    brokers = _client(bootstrap, client).list_topics(timeout=REQUEST_TIMEOUT_S).brokers
    if not brokers:
        raise ValueError(f"the cluster at {bootstrap} names no brokers in its metadata")
    return len(brokers)


def create_topic(
    bootstrap: str,
    name: str,
    partitions: int,
    replication_factor: int,
    topic_config: dict[str, str],
    client: dict[str, str],
) -> None:
    admin = _client(bootstrap, client)
    topic = NewTopic(name, num_partitions=partitions, replication_factor=replication_factor, config=dict(topic_config))
    try:
        admin.create_topics([topic], request_timeout=REQUEST_TIMEOUT_S)[name].result()
    except KafkaException as err:
        if _error_code(err) == KafkaError.TOPIC_ALREADY_EXISTS:
            raise ValueError(f"topic {name!r} already exists on {bootstrap}; drop it before starting a run") from err
        raise
    _await_visibility(admin, name, present=True)


def delete_topic(bootstrap: str, name: str, client: dict[str, str]) -> bool:
    """Drop ``name``, returning whether there was a topic to drop.

    An absent topic is already dropped rather than an error: teardown runs
    against runs that failed before staging created one, and a refusal there
    would leave the rest of a teardown undone.
    """
    admin = _client(bootstrap, client)
    try:
        admin.delete_topics([name], request_timeout=REQUEST_TIMEOUT_S)[name].result()
    except KafkaException as err:
        if _error_code(err) != KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise
        return False
    _await_visibility(admin, name, present=False)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="drop-topic", description="Drop a run's topic, whether or not it is there to drop."
    )
    parser.add_argument("--bootstrap", required=True, metavar="HOST:PORT", help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="the topic to drop, which is the run id")
    parser.add_argument(
        "--kafka-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a librdkafka client property, repeatable, as the site declares them. A ${env:NAME} value is read "
        "from this process's environment; aws.region is taken here too, to sign an Amazon MSK IAM token with",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    bootstrap, topic = str(parsed.bootstrap), str(parsed.topic)
    client = resolve_env_placeholders(parse_key_values([str(prop) for prop in parsed.kafka_prop], "--kafka-prop"))
    if delete_topic(bootstrap, topic, client):
        print(f"dropped topic {topic!r} on {bootstrap}")
    else:
        print(f"no topic {topic!r} on {bootstrap}, so nothing to drop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
