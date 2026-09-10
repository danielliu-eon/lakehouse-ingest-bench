# SPDX-License-Identifier: Apache-2.0
"""Create, inspect, and delete run-specific Kafka topics.

Reject existing topics at creation to prevent records from earlier runs from
contaminating the manifest comparison. Deletion tolerates absent topics so
teardown can be retried. The ``drop-topic`` command can run inside a broker's
network when the operator cannot reach it directly.

Callers resolve credential references before passing client properties;
``kafka_auth`` handles authentication-specific configuration.
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

# Retain data for two days for scoring and inspection, without a byte cap.
DEFAULT_TOPIC_CONFIG: dict[str, str] = {"retention.ms": "172800000", "retention.bytes": "-1"}

# Wait for metadata visibility after controller acknowledgements so later
# client requests do not race topic creation or deletion.
_VISIBILITY_TIMEOUT_S = 30.0
_VISIBILITY_POLL_S = 0.25

# Poll to initialize OAUTHBEARER callbacks before blocking admin requests.
# Skip further polling once a token has been supplied.
_TOKEN_POLL_S = 0.1
_TOKEN_POLL_ATTEMPTS = 20


def _client(bootstrap: str, client: dict[str, str]) -> AdminClient:
    served = False

    def token_served() -> None:
        nonlocal served
        served = True

    config = kafka_auth.librdkafka_config({"bootstrap.servers": bootstrap, **client}, on_token=token_served)
    # The client stub permits only scalar properties, but oauth_cb is callable.
    admin = AdminClient(cast("dict[str, str | int | float | bool]", config))
    if "oauth_cb" in config:
        # Let the subsequent request report the authentication failure.
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
    """Read the broker count from cluster metadata to size topic replication."""
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
    """Delete ``name`` and return whether it existed.

    An absent topic is a successful teardown state.
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
