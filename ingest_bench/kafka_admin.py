"""Create and drop the topic a run publishes into.

A run owns its topic: it is created before the producer starts and dropped
after the score is written, so one run's retained bytes cannot be read as
another's. Creation is refused rather than made idempotent — a topic that
already exists holds records from an earlier run, and appending to it would
put rows in the table that no manifest accounts for.
"""

from __future__ import annotations

import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

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


def _client(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap})


def _error_code(err: KafkaException) -> int:
    return int(err.args[0].code())


def _exists(client: AdminClient, name: str) -> bool:
    return name in client.list_topics(timeout=REQUEST_TIMEOUT_S).topics


def _await_visibility(client: AdminClient, name: str, present: bool) -> None:
    deadline = time.monotonic() + _VISIBILITY_TIMEOUT_S
    while _exists(client, name) != present:
        if time.monotonic() >= deadline:
            state = "appear" if present else "disappear"
            raise TimeoutError(
                f"topic {name!r} did not {state} in cluster metadata within {_VISIBILITY_TIMEOUT_S:.0f}s"
            )
        time.sleep(_VISIBILITY_POLL_S)


def topic_exists(bootstrap: str, name: str) -> bool:
    return _exists(_client(bootstrap), name)


def create_topic(bootstrap: str, name: str, partitions: int, replication_factor: int, config: dict[str, str]) -> None:
    client = _client(bootstrap)
    topic = NewTopic(name, num_partitions=partitions, replication_factor=replication_factor, config=dict(config))
    try:
        client.create_topics([topic], request_timeout=REQUEST_TIMEOUT_S)[name].result()
    except KafkaException as err:
        if _error_code(err) == KafkaError.TOPIC_ALREADY_EXISTS:
            raise ValueError(f"topic {name!r} already exists on {bootstrap}; drop it before starting a run") from err
        raise
    _await_visibility(client, name, present=True)


def delete_topic(bootstrap: str, name: str) -> None:
    """Drop ``name``, treating an absent topic as already dropped."""
    client = _client(bootstrap)
    try:
        client.delete_topics([name], request_timeout=REQUEST_TIMEOUT_S)[name].result()
    except KafkaException as err:
        if _error_code(err) != KafkaError.UNKNOWN_TOPIC_OR_PART:
            raise
        return
    _await_visibility(client, name, present=False)
