import os

import pytest

from ingest_bench import kafka_admin

pytestmark = pytest.mark.integration
BOOTSTRAP = os.environ.get("INGEST_BENCH_TEST_BOOTSTRAP")


@pytest.mark.skipif(not BOOTSTRAP, reason="set INGEST_BENCH_TEST_BOOTSTRAP to a reachable broker")
def test_topic_lifecycle() -> None:
    assert BOOTSTRAP is not None
    name = "ingest-bench-test-topic"
    client: dict[str, str] = {}
    assert kafka_admin.broker_count(BOOTSTRAP, client) >= 1
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
    kafka_admin.create_topic(
        BOOTSTRAP, name, partitions=3, replication_factor=1, topic_config={"retention.ms": "60000"}, client=client
    )
    assert kafka_admin.topic_exists(BOOTSTRAP, name, client)
    with pytest.raises(ValueError):
        kafka_admin.create_topic(BOOTSTRAP, name, partitions=3, replication_factor=1, topic_config={}, client=client)
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
