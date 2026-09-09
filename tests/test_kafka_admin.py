import os

import pytest

from ingest_bench import kafka_admin

pytestmark = pytest.mark.integration
BOOTSTRAP = os.environ.get("INGEST_BENCH_TEST_BOOTSTRAP")
MSK_IAM = os.environ.get("INGEST_BENCH_TEST_MSK_IAM") == "1"
MSK_IAM_REGION = os.environ.get("AWS_REGION")


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


@pytest.mark.skipif(
    not (BOOTSTRAP and MSK_IAM and MSK_IAM_REGION),
    reason="set INGEST_BENCH_TEST_BOOTSTRAP, INGEST_BENCH_TEST_MSK_IAM=1 and AWS_REGION, and run it where the "
    "ambient credentials reach a cluster with IAM authentication",
)
def test_topic_lifecycle_with_iam_authentication() -> None:
    """The same lifecycle against a cluster whose token is signed per connection.

    `broker_count` runs first because it is the first call staging makes, and
    the one that fails on a client whose token was never served: a metadata
    request does not poll, so nothing else would have asked for the token.
    """
    assert BOOTSTRAP is not None and MSK_IAM_REGION is not None
    client = {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": MSK_IAM_REGION}
    name = "ingest-bench-test-topic-iam"
    brokers = kafka_admin.broker_count(BOOTSTRAP, client)
    assert brokers >= 1
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
    kafka_admin.create_topic(
        BOOTSTRAP,
        name,
        partitions=3,
        replication_factor=min(3, brokers),
        topic_config={"retention.ms": "60000"},
        client=client,
    )
    assert kafka_admin.topic_exists(BOOTSTRAP, name, client)
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
