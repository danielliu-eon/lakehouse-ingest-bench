# Stock Spark plus the connector jars a run needs. No platform pin: upstream
# publishes this tag for amd64 and arm64 and nothing added below is native, so
# the local stack runs the engine on the machine's own architecture.
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.9
ARG ICEBERG_VERSION=1.9.2
# The version spark-sql-kafka-0-10's own pom declares at this Spark version.
# The client is what talks the broker's protocol, so a mismatched one surfaces
# as an unsupported API version rather than as a configuration error.
ARG KAFKA_CLIENTS_VERSION=3.4.1
# The connector's consumer pool, and also the version its pom declares. The
# image ships commons-pool 1.x, which is a different package entirely.
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MSK_IAM_AUTH_VERSION=2.3.8
# Matching the `hadoop-client-*` jars the image already carries: hadoop-aws
# links against Hadoop's own internals, so it is the one jar here whose version
# is not ours to choose.
ARG HADOOP_VERSION=3.3.4
# The AWS SDK v1 hadoop-aws 3.3.4 reaches S3 through. Later than the version
# its pom pins, because the pinned one predates the container credential
# provider that resolves a pod's own identity — with it, a checkpoint on S3
# needs no static key anywhere.
ARG AWS_SDK_V1_VERSION=1.12.779

USER root
WORKDIR /opt/spark/jars
# The Kafka source, its token provider and an unshaded client. Amazon MSK's
# `IAMClientCallbackHandler` implements the unshaded
# `org.apache.kafka.common.security.auth.AuthenticateCallbackHandler`, so the
# client on the classpath has to be the unshaded one or the two class names
# never meet. The compression codecs are already in the image — Spark uses the
# same three — so only the client itself is added.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar .
# `from_avro` lives here rather than in the Avro core jar the image ships.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar .
# The runtime jar carries Iceberg's own AWS and GCP code but not the cloud SDKs
# it calls, which is what the bundles are.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# The `all` classifier, which bundles the AWS SDK v2 the signer needs. The SDK
# resolves the pod's own credentials, so an MSK IAM connection needs a region
# and no secret anywhere.
ADD https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar .
# Checkpoints, not table data: a streaming query's state goes to a Hadoop
# filesystem and the table's files go through Iceberg's FileIO, so both an S3A
# client and the Iceberg AWS bundle are needed and neither replaces the other.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar .
RUN chown spark:spark /opt/spark/jars/*.jar && chmod 644 /opt/spark/jars/*.jar

# The job, kept at its package path so the import reads the same here as in the
# repository. The renderer itself is not copied: it reaches the corpus and the
# catalog through the harness's dependencies, which this image has no reason to
# carry.
ENV PYTHONPATH=/opt/bench
COPY engines/__init__.py /opt/bench/engines/
COPY engines/spark/__init__.py engines/spark/env.py engines/spark/stream_to_iceberg.py /opt/bench/engines/spark/
USER spark
WORKDIR /opt/spark
