# The pinned PyFlink release requires amd64. Local arm64 runs use emulation
# and validate correctness only, not performance.
ARG IMAGE_PLATFORM=linux/amd64
FROM --platform=${IMAGE_PLATFORM} flink:1.20.1-scala_2.12-java17

ARG ICEBERG_VERSION=1.9.2
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG KAFKA_CLIENTS_VERSION=3.4.0
ARG MSK_IAM_AUTH_VERSION=2.3.8
# Use the compression codecs and versions declared by kafka-clients.
ARG ZSTD_JNI_VERSION=1.5.2-1
ARG LZ4_JAVA_VERSION=1.8.0
ARG SNAPPY_JAVA_VERSION=1.1.8.4
ARG FLINK_VERSION=1.20.1
ARG HADOOP_VERSION=3.3.6

# Enable the S3 plugin for checkpoints. Iceberg table data uses its own FileIO.
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-${FLINK_VERSION}.jar

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && python3 -m pip install --no-cache-dir "apache-flink==${FLINK_VERSION}" "pyyaml==6.0.2"

WORKDIR /opt/flink/lib
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar .
# Supply the AWS and GCP SDKs used by Iceberg's runtime.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# MSK's IAMClientCallbackHandler requires unshaded Kafka interfaces. Use the
# plain connector and client; the shaded SQL jar relocates those interfaces.
# The plain connector still registers KafkaDynamicTableFactory.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/github/luben/zstd-jni/${ZSTD_JNI_VERSION}/zstd-jni-${ZSTD_JNI_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/lz4/lz4-java/${LZ4_JAVA_VERSION}/lz4-java-${LZ4_JAVA_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/${SNAPPY_JAVA_VERSION}/snappy-java-${SNAPPY_JAVA_VERSION}.jar .
# The all classifier includes AWS SDK v2 for pod identity credentials.
ADD https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar .
# This jar registers both avro and avro-confluent. Adding flink-sql-avro too
# would duplicate shaded Avro classes.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar .
# Iceberg needs Hadoop Configuration even with object-store FileIO. Flink's
# HadoopModule also initializes UserGroupInformation and its dependencies.
# The shaded client pair supplies that dependency tree without separate jars.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar .
RUN chown -R flink:flink /opt/flink/lib && chmod 644 /opt/flink/lib/*.jar

# Preserve package paths for imports. Copy only the submitter and its helper;
# the renderer depends on harness packages that the image does not need.
ENV PYTHONPATH=/opt/bench
COPY engines/__init__.py /opt/bench/engines/
COPY engines/flink/__init__.py engines/flink/script.py engines/flink/job.py /opt/bench/engines/flink/
USER flink
WORKDIR /opt/flink
