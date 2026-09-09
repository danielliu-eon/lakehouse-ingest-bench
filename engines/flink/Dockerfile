# PyFlink publishes no Linux aarch64 wheel in any release, so the image is
# amd64 wherever it is built; the alternative is building PyFlink from source
# inside the image. A benchmark fleet is x86 in any case. On an arm64
# developer machine the local stack then runs emulated, which is enough to
# check a run end to end but not to measure one.
ARG IMAGE_PLATFORM=linux/amd64
FROM --platform=${IMAGE_PLATFORM} flink:1.20.1-scala_2.12-java17

ARG ICEBERG_VERSION=1.9.2
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG KAFKA_CLIENTS_VERSION=3.4.0
ARG MSK_IAM_AUTH_VERSION=2.3.8
# The compression codecs kafka-clients declares, at the versions its own pom
# declares them at: the codec is chosen by the producer and decompressed by the
# consumer, so a missing one surfaces as an unreadable record rather than as a
# configuration error.
ARG ZSTD_JNI_VERSION=1.5.2-1
ARG LZ4_JAVA_VERSION=1.8.0
ARG SNAPPY_JAVA_VERSION=1.1.8.4
ARG FLINK_VERSION=1.20.1
ARG HADOOP_VERSION=3.3.6

# The entrypoint moves the named jar out of `opt` into `plugins`, which is
# what puts an S3 filesystem on the checkpoint path's classpath. Table data
# takes a different route: the Iceberg jars below reach storage through their
# own FileIO.
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-${FLINK_VERSION}.jar

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && python3 -m pip install --no-cache-dir "apache-flink==${FLINK_VERSION}" "pyyaml==6.0.2"

WORKDIR /opt/flink/lib
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar .
# The runtime jar carries Iceberg's own AWS and GCP code but not the cloud
# SDKs it calls, which is what these two bundles are.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# The plain connector and an unshaded Kafka client rather than the SQL uber
# jar, which is the same connector shaded: it relocates `org.apache.kafka` to
# `org.apache.flink.kafka.shaded.org.apache.kafka` and carries no unshaded
# copy. Amazon MSK's `IAMClientCallbackHandler` implements the unshaded
# `org.apache.kafka.common.security.auth.AuthenticateCallbackHandler`, so a
# shaded client cannot load it whatever it is configured with — the two class
# names never meet. The plain connector registers the same
# `KafkaDynamicTableFactory`, so `connector = 'kafka'` resolves either way.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/github/luben/zstd-jni/${ZSTD_JNI_VERSION}/zstd-jni-${ZSTD_JNI_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/lz4/lz4-java/${LZ4_JAVA_VERSION}/lz4-java-${LZ4_JAVA_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/${SNAPPY_JAVA_VERSION}/snappy-java-${SNAPPY_JAVA_VERSION}.jar .
# The `all` classifier, which bundles the AWS SDK v2 the signer needs. The SDK
# resolves the pod's own credentials, so an MSK IAM connection needs a region
# and no secret anywhere.
ADD https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar .
# The registry jar and not the plain `flink-sql-avro`: it is a shading run over
# that module plus the Confluent registry format and its client, and its
# factory service file registers `avro` alongside `avro-confluent`. So this one
# jar serves both formats a run can ask for, where the two jars together would
# carry the same shaded Avro and `flink-avro` classes twice.
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar .
# Iceberg's Flink runtime resolves a table through Hadoop's `Configuration`
# whichever FileIO then reads it, so Hadoop is needed even for object storage.
#
# The shaded client pair rather than `hadoop-common` and its siblings: Flink
# installs its `HadoopModule` as soon as it finds Hadoop on the classpath, and
# that initializes `UserGroupInformation`, which pulls in commons-configuration2,
# guava, re2j and more. `hadoop-client-api` and `hadoop-client-runtime` are one
# shading run of exactly that closure — the api jar's references point at the
# relocated third-party packages the runtime jar carries — so the two together
# are the whole Hadoop classpath and nothing else has to be enumerated.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar .
RUN chown -R flink:flink /opt/flink/lib && chmod 644 /opt/flink/lib/*.jar

# The submitter and the one module it shares with the renderer, kept at their
# package path so the import reads the same here as in the repository. The
# renderer itself is not copied: it reaches the catalog through pyiceberg,
# which this image has no reason to carry.
ENV PYTHONPATH=/opt/bench
COPY engines/__init__.py /opt/bench/engines/
COPY engines/flink/__init__.py engines/flink/script.py engines/flink/job.py /opt/bench/engines/flink/
USER flink
WORKDIR /opt/flink
