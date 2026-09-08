# PyFlink publishes no Linux aarch64 wheel in any release, so the image is
# amd64 wherever it is built; the alternative is building PyFlink from source
# inside the image. A benchmark fleet is x86 in any case. On an arm64
# developer machine the local stack then runs emulated, which is enough to
# check a run end to end but not to measure one.
ARG IMAGE_PLATFORM=linux/amd64
FROM --platform=${IMAGE_PLATFORM} flink:1.20.1-scala_2.12-java17

ARG ICEBERG_VERSION=1.9.2
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
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
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro/${FLINK_VERSION}/flink-sql-avro-${FLINK_VERSION}.jar .
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
