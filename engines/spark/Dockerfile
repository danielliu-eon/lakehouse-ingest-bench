# Stock Spark and its connectors support amd64 and arm64 without a platform pin.
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

ARG SPARK_VERSION=3.5.9
ARG ICEBERG_VERSION=1.9.2
# Match the Kafka client version declared by this Spark connector.
ARG KAFKA_CLIENTS_VERSION=3.4.1
# Spark ships commons-pool 1.x; the Kafka consumer pool requires 2.x.
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MSK_IAM_AUTH_VERSION=2.3.8
# Match hadoop-aws to the hadoop-client jars bundled with Spark.
ARG HADOOP_VERSION=3.3.4
# Use an AWS SDK v1 release with the container credential provider needed
# for S3 checkpoints authenticated through pod identity.
ARG AWS_SDK_V1_VERSION=1.12.779

USER root
WORKDIR /opt/spark/jars
# MSK's IAMClientCallbackHandler requires unshaded Kafka interfaces.
# Spark already bundles the client's three compression codecs.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar .
# from_avro is in spark-avro, not the bundled Avro core jar.
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar .
# Iceberg's cloud integrations also need these AWS and GCP SDK bundles.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar .
# The all classifier includes AWS SDK v2 for pod identity credentials.
ADD https://repo1.maven.org/maven2/software/amazon/msk/aws-msk-iam-auth/${MSK_IAM_AUTH_VERSION}/aws-msk-iam-auth-${MSK_IAM_AUTH_VERSION}-all.jar .
# Checkpoints use Hadoop S3A; table files use Iceberg FileIO. Both clients
# are required.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar .
RUN chown spark:spark /opt/spark/jars/*.jar && chmod 644 /opt/spark/jars/*.jar

# Preserve package paths for imports. Omit the renderer and its harness dependencies.
ENV PYTHONPATH=/opt/bench
COPY engines/__init__.py /opt/bench/engines/
COPY engines/spark/__init__.py engines/spark/env.py engines/spark/stream_to_iceberg.py /opt/bench/engines/spark/
USER spark
WORKDIR /opt/spark
