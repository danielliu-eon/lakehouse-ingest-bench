# Spark engine

The engine runs Apache Spark 3.5.9 Structured Streaming with released Kafka,
Avro, and Iceberg connectors. `knobs.py` renders the configuration, reader schema,
and job document; `stream_to_iceberg.py` reads them and starts one query.

## Runtime dependencies

| Piece | What it is |
|---|---|
| Image | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu`, multi-arch |
| Source | `spark-sql-kafka-0-10_2.12:3.5.9` + `spark-token-provider-kafka-0-10_2.12:3.5.9`, an unshaded `kafka-clients:3.4.1` and `commons-pool2:2.11.1` |
| Avro | `spark-avro_2.12:3.5.9` — `from_avro` is here, not in the Avro core jar the image ships |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier, so its AWS SDK v2 comes with it) |
| Sink | `iceberg-spark-runtime-3.5_2.12:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Checkpoints | `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.779` |


Spark already includes Kafka's zstd, lz4, and snappy codecs. The image adds
`commons-pool2` because the bundled `commons-pool` 1.x uses a different package.
The Kafka client must remain unshaded so MSK's `IAMClientCallbackHandler` can
load its `AuthenticateCallbackHandler` interface. See `Dockerfile` for the
classpath constraints.

## Local runs

The `spark` Compose profile runs one container with
`--master local[executors × executor_cores]`. All work runs in the driver's JVM,
using `driver_mem_mb`; `executor_mem_mb` does not allocate a separate heap.
Set `RUN_DIR` to the staged run directory. Compose reads `LOCAL_CORES` and
`DRIVER_MEM_MB` from its `job.env`.

Readiness requires an application named for the run and an active query in the
Structured Streaming UI. Spark 3.5's `/streaming` REST resource serves DStreams,
so the readiness check uses `/api/v1/applications` and the Structured Streaming
tab. Disabling `spark.sql.streaming.ui.enabled` prevents readiness.

Local runs check integration, not throughput: the engine shares a machine with
the harness, broker, and object store. No local Spark result is recorded here.
See the [recorded AWS smoke run](../../docs/examples/aws-smoke-spark/).

## Kubernetes runs

Each run creates a `SparkApplication` in cluster mode and a ConfigMap mounted
at `/opt/bench/run`. The driver uses `site.kubernetes.spark_service_account`,
which has the cloud identity and RBAC needed to create executors. Setup disables
the operator chart's separate Spark service account and RBAC.

Driver and executor CPU requests match their limits for Guaranteed QoS. Both
receive the AWS region for broker authentication and storage access.
`verify-spark` checks the effective driver settings, one Running driver, and the
requested number of Running executors with Guaranteed QoS. It accepts the run ID
or its lowercase Kubernetes name as the application name. It cannot read back
the trigger interval during staging; see [pitfalls](../../docs/pitfalls.md).

## Knobs

| Knob | Default | Effect |
|---|---|---|
| `executors` | required | executor count; `local[N]` multiplies it into cores |
| `executor_cores` | required | `spark.executor.cores` |
| `executor_mem_mb` | required | `spark.executor.memory` |
| `driver_cores` | `1` | `spark.driver.cores` |
| `driver_mem_mb` | `2048` | `spark.driver.memory`, and the local stack's `--driver-memory` |
| `trigger_interval` | required | `trigger(processingTime=…)` |
| `max_offsets_per_trigger` | unset | `maxOffsetsPerTrigger`; the option is omitted when unset |
| `distribution_mode` | required | `none` / `hash` / `range`, as a write option |
| `fanout` | `false` | `fanout-enabled` write option |
| `machine_type` | unset | cost column and placement |
| `extra_spark_conf` | `{}` | applied last, so it overrides anything above |


Shuffle partitions default to `executors × executor_cores`, avoiding Spark's
200-way default shuffle and excess small files on smaller fleets.

## Avro and timestamp handling

The corpus encodes `event_time` as Avro `timestamp-millis`. Spark normally reads
that as a zoned instant, but the Iceberg table requires a zoneless `timestamp`.
The renderer changes the reader schema annotation to `local-timestamp-millis`,
which makes `from_avro` return `TimestampNTZ`. Both annotations encode the same
`long`, so the bytes and millisecond value are preserved when widened to the
table's microsecond precision.

With `kafka.value_encoding: confluent`, the job removes the five-byte header
using `substring(value, 6, length(value) - 5)` before the same decode. It needs
no registry client: each run registers one schema and already has the matching
reader schema. Staging still requires `site.kafka.schema_registry` to obtain the
schema ID. See [wire framing](../../docs/adding-an-engine.md).

## Configuration constraints

- **Nullability:** `from_avro` marks its output nullable regardless of the Avro
  schema. The job sets `check-nullability=false` to bypass Iceberg's static
  comparison with required table columns. Spark's runtime `AssertNotNull`
  checks still fail the run if a required value is null.
- **Trigger intervals:** use a count, a space, and a full unit word, such as
  `10 seconds`, `500 milliseconds`, `2 minutes`, or `1 hour`. Spark 3.5.9 rejects
  abbreviations such as `10s`, `500ms`, `1m`, `1h`, and `10 sec`, as well as
  `10seconds`. The renderer validates this before staging.
- **Two storage clients:** Iceberg FileIO writes table data using
  `spark.sql.catalog.ice.s3.*`; Hadoop S3A writes checkpoints using
  `spark.hadoop.fs.s3a.*`. Local S3A settings are derived from the catalog
  properties. Checkpoint URIs use `s3a://`, which stock Spark recognizes.
- **Stable checkpoints:** the job sets the writer's `checkpointLocation` option.
  The session setting `spark.sql.streaming.checkpointLocation` is a parent path
  that gives unnamed queries random subdirectories. Reusing that setting after
  a restart could replay already committed rows.
- **Environment references:** `spark-submit` does not resolve `${env:NAME}` in
  its properties file. The renderer rejects such references there. Kafka
  options in `job.json` can contain references resolved by the job. Kubernetes
  storage access uses pod identity; only local sites render static S3A keys.
- **Catalog:** only Iceberg REST catalogs are supported. Spark renders
  `spark.sql.catalog.ice.type = rest` alongside the `SparkCatalog` class.
  Kafka security properties become `kafka.*` options, with MSK IAM translation.
  See the [run specification](../../docs/run-spec.md) for translated keys.
