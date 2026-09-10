# Flink engine

The engine runs Apache Flink 1.20.1 SQL with released Kafka, Avro, and Iceberg
connectors. `knobs.py` renders SQL and settings; `job.py` submits them through
the PyFlink Table API.

## Runtime dependencies

| Piece | What it is |
|---|---|
| Image | `flink:1.20.1-scala_2.12-java17`, **`linux/amd64`** |
| Source | `flink-connector-kafka:3.4.0-1.20` + `kafka-clients:3.4.0` and its codecs (`zstd-jni:1.5.2-1`, `lz4-java:1.8.0`, `snappy-java:1.1.8.4`) |
| Avro | `flink-sql-avro-confluent-registry:1.20.1`; one shaded jar registers both `avro` and `avro-confluent` |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier includes AWS SDK v2) |
| Sink | `iceberg-flink-runtime-1.20:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Classpath | `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6` — the shaded client pair, not `hadoop-common` and siblings |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar` |
| PyFlink | `apache-flink==1.20.1`, `pyyaml==6.0.2` |

The pinned PyFlink release requires amd64, so local arm64 runs use emulation.
The shaded Hadoop client jars supply Flink's Hadoop dependencies. Kafka must
remain unshaded so MSK's `IAMClientCallbackHandler` can load the expected
interface. See `Dockerfile` for the classpath constraints.

## Local runs

The `flink` Compose profile starts the cluster; `flink-job` submits one run
detached. Set `RUN_DIR` to the staged directory so the submitter can mount it.

Catalog and Kafka properties appear in `job.sql`. Use `${env:NAME}` for
credentials so the submitter resolves them from the container environment
instead of writing values into the rendered file. See
[pitfalls](../../docs/pitfalls.md).

Local runs validate integration, not engine performance. The
[recorded smoke run](../../docs/examples/smoke-flink/) absorbed 96.9% of the
5 MB/s offer, with 17.6 s p95 freshness and a 4.87 s drain. It used eight slots
and an emulated engine image on the local stack.

## Kubernetes runs

Sites with a `kubernetes` block also render `flinkdeployment.yaml` and
`flink-job-configmap.yaml`, using the tag of the pushed image. The ConfigMap
mounts `job.sql` and `flink-conf.yaml` at `/opt/bench/run`.

The deployment uses `mode: standalone` to honor the `taskmanagers` count.
Native mode would derive that count from job parallelism. The pod selector
requires `kubernetes.io/arch: amd64`, overriding any conflicting site selector.

## Knobs

| Knob | Default | Effect |
|---|---|---|
| `taskmanagers` | required | TaskManager count |
| `slots` | required | slots per TaskManager |
| `tm_cpu` | required | vCPU per TaskManager; Kubernetes CPU request |
| `tm_mem_mb` | required | `taskmanager.memory.process.size` |
| `jm_cpu` | `1` | JobManager vCPU; Kubernetes CPU request |
| `jm_mem_mb` | `2048` | `jobmanager.memory.process.size` |
| `checkpoint_interval` | required | `execution.checkpointing.interval` |
| `min_pause` | `0s` | `execution.checkpointing.min-pause` |
| `unaligned_checkpoints` | `false` | `execution.checkpointing.unaligned.enabled` |
| `source_parallelism` | `1` | Kafka readers; must be `<=` topic partitions |
| `max_parallelism` | `4 * taskmanagers * slots` | `pipeline.max-parallelism` |
| `distribution_mode` | required | `none` / `hash` / `range`, as a sink hint |
| `machine_type` | unset | cost column and placement |
| `extra_flink_conf` | `{}` | applied last, so it overrides anything above |

The benchmark requires `EXACTLY_ONCE` checkpointing. Verification rejects a
weaker mode, including one set through `extra_flink_conf`. CPU knobs size
Kubernetes requests and contribute to cost; local Compose does not apply them.

## Reader and writer parallelism

The pinned Kafka connector has no `scan.parallelism` option. The renderer sets
`parallelism.default` to `source_parallelism` so readers inherit it, then applies
the Iceberg sink hint `'write-parallelism' = taskmanagers * slots` when the
writer count differs. Flink 1.20.1 enables dynamic table options by default,
allowing the hint to take effect.

Verification reads the live graph to confirm reader and writer counts. The
Iceberg committer must have parallelism 1 because commits are serialized.

## Staging verification

`verify.py` reads the JobManager REST API through a port-forward on Kubernetes
or the Compose network locally. It checks:

- A RUNNING attempt named for the run.
- Checkpoint interval and minimum pause, including explicit configuration
  overrides, plus exactly-once mode.
- Sources at `source_parallelism`, each Iceberg writer at
  `taskmanagers * slots`, and the committer at 1.
- The requested number of registered TaskManagers.

Exit code 3 reports drift. Exit code 2 means the response could not be read;
cluster staging retries it three times.

## Catalog and wire encoding

Only Iceberg REST catalogs are supported. Flink renders `'type' = 'iceberg'`
and `'catalog-type' = 'rest'`. Kafka security properties become `properties.*`
options, with MSK IAM translation. See the
[run specification](../../docs/run-spec.md) for translated keys.

The source DDL uses these format options for each wire encoding:

| `kafka.value_encoding` | `'format'` | Options rendered beside it |
|---|---|---|
| `avro` (the default) | `avro` | `'avro.timestamp_mapping.legacy' = 'false'` |
| `confluent` | `avro-confluent` | `'avro-confluent.url'` from `site.kafka.schema_registry.url`, plus `'avro-confluent.basic-auth.credentials-source' = 'USER_INFO'` and `'avro-confluent.basic-auth.user-info'` where the site names a credential |

Confluent decoding looks up writer schemas by the ID in each five-byte header,
so both staging and the renderer require a registry. The shipped
`runs/smoke-flink-confluent.yaml` differs from `smoke-flink.yaml` only in encoding.

The source uses `TIMESTAMP(3)` for the corpus's millisecond `event_time`.
This is the highest precision `avro-confluent` can plan with its legacy Avro
mapping. That format has no option to disable the mapping; the plain format's
`avro.timestamp_mapping.legacy` option is not accepted. Supplying a schema does
not bypass the DDL conversion because `RegistryAvroFormatFactory` evaluates it
eagerly through `Optional.orElse`.

Plain Avro maps the column to `local-timestamp-millis`; Confluent Avro maps it
to `timestamp-millis`. Both encode the same `long`. Logical annotations do not
change the bytes, and Java Avro resolves these records by field name. Both
formats therefore preserve the producer's millisecond value when writing the
table's microsecond timestamp.
