# Flink engine

Stock Apache Flink 1.20.1 driven by SQL. `knobs.py` renders the script and the
settings, `job.py` submits them with the PyFlink Table API, and no source, sink
or serializer is written here — so a Flink result is Flink's.

## What runs

| Piece | What it is |
|---|---|
| Image | `flink:1.20.1-scala_2.12-java17`, **`linux/amd64`** |
| Source | `flink-connector-kafka:3.4.0-1.20` + `kafka-clients:3.4.0` and its codecs (`zstd-jni:1.5.2-1`, `lz4-java:1.8.0`, `snappy-java:1.1.8.4`), Avro via `flink-sql-avro-confluent-registry:1.20.1` — one shaded jar registering both the `avro` and the `avro-confluent` format |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier, so its AWS SDK v2 comes with it) |
| Sink | `iceberg-flink-runtime-1.20:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Classpath | `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6` — the shaded client pair, not `hadoop-common` and siblings |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar` |
| PyFlink | `apache-flink==1.20.1`, `pyyaml==6.0.2` |

The image is amd64 because **PyFlink publishes no Linux aarch64 wheel in any
release**; on arm64 the stack runs emulated. Two jar choices are load-bearing,
and the `Dockerfile` carries the reasoning where a maintainer would change them:
Hadoop arrives shaded because Flink installs its `HadoopModule` the moment it
finds Hadoop on the classpath, and the Kafka client is deliberately **unshaded**
because Amazon MSK's `IAMClientCallbackHandler` implements the unshaded
`AuthenticateCallbackHandler`, which a shaded client can never load.

`job.sql` carries whatever `site.catalog.props` and `site.kafka.security` hold,
so a credential written there literally is in the file; write `${env:NAME}` and
the submitter resolves it from the container's environment instead
([`../../docs/pitfalls.md`](../../docs/pitfalls.md)). Locally, the `flink`
profile starts the cluster and `flink-job` submits one run detached, mounting
`$RUN_DIR` — which must be the staged run directory or the mount fails.

## On Kubernetes

A site that declares a `kubernetes` block gets two more rendered files, and
`render` then needs the tag of the image that was pushed: `flinkdeployment.yaml`,
one `FlinkDeployment` per run named by the run id, and
`flink-job-configmap.yaml`, which mounts `job.sql` and `flink-conf.yaml` at
`/opt/bench/run`.

`mode: standalone`, so the operator starts the `taskmanagers` the knobs ask for;
native mode would size the fleet from the job's parallelism and leave that knob
unhonoured. The pod pins `kubernetes.io/arch: amd64` over whatever the site's
`node_selector` says, since the image has no aarch64 PyFlink to run.

## Knobs

| Knob | Default | Effect |
|---|---|---|
| `taskmanagers` | required | taskmanager count |
| `slots` | required | slots per taskmanager |
| `tm_cpu` | required | vCPU per taskmanager (cost column only) |
| `tm_mem_mb` | required | `taskmanager.memory.process.size` |
| `jm_cpu` | `1` | jobmanager vCPU (cost column only) |
| `jm_mem_mb` | `2048` | `jobmanager.memory.process.size` |
| `checkpoint_interval` | required | `execution.checkpointing.interval` |
| `min_pause` | `0s` | `execution.checkpointing.min-pause` |
| `unaligned_checkpoints` | `false` | `execution.checkpointing.unaligned.enabled` |
| `source_parallelism` | `1` | Kafka readers; must be `<=` topic partitions |
| `max_parallelism` | `4 * taskmanagers * slots` | `pipeline.max-parallelism` |
| `distribution_mode` | required | `none` / `hash` / `range`, as a sink hint |
| `machine_type` | unset | cost column and placement |
| `extra_flink_conf` | `{}` | applied last, so it overrides anything above |

`execution.checkpointing.mode` is always `EXACTLY_ONCE`: it is the promise
duplication is scored against, so it is not a knob. `tm_cpu`, `jm_cpu` and
`machine_type` are carried, not consumed.

## Source parallelism: the path taken

`flink-connector-kafka:3.4.0-1.20` **has no `scan.parallelism` option**
(verified against the jar: `KafkaConnectorOptions` declares `SINK_PARALLELISM`
and no `SCAN_PARALLELISM`; FLINK-33262 is not in this release), and an
unsupported `WITH` key fails validation. So the fallback is taken:
`parallelism.default` is `source_parallelism`, which the readers inherit, and
the sink carries `'write-parallelism' = taskmanagers * slots` — emitted only
when it differs from the default — so the writers use the whole fleet. The hint
reaches the table through `table.dynamic-table-options.enabled`, `true` by
default in 1.20.1 (verified in `TableConfigOptions`).

That the hint applies is not assumed: `verify.py` reads the live graph's three
parallelisms on every run, and a writer sitting at the reader count is how a
dropped hint would look. The committer is a singleton by construction — the sink
serialises commits whatever the writers do. A fleet whose slots equal
`source_parallelism` emits no hint at all, the two numbers coinciding.

## What staging verifies

`verify.py` reads the running job back and refuses the run on any line it
prints — `stage.sh` through a port-forward, `smoke.sh` over the stack's network.
Exit 3 is drift; 2 is unread, retried three times:

- the job named by the run id is RUNNING, on its live attempt;
- `interval` and `min_pause` match the knobs in ms, or an
  `extra_flink_conf` override of either; `mode` is always `exactly_once`;
- the source at `source_parallelism`, each `IcebergStreamWriter` at
  `taskmanagers * slots`, the committer at 1;
- the cluster has registered `taskmanagers` taskmanagers.

## Sizing

Slots, not memory, are the dial for freshness on this shape of workload:
checkpoint state is a few kilobytes, and what a checkpoint spends its seconds on
is writers flushing Parquet. On the smoke corpus's 5 MB/s, four slots absorb
about three quarters of the offered rate — draining every row exactly and still
missing a 60 s p95 bound — which is why `runs/smoke-flink.yaml` asks for eight.
Its recorded run is in
[`../../docs/examples/smoke-flink/`](../../docs/examples/smoke-flink/): 96.9%
absorbed, p95 17.6 s, drain 4.87 s. Both figures come from the local stack with
the engine image emulated, so neither is a result.

## Catalog properties, and the wire format

A run needs an Iceberg **REST** catalog; any other `type` in
`site.catalog.props` is refused at stage time. The translated keys are in
[`../../docs/run-spec.md`](../../docs/run-spec.md); Flink renders `type` as
`'type' = 'iceberg'` beside `'catalog-type' = 'rest'`, and takes
`site.kafka.security` as `properties.*` verbatim bar that file's MSK IAM
translation.

A run's `kafka.value_encoding` picks the source's format, and both formats read
the same DDL:

| `kafka.value_encoding` | `'format'` | Options rendered beside it |
|---|---|---|
| `avro` (the default) | `avro` | `'avro.timestamp_mapping.legacy' = 'false'` |
| `confluent` | `avro-confluent` | `'avro-confluent.url'` from `site.kafka.schema_registry.url`, plus `'avro-confluent.basic-auth.credentials-source' = 'USER_INFO'` and `'avro-confluent.basic-auth.user-info'` where the site names a credential |

`avro-confluent` resolves each value's writer schema by the id in that value's
five-byte header, so the registry is not optional for it: `knobs.render_sql`
refuses a `confluent` run against a site that declares none, and staging
refuses it earlier still. `runs/smoke-flink-confluent.yaml` is the shipped run;
it is `smoke-flink.yaml` plus the encoding, and a test holds the pair to that.

The corpus's `event_time` is what makes one DDL serve both. It is Avro
`timestamp-millis`, so the column is `TIMESTAMP(3)` — and 3 is the widest
timestamp `avro-confluent` can plan: it converts the DDL to a reader schema
under Flink's legacy Avro timestamp mapping and declares no option to disable
that mapping (`avro.timestamp_mapping.legacy` belongs to the plain `avro`
format, and an unknown `avro-confluent.*` key fails validation). Its own
`schema` option is no way round it either, since `RegistryAvroFormatFactory`
passes the DDL conversion to `Optional.orElse`, which evaluates eagerly.

The two formats then map that column to different logical types —
`local-timestamp-millis` under `avro`, `timestamp-millis` under
`avro-confluent` — and it makes no difference: both annotate the same `long`, an
Avro logical type is not on the wire, and Java Avro resolves a writer against a
reader of a different record name by field name. So either format commits the
millisecond the producer wrote, widened into the table's microsecond `timestamp`.
