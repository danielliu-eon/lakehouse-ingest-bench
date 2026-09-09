# Flink engine

Stock Apache Flink 1.20.1 driven by SQL. `knobs.py` renders the script and the
settings, `job.py` submits them with the PyFlink Table API, and no source, sink
or serializer is written here — so a Flink result is Flink's.

## What runs

| Piece | What it is |
|---|---|
| Image | `flink:1.20.1-scala_2.12-java17`, **`linux/amd64`** |
| Source | `flink-connector-kafka:3.4.0-1.20` + `kafka-clients:3.4.0` and its codecs (`zstd-jni:1.5.2-1`, `lz4-java:1.8.0`, `snappy-java:1.1.8.4`), Avro via `flink-sql-avro:1.20.1` with `avro.timestamp_mapping.legacy = false` — the legacy default caps SQL `TIMESTAMP` at milliseconds, so a `TIMESTAMP(6)` column cannot be planned at all |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier, so its AWS SDK v2 comes with it) |
| Sink | `iceberg-flink-runtime-1.20:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Classpath | `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6` — Iceberg resolves a table through Hadoop's `Configuration` whichever FileIO reads it |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar` |
| PyFlink | `apache-flink==1.20.1`, `pyyaml==6.0.2` |

The image is amd64 because **PyFlink publishes no Linux aarch64 wheel in any
release**; on arm64 the stack runs emulated, which checks a run end to end but
does not measure one. `job.sql` carries whatever `site.catalog.props` and
`site.kafka.security` hold, so a credential written there literally is in the
file: it is a config file, not the publishable record — `facts.json` is that.
Write the credential as `${env:NAME}` and the file names it instead, leaving the
submitter to read it out of the container's environment.

On **Apple Silicon** that emulation is why the smoke takes appreciably longer
than its native equivalent, and why `runs/smoke-flink.yaml` asks for two
TaskManagers to absorb a 5 MB/s corpus. Replacing `job.py` with a Java SQL
runner would retire the platform pin and make this image multi-arch; that is a
planned follow-up, and `script.py` is stdlib-only so the submitter can be
swapped without touching the rest.

Hadoop arrives as the **shaded client pair** and not as `hadoop-common` plus
siblings. Flink installs its `HadoopModule` the moment it finds Hadoop on the
classpath, and installing it initializes `UserGroupInformation` — which needs
commons-configuration2, guava and re2j behind it. `hadoop-client-api` and
`hadoop-client-runtime` are one shading run over exactly that closure, so the
transitive set never has to be enumerated jar by jar.

The Kafka client is the **plain connector plus an unshaded `kafka-clients`**
and not the SQL uber jar, which is the same connector with
`org.apache.kafka` relocated to `org.apache.flink.kafka.shaded.org.apache.kafka`
and no unshaded copy left. Amazon MSK's `IAMClientCallbackHandler` implements
the unshaded `AuthenticateCallbackHandler`, so a shaded client can never load
it — the two class names never meet. Unshading costs the codec jars, which the
uber jar bundled: the producer picks the compression and the consumer has to
decompress it.

The `flink` profile starts the cluster; `flink-job` submits one run detached,
mounting `$RUN_DIR` — set it to the staged run directory, or the mount fails.

## On Kubernetes

A site that declares a `kubernetes` block gets two more rendered files, and
`render` then needs the tag of the image that was pushed:

| File | What it is |
|---|---|
| `flinkdeployment.yaml` | one `FlinkDeployment` per run, named by the run id |
| `flink-job-configmap.yaml` | `job.sql` and `flink-conf.yaml`, mounted at `/opt/bench/run` |

`mode: standalone`, so the operator starts the `taskmanagers` the knobs ask
for; native mode would size the fleet from the job's parallelism instead and
leave that knob unhonoured. The pod is pinned to `kubernetes.io/arch: amd64`
over whatever the site's `node_selector` says, because the image has no
aarch64 PyFlink to run. `AWS_REGION` is set on the container only where the
site names an `aws_region`, and it is what the MSK token signer and S3 read
when nothing else names a region for them.

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
| `machine_type` | unset | cost column and phase-2 placement |
| `extra_flink_conf` | `{}` | applied last, so it overrides anything above |

`execution.checkpointing.mode` is always `EXACTLY_ONCE`: it is the promise
duplication is scored against, so it is not a knob. `tm_cpu`, `jm_cpu` and
`machine_type` are carried, not consumed — `flink.env` sizes the containers.

## Source parallelism: the path taken

`flink-connector-kafka:3.4.0-1.20` **has no `scan.parallelism` option**
(verified against the jar: `KafkaConnectorOptions` declares `SINK_PARALLELISM`
and no `SCAN_PARALLELISM`; FLINK-33262 is not in this release), and an
unsupported `WITH` key fails validation. So the fallback is taken:

- `parallelism.default` = `source_parallelism`, which the readers inherit.
- The sink carries `'write-parallelism' = taskmanagers * slots`, emitted only
  when it differs from the default, so the writers use the whole fleet.

The hint reaches the table through `table.dynamic-table-options.enabled`,
`true` by default in 1.20.1 (verified in `TableConfigOptions`).
`flink-conf.yaml`'s cluster-shaped keys are informational on a job config; the
stack applies them from `flink.env`.

`scripts/smoke.sh` confirms the rest on a live job. On `runs/smoke-flink.yaml`
— 2 taskmanagers, 4 slots, `source_parallelism: 4` — the insert renders
`'write-parallelism' = '8'` and the graph comes back with
`Source: kafka_source -> ConstraintEnforcer` at 4, `IcebergStreamWriter` at 8
and `IcebergFilesCommitter -> IcebergSink` at 1, so the hint does apply. A
writer sitting at the reader count on such a fleet is how one that did not
would look. The committer is a singleton by construction — the sink serialises
commits whatever the writers do. `GET /jobs/<id>/checkpoints/config` reports
`interval 10000`, `min_pause 2000`, `mode exactly_once`, matching the knobs.

A fleet whose slots equal `source_parallelism` emits no hint at all, because
the two numbers coincide and there is nothing to say.

## What staging verifies

`verify.py` reads those documents back and refuses the run on any line it
prints — `stage.sh` through a port-forward to `svc/<run>-rest`, `smoke.sh`
over the stack's network. Exit 3 is drift; 2 is unread, retried three times:

- the job named by the run id is RUNNING, on its live attempt;
- `interval` and `min_pause` match the knobs in ms, or an
  `extra_flink_conf` override of either; `mode` is always `exactly_once`;
- the source at `source_parallelism`, each `IcebergStreamWriter` at
  `taskmanagers * slots`, the committer at 1;
- the cluster has registered `taskmanagers` taskmanagers.

## Sizing

Two figures worth carrying into a real spec, both measured on the local stack
against the `smoke` corpus's 5 MB/s, with the engine image emulated on arm64:

| Fleet | Result |
|---|---|
| 1 taskmanager, 4 slots | absorbs ~77% of the offered rate. Drains every row exactly, freshness p95 ~95 s against a 60 s bound |
| 2 taskmanagers, 8 slots | absorbs ~98%. Freshness p95 ~15 s, drain ~10 s |

Checkpoint duration is what the slots are spent on: state is a few kilobytes,
and the 5-to-17 seconds a checkpoint takes is writers flushing Parquet. So
slots, not memory, are the dial for freshness on this shape of workload — and a
freshness breach with clean exactness means the fleet was too small for the
offer, which is the thing the benchmark exists to detect.

## Catalog properties

A run needs an Iceberg **REST** catalog; any other `type` in
`site.catalog.props` is refused at stage time. pyiceberg's names carry through
unchanged bar one, and `type` is translated rather than passed on:

| `site.catalog.props` | Rendered |
|---|---|
| `type` (`rest`, or absent) | `'type' = 'iceberg'`, `'catalog-type' = 'rest'` |
| `s3.region` | `client.region` |
| `s3.{endpoint,access-key-id,secret-access-key,path-style-access}` | unchanged |
| `site.warehouse` on `s3://` / `gs://` | adds the matching `'io-impl'`. The catalog's own `warehouse` is not always a location — a Glue REST endpoint takes the account id there — so the scheme is read off the site's warehouse instead |

## Confluent wire format: refused, and why

`kafka.value_encoding: confluent` is refused at stage time for a Flink run —
`knobs.validate` raises, before a topic exists — because Flink 1.20.1 cannot
read such a topic without changing the data. Two upstream facts, the second of
which rules out the way round the first:

- **`avro-confluent` cannot plan a `TIMESTAMP(6)` column.** It builds its
  reader schema from the DDL under Flink's legacy Avro timestamp mapping, which
  caps SQL `TIMESTAMP` at milliseconds, and it declares no option to disable
  that mapping (`avro.timestamp_mapping.legacy` belongs to the plain `avro`
  format; an unknown `avro-confluent.*` key fails validation). Its own `schema`
  option does not help: `RegistryAvroFormatFactory` passes the DDL conversion
  to `Optional.orElse`, which evaluates eagerly, so the conversion runs — and
  throws — even when a reader schema is stated. Still the case on Flink master.
- **The column cannot be rebuilt from a `BIGINT`.** Declaring it `BIGINT` and
  converting in the insert needs `TO_TIMESTAMP_LTZ`, whose runtime accepts
  second and millisecond precision only, and Flink SQL has no exact
  microsecond route. The run would commit event times truncated to
  milliseconds — a column the corpus published, changed by the reader, in the
  column freshness is measured on.

Every corpus carries `event_time` among its reserved fields, so this is not a
workload one can avoid. Offer the corpus to Flink as `avro`, which is what
`runs/smoke-flink.yaml` does; a Confluent-framed run belongs to an engine that
reads the five-byte header itself — `runs/smoke-external-confluent.yaml` is
that spec. The harness half is unaffected: staging registers the schema and
the producer frames every value whatever engine is reading.

`site.kafka.security` reaches the source as `properties.*` verbatim, with
one translation. `sasl.mechanism: OAUTHBEARER` beside the harness's own
`aws.region` is its MSK IAM signal, and the Java client spells that
authentication `AWS_MSK_IAM` with the `IAMLoginModule` and its callback
handler — so those four properties are rendered and the pseudo-key is not.
Every other key still passes through. Neither form carries a credential:
the module signs a token from whatever the pod's own identity is.
