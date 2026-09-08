# Flink engine

Stock Apache Flink 1.20.1 driven by SQL. `knobs.py` renders the script and the
settings, `job.py` submits them with the PyFlink Table API, and no source, sink
or serializer is written here — so a Flink result is Flink's.

## What runs

| Piece | What it is |
|---|---|
| Image | `flink:1.20.1-scala_2.12-java17`, **`linux/amd64`** |
| Source | `flink-sql-connector-kafka:3.4.0-1.20`, Avro via `flink-sql-avro:1.20.1` with `avro.timestamp_mapping.legacy = false` — the legacy default caps SQL `TIMESTAMP` at milliseconds, so a `TIMESTAMP(6)` column cannot be planned at all |
| Sink | `iceberg-flink-runtime-1.20:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Classpath | `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6` — Iceberg resolves a table through Hadoop's `Configuration` whichever FileIO reads it |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar` |
| PyFlink | `apache-flink==1.20.1`, `pyyaml==6.0.2` |

The image is amd64 because **PyFlink publishes no Linux aarch64 wheel in any
release**; on arm64 the stack runs emulated, which checks a run end to end but
does not measure one. `job.sql` holds the catalog's credentials verbatim: it
is a config file, not the publishable record — `facts.json` is that.

Hadoop arrives as the **shaded client pair** and not as `hadoop-common` plus
siblings. Flink installs its `HadoopModule` the moment it finds Hadoop on the
classpath, and installing it initializes `UserGroupInformation` — which needs
commons-configuration2, guava and re2j behind it. `hadoop-client-api` and
`hadoop-client-runtime` are one shading run over exactly that closure, so the
transitive set never has to be enumerated jar by jar.

The `flink` profile starts the cluster; `flink-job` submits one run detached,
mounting `$RUN_DIR` — set it to the staged run directory, or the mount fails.

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

`flink-sql-connector-kafka:3.4.0-1.20` **has no `scan.parallelism` option**
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

The leg needs an Iceberg **REST** catalog; any other `type` in
`site.catalog.props` is refused at stage time. pyiceberg's names carry through
unchanged bar one, and `type` is translated rather than passed on:

| `site.catalog.props` | Rendered |
|---|---|
| `type` (`rest`, or absent) | `'type' = 'iceberg'`, `'catalog-type' = 'rest'` |
| `s3.region` | `client.region` |
| `s3.{endpoint,access-key-id,secret-access-key,path-style-access}` | unchanged |
| warehouse on `s3://` / `gs://` | adds the matching `'io-impl'` |
