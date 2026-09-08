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
| Classpath | `hadoop-{common,auth,hdfs-client,mapreduce-client-core}:3.3.6`, `woodstox-core:6.5.1`, `stax2-api:4.2.1`, `commons-logging:1.2` — Iceberg resolves a table through Hadoop's `Configuration` whichever FileIO reads it |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar` |
| PyFlink | `apache-flink==1.20.1`, `pyyaml==6.0.2` |

The image is amd64 because **PyFlink publishes no Linux aarch64 wheel in any
release**; on arm64 the stack runs emulated, which checks a run end to end but
does not measure one. `job.sql` holds the catalog's credentials verbatim: it
is a config file, not the publishable record — `facts.json` is that.

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
`true` by default in 1.20.1 (verified in `TableConfigOptions`). Two things to
confirm on the first live job: the graph at
`http://<jobmanager>:8081` should show the source at `source_parallelism` and
the writer at `taskmanagers * slots` (a writer at the reader count means the
hint did not apply), and `GET /jobs/<id>/checkpoints/config` should report the
interval asked for. `flink-conf.yaml`'s cluster-shaped keys are informational
on a job config; the stack applies them from `flink.env`.

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
