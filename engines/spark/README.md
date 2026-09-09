# Spark engine

Stock Apache Spark 3.5.9 driven by Structured Streaming. `knobs.py` renders the
properties, the reader schema and the job document; `stream_to_iceberg.py` reads
those three and starts one query. No source, sink or serializer is written here,
so a Spark result is Spark's.

## What runs

| Piece | What it is |
|---|---|
| Image | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu`, multi-arch |
| Source | `spark-sql-kafka-0-10_2.12:3.5.9` + `spark-token-provider-kafka-0-10_2.12:3.5.9`, an unshaded `kafka-clients:3.4.1` and `commons-pool2:2.11.1` |
| Avro | `spark-avro_2.12:3.5.9` — `from_avro` is here, not in the Avro core jar the image ships |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier, so its AWS SDK v2 comes with it) |
| Sink | `iceberg-spark-runtime-3.5_2.12:1.9.2` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Checkpoints | `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.779` |

The three codecs `kafka-clients` needs — zstd, lz4, snappy — are already in the
image, because Spark uses the same three. `commons-pool2` is not: the image ships
`commons-pool` **1.x**, a different package, and the consumer pool needs 2.x. The
client is unshaded for the reason the `Dockerfile` gives: Amazon MSK's
`IAMClientCallbackHandler` implements the unshaded `AuthenticateCallbackHandler`,
which a shaded client cannot load.

## The local stack

The `spark` profile is one service. `--master local[executors × executor_cores]`
runs the executors as threads inside the driver's own JVM, so there is no second
container to scale and `executor_mem_mb` is never spent: `driver_mem_mb` is the
heap the whole fleet decodes and buffers Parquet in. That costs a cluster-shaped
run and buys a stack that fits on a laptop. Set `RUN_DIR` to the staged run
directory or the mount fails; `job.env`'s `LOCAL_CORES` and `DRIVER_MEM_MB` are
sourced into this one compose command and nothing else reads them.

Readiness is read off the driver's own UI, because **Spark 3.5 publishes no REST
resource for Structured Streaming**: `api/v1/applications/<id>/streaming` is the
DStream one, registered only where a `StreamingContext` exists, so it answers 404.
`wait_for_spark_query` polls `/api/v1/applications` for an application named after
the run, then the Structured Streaming tab for an active query — so a run setting
`spark.sql.streaming.ui.enabled=false` is never ready.

## On Kubernetes

A run is one `SparkApplication` for the Kubeflow spark-operator in `cluster` mode
plus the ConfigMap its pods mount at `/opt/bench/run`. The driver runs as
`site.kubernetes.spark_service_account`, the account the cloud's identity is bound
to — so the chart's own spark account and RBAC are off and the namespace manifest
grants ours the rules a driver needs to create its executors. Both halves ask for as
much CPU as they cap at, so their pods are Guaranteed, and both carry the region
under its two names because an executor signs its own broker token and writes the
table's files ([`../../docs/pitfalls.md`](../../docs/pitfalls.md)).

`verify-spark` reads the driver's UI through a tunnel and the pods through
`kubectl`: one application named after the run under either spelling of it (the
object's name is the lowercased run id and the submitted `spark.app.name` is
not), every setting the fleet is sized by as the driver reports it, and a driver
plus `executors` executors `Running` and Guaranteed. Not the trigger interval,
which nothing can read — see [`../../docs/pitfalls.md`](../../docs/pitfalls.md).

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

`spark.sql.shuffle.partitions` is `executors × executor_cores` rather than
Spark's default two hundred: under `hash` or `range` the shuffle width is the
writer count, and two hundred writers per commit is that many files.

## The timestamp rewrite

`from_avro` maps the corpus's `timestamp-millis` `event_time` to a zoned instant,
which would want a `timestamptz` column rather than the table's zoneless
`timestamp`. So `knobs.py` renders `reader-schema.avsc` as the corpus schema with
every `timestamp-millis` rewritten to `local-timestamp-millis`. Both annotate the
same `long` and encode identically, so the rewrite reads the corpus's bytes
unchanged and yields `TimestampNTZ`, widened into the table's microsecond column
and never rounded. A test pins it.

## Traps

**`from_avro` returns a nullable struct whatever the schema says.** Every column
read out of it is therefore nullable, and Iceberg's static write check refuses a
table whose columns are required — one *"should be required, but is optional"*
per column, with the topic and the table already created. The write options carry
`check-nullability=false` for that reason. What is skipped is a schema comparison
before any row is read; Spark's own `AssertNotNull` per required column remains,
and fails the run if a null arrives. Not a knob: a consequence of the decode.

**A trigger interval is a count and a whole unit word.** `Trigger.ProcessingTime`
parses its argument as a SQL interval, which takes `10 seconds`,
`500 milliseconds`, `2 minutes`, `1 hour` and rejects `10s`, `500ms`, `1m`, `1h`,
`10 sec` and `10seconds` alike (checked against 3.5.9). `knobs.py` refuses the
abbreviations up front: a query that dies at its first micro-batch does so
minutes into a staged run.

**One object store, two spellings.** The table's data files go through Iceberg's
own FileIO, configured by `spark.sql.catalog.ice.s3.*`; the query's checkpoints
go through a Hadoop filesystem, configured by `spark.hadoop.fs.s3a.*`. Both are
needed and neither replaces the other, so `knobs.py` renders the S3A half from
the same catalog properties. `s3://` is a vendor alias a stock Spark leaves
unbound, so the checkpoint path is `s3a://`.

**The checkpoint location is the writer's option, never the session conf.**
`spark.sql.streaming.checkpointLocation` is a *parent* path:
`StreamingQueryManager.createQuery` joins it with the query's name, and a query
with no `queryName` gets a fresh random subdirectory on every start. A driver
restart would then resume from no state, read the topic from `earliest` again and
duplicate every row already committed — the column exactness measures — leaving
an orphan checkpoint behind. The `checkpointLocation` write option is used as it
stands.

**`spark-submit` reads `spark-defaults.conf` itself,** so a `${env:NAME}` in it
is not resolved — unlike Flink, whose submitter resolves one. Only a site with no
`kubernetes` block renders those four S3A lines; on a cluster no static key is.

## Confluent values, and catalog properties

Under `kafka.value_encoding: confluent` the job drops the five-byte header —
`substring(value, 6, length(value) - 5)` — and decodes the rest against
`reader-schema.avsc`, so both encodings share one decode. There is no registry
client: one run registers exactly one schema, so every header names the same id
and the reader schema is already the writer's. Staging still needs
`site.kafka.schema_registry` to get that id; the framing is in
[`../../docs/adding-an-engine.md`](../../docs/adding-an-engine.md).

A run needs an Iceberg **REST** catalog; any other `type` is refused at stage
time. The translated keys are in
[`../../docs/run-spec.md`](../../docs/run-spec.md): Spark renders `type` as
`spark.sql.catalog.ice.type = rest` beside the `SparkCatalog` class, and takes
`site.kafka.security` as `kafka.*` verbatim bar that file's MSK IAM translation.

## Sizing

A local Spark figure says the pieces agree and nothing about speed: the stack
shares one machine with the harness, the broker and the object store, and
`executor_mem_mb` is not spent. No local Spark run is recorded here yet; the
measured cluster run is
[`../../docs/examples/aws-smoke-spark/`](../../docs/examples/aws-smoke-spark/).
