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

The three compression codecs `kafka-clients` needs — zstd, lz4, snappy — are
already in the image, because Spark uses the same three. `commons-pool2` is not:
the image ships `commons-pool` **1.x**, a different package, and the Kafka
source's consumer pool needs 2.x.

The client is the **plain connector plus an unshaded `kafka-clients`** rather
than an uber jar with `org.apache.kafka` relocated: Amazon MSK's
`IAMClientCallbackHandler` implements the unshaded
`AuthenticateCallbackHandler`, so a shaded client can never load it.

## The local stack

The `spark` profile is one service. `--master local[executors × executor_cores]`
runs the executors as threads inside the driver's own JVM, so there is no second
container to scale and `executor_mem_mb` is never spent: `driver_mem_mb` is the
heap the whole fleet decodes and buffers Parquet in. That costs a cluster-shaped
run and buys a stack that fits on a laptop, which is all the smoke is for.
Set `RUN_DIR` to the staged run directory or the mount fails. `job.env`'s keys —
`LOCAL_CORES`, `DRIVER_MEM_MB` — are unprefixed as `flink.env`'s are: the file is
sourced into one engine's own compose command and nothing else reads it.

Readiness is read off the driver's own UI. **Spark 3.5 publishes no REST
resource for Structured Streaming** — `api/v1/applications/<id>/streaming` is the
DStream one, registered only where a `StreamingContext` exists, so it answers
404 — so `wait_for_spark_query` polls `/api/v1/applications` for an application
named after the run and then the Structured Streaming tab for an active query. A
run whose `extra_spark_conf` sets `spark.sql.streaming.ui.enabled=false` has
nothing left to read and is never seen as ready.

## On Kubernetes

A run is one `SparkApplication` for the Kubeflow spark-operator in `cluster`
mode plus the ConfigMap its pods mount at `/opt/bench/run`. The driver runs as
`site.kubernetes.spark_service_account`, the account the cloud's identity is
bound to — so the chart's own spark account and RBAC are off and the namespace
manifest grants ours the rules a driver needs to create its executors. Both
halves ask for as much CPU as they cap at, so their pods are Guaranteed: cores
the node can reclaim would make a measured rate the node's answer. Both carry
`AWS_REGION` and `AWS_DEFAULT_REGION`, because an executor signs its own broker
token and writes the table's files itself.

`verify-spark` reads the driver's UI through a tunnel to `<application>-ui-svc`
on 4040 and the pods through `kubectl`: one application named after the run,
every setting the fleet is sized by as the driver reports it, and a driver plus
`executors` executors `Running` and Guaranteed. **Not the trigger interval.**
It is a `writeStream` argument, Spark 3.5 publishes no REST resource for a
streaming query, and the topic is empty until `launch.sh` — so no batch has run
to read a cadence off. What is checked is that nothing pretends otherwise: a
property under `spark.sql.streaming.trigger` is a run moving its own cadence
somewhere nothing reads it.

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
| `machine_type` | unset | cost column and phase-2 placement |
| `extra_spark_conf` | `{}` | applied last, so it overrides anything above |

`spark.sql.shuffle.partitions` is set to `executors × executor_cores` rather
than left at Spark's default two hundred: under `hash` or `range` the shuffle
width is the writer count, and two hundred writers per commit is that many files.

## The timestamp rewrite

The corpus publishes `event_time` as Avro `timestamp-micros`; the table's column
is a zoneless Iceberg `timestamp`. `from_avro` maps `timestamp-micros` to a
zoned instant, which would want a `timestamptz` column instead. So `knobs.py`
renders `reader-schema.avsc` as the corpus schema with every `timestamp-micros`
rewritten to `local-timestamp-micros`. The two annotate the same `long` and
encode identically — an Avro logical type is not on the wire — so the rewrite
reads the corpus's bytes unchanged and yields `TimestampNTZ`. A test pins it.

## Confluent values

`kafka.value_encoding: confluent` frames every value as a zero magic byte, the
schema's registry id as a four-byte big-endian integer, and then the same Avro
binary a raw run carries. The job drops those five bytes —
`substring(value, 6, length(value) - 5)` — and decodes the rest against
`reader-schema.avsc`, so both encodings share one decode. No registry client:
one run registers exactly one schema, so every header in it names the same id
and the reader schema is already the writer's. Staging still needs
`site.kafka.schema_registry` to get that id, and refuses a `confluent` spec
without one. `runs/smoke-spark-confluent.yaml` is the shipped run.

## Traps

**`from_avro` returns a nullable struct whatever the schema says.** Every column
read out of it is therefore nullable, and Iceberg's static write check refuses a
table whose columns are required — one *"should be required, but is optional"*
per column, with the topic and the table already created. The write options
carry `check-nullability=false` for that reason. What is skipped is a schema
comparison that runs before any row is read; what remains is Spark's own
`AssertNotNull` on each required column, which fails the run if a null actually
arrives. Not a knob: a consequence of the decode, not an axis a run varies.

**A trigger interval is a count and a whole unit word.**
`Trigger.ProcessingTime` parses its argument as a SQL interval, which takes
`10 seconds`, `500 milliseconds`, `2 minutes`, `1 hour` and rejects `10s`,
`500ms`, `1m`, `1h`, `10 sec` and `10seconds` alike (checked against 3.5.9).
`knobs.py` refuses the abbreviations up front: a query that dies at its first
micro-batch does so minutes into a staged run.

**One object store, two spellings.** The table's data files go through
Iceberg's own FileIO, configured by `spark.sql.catalog.ice.s3.*`; the query's
checkpoints go through a Hadoop filesystem, configured by
`spark.hadoop.fs.s3a.*`. Both are needed and neither replaces the other, so
`knobs.py` renders the S3A half from the same catalog properties. `s3://` is a
vendor alias a stock Spark leaves unbound, so the checkpoint path is `s3a://`.

**The checkpoint location is the writer's option, never the session conf.**
`spark.sql.streaming.checkpointLocation` is a *parent* path:
`StreamingQueryManager.createQuery` joins it with the query's name, and a query
with no `queryName` gets a fresh random subdirectory on every start. A driver
restart would then resume from no state, read the topic from `earliest` again,
and duplicate every row already committed — which is the column exactness
measures — leaving an orphan checkpoint behind each attempt. The
`checkpointLocation` write option is used as it stands.

**A secret in a no-cluster site's catalog properties lands in the properties
file.** `spark-submit` reads `spark-defaults.conf` itself, so a `${env:NAME}`
placeholder is not resolved there — unlike Flink, whose submitter resolves it.
Only a site with no `kubernetes` block renders those four S3A lines; a cluster
reaches storage as the pod's own identity and no static key is rendered.

## Sizing

Measured on the local stack against the `smoke` corpus's 5 MB/s, natively on
arm64, with `executors: 2, executor_cores: 2` — four cores in one JVM:

| Corpus | Result |
|---|---|
| 30 s | absorbs ~63% at the offer's end, drains exactly in 7 s, freshness 12.5 s |
| 300 s | absorbs ~96%, freshness p50 7.0 s / p95 11.9 s against a 60 s bound, drain ~10 s |

The 30 s figure is a cold start showing through a run too short to amortise it.
Neither is a result: the stack shares a machine with the engine.

## Catalog properties

A run needs an Iceberg **REST** catalog; any other `type` in
`site.catalog.props` is refused at stage time. pyiceberg's names carry through
unchanged bar one:

| `site.catalog.props` | Rendered |
|---|---|
| `type` (`rest`, or absent) | `spark.sql.catalog.ice.type = rest`, beside the `SparkCatalog` class the name is bound to |
| `s3.region` | `client.region` |
| `s3.{endpoint,access-key-id,secret-access-key,path-style-access}` | unchanged |
| `site.warehouse` on `s3://` / `gs://` | adds the matching `io-impl`. The catalog's own `warehouse` is not always a location — a Glue REST endpoint takes the account id there — so the scheme is read off the site's warehouse instead |

`site.kafka.security` reaches the source as `kafka.*` verbatim, with one
translation. `sasl.mechanism: OAUTHBEARER` beside the harness's own `aws.region`
is its MSK IAM signal, and the Java client spells that authentication
`AWS_MSK_IAM` with the `IAMLoginModule` and its callback handler — so those four
properties are rendered and the pseudo-key is not. Every other key still passes
through. Neither form carries a credential: the module signs a token from
whatever the pod's own identity is.
