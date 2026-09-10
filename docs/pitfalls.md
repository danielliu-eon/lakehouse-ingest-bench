# Pitfalls

Traps that cost a run or, worse, produce a figure that looks fine. Each is a
mechanism, so it holds wherever the mechanism does.

## Table properties are create-time only

An engine's writer path applies Iceberg table properties to a table **it
created**. Set `table.properties` on a run whose `table.managed_by` is `harness`
and the harness creates the table with them, which is why they are the ones the
writer sees. Ask for `managed_by: engine` and the harness runs no DDL at all: it
hands your engine the equivalent `CREATE TABLE` in `facts.ddl`, and a property
your engine drops from that statement is a property the run does not have. Two
runs compared on file geometry must have got their properties the same way.

## A partition every writer touches is a small-file storm

An identity partition over many values, written by a fleet of several writers,
lands up to one file per value per writer at every commit — so a commit's files
are the partition's cardinality times the fleet's width, however few rows they
hold. A higher offer rate does not change that count, only how often the commit
happens.

It is the scorer's problem too, since it reads the id column of every one of
those files; `--read-workers` is how many of those reads overlap, and the pod is
sized for that width. A run whose `POLL` lines carry a `SLOW_POLL` is a reader
that cannot finish inside its own poll interval, and it needs more readers or a
coarser partition before its verdict means anything: the gate voids a reading
gone stale, and a voided run says nothing about the engine under it.

## A dropped setting fails nothing

Both engines accept configuration they then ignore, and neither submission
fails. So staging reads the effective state back out of the running engine and
refuses the run on any line that disagrees with the spec — a document you
submitted is a record of an action requested, not evidence the engine took it.

Two shapes of this are worth knowing before writing a spec:

- **Flink: a CRD field beats `spec.flinkConfiguration`.** The operator applies
  its own resource fields over the configuration, so a memory or CPU size
  overridden through `extra_flink_conf` would be honoured by the job that was
  submitted and discarded by the cluster running it. `knobs.py` reads those
  three fields back out of the effective configuration for that reason.
  `min_pause` is the other one to watch: it is `0s` unless a spec says
  otherwise, and a commit cadence set only by `checkpoint_interval` with no
  pause behind it is a different workload from one with a pause.
- **Spark: a cadence nothing reads.** The trigger interval is a `writeStream`
  argument, and Spark 3.5 publishes no REST resource for a streaming query, so
  no reading can confirm it. What is checked instead is that nothing pretends
  otherwise: a property under `spark.sql.streaming.trigger` is a run moving its
  own cadence somewhere nothing reads it, and it is refused.

## Spark's pods must be Guaranteed, and the heap is not the footprint

Both halves of a Spark fleet ask for as much CPU as they cap at, which is what
makes their pods Guaranteed. A Burstable pod's cores are a share the node may
reclaim, so a rate measured on one is the node's answer rather than the engine's.

`executor_mem_mb` is the JVM heap Spark is given, not the whole process's
footprint: a JVM's resident memory is its heap plus its own overhead, and the
heap is what decodes and buffers Parquet. Both shipped cloud specs double the
memory their local siblings ask for, for that reason. Under `--master local[N]`
the figure is not spent at all — see [`engines/spark/README.md`](../engines/spark/README.md).

## One 2-CPU pod per small node

The scorer requests 2 CPU, so does every producer shard, and so does each engine
worker at both shipped cloud specs' `tm_cpu: 2` / `executor_cores: 2`. A 4-vCPU
node — the `m6i.xlarge` the example `eksctl` config builds — reports under 4 CPU
allocatable once the kubelet's reservation is taken, and the cluster's own
daemonsets hold part of the rest, so each of these pods takes a node of its own.
The shipped hour-long runs place the scorer, five shards and a whole engine
fleet at once.

Nothing fails loudly when they do not fit: a pod nothing can schedule stays
Pending with no log, so `launch.sh` waits out `FIRST_POLL_WAIT_S` (300 s) and
reports a scorer that published no reading, over an empty log. The scheduler's own
`FailedScheduling ... Insufficient cpu` is in the pod's events, which the drivers
print beside that log, and `launch.sh` counts the nodes with room before it
applies anything. It warns rather than refuses — with an autoscaler, a Pending
pod is what buys the node. Without one, size the cluster first: see
[`../deploy/aws/README.md`](../deploy/aws/README.md) §Sizing the cluster.

## The measurement outlives the run, so expiry can still destroy it

Rule 6 of the [tier 1 contract](adding-an-engine.md) says why snapshot expiry is
off during a run. The trap is that the window does not close when the run does:
geometry is a manifest walk of the final metadata document, and `finish.sh`
performs it after the fleet is gone. So run `finish.sh` before anything —
including a maintenance job on a schedule — expires the run's snapshots, and
before `purge.sh`, which is the one script that deletes measured data.

## Every run gets a fresh identity

A run id is its spec's name plus a UTC stamp, and the topic, the table and the
run directory all derive from it. That is what makes two runs independent and a
leftover topic traceable to the run that created it. Nothing before `purge.sh`
deletes a table, so a campaign accumulates them until they are reclaimed
deliberately.

Never point two runs at one topic or one table. A consumer group's committed
offsets, a checkpoint directory and a table's existing rows all survive, and the
second run measures the first.

## The two AWS SDKs disagree about the region variable

The Java client reads `AWS_REGION`; botocore reads `AWS_DEFAULT_REGION` alone and
is left with no region when only the other is set. Both halves of an MSK IAM
connection need one — the token signer, and S3 under the table's FileIO — so
every pod gets the region under **both** names. Left unset, an S3 client signs
for one region against the global endpoint and a bucket that lives elsewhere
rejects the redirected request as unsigned, which surfaces far from the config
that caused it.

## A run id is not a Kubernetes name

An RFC 1123 subdomain is lowercase, and a run id's stamp is not: the `T` and the
`Z` in it are refused by the API server. So the objects a run becomes are named
by the run id **lowercased**, while the run id itself — the topic, the table, the
run directory, the Spark application name — stays as it is. Anything addressing
a run's objects by hand has to lowercase first.

## A credential written out in full lands wherever the properties do

`site.catalog.props` and `site.kafka.security` reach a pod through a rendered
script or a command line, and both of those are applied as a ConfigMap and
uploaded to the runs prefix. So a site declaring a cluster refuses a
credential-named property whose value is a literal, and `${env:NAME}` plus the
Secret in [`run-spec.md`](run-spec.md) §Secrets is the shape that works there.

The local stack is the exception, and the trap: it declares no cluster, so its
literals load — they are a container image's published defaults — and a site
copied from it onto a cluster meets the refusal rather than the leak. Published
results are unaffected either way: `collect` redacts credential-named properties
and never reads `site.kafka.security` at all.
