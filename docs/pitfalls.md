# Pitfalls

These issues can invalidate a run or make its measurements misleading.

## Set table properties at creation

With `table.managed_by: harness`, the harness creates the table using
`table.properties`. With `managed_by: engine`, it supplies equivalent DDL in
`facts.ddl` but creates nothing. The engine must apply those properties itself;
writer configuration alone does not establish the properties of an existing
table. Use the same creation path when comparing file geometry.

## Many partitions and writers produce small files

If every writer touches every identity partition, a commit can produce up to one
file per partition value per writer. Increasing the offer rate does not eliminate
this multiplication; partitioning and commit cadence still determine file counts.

The scorer must read the id column from every file. `--read-workers` controls
concurrent reads, and the scorer pod must have enough resources for that setting.
`SLOW_POLL` in a `POLL` line means a poll exceeded its interval. Increase reader
capacity or use coarser partitioning before relying on the verdict: the gate
returns `VOID` when measurements become stale.

## Accepted settings may still be ignored

Both engines can accept settings that have no effect. Staging therefore compares
the running engine's effective configuration with the spec and rejects mismatches.

- **Flink resource fields override `spec.flinkConfiguration`.** Resource values
  in the custom resource take precedence over `extra_flink_conf`. Verification
  checks effective resource settings. Also check `min_pause`: its default is
  `0s`, and a nonzero pause changes cadence even with the same
  `checkpoint_interval`.
- **Spark trigger interval is a `writeStream` argument.** Spark 3.5 exposes no
  REST resource for verifying a streaming query's interval. Settings under
  `spark.sql.streaming.trigger` are rejected because they do not configure it.

## Spark heap size is not total memory

Cloud Spark driver and executor pods request their full CPU limits and use
Guaranteed QoS. Keep resource requests consistent when comparing runs.

`executor_mem_mb` sets the JVM heap. The process also needs memory for JVM and
other overhead, while Parquet decoding and buffering consume heap. The shipped
cloud specs request twice their local counterparts' executor memory. Local runs
use `--master local[N]`, where the driver's heap holds the workload and
`executor_mem_mb` does not allocate a separate executor heap; see
[`engines/spark/README.md`](../engines/spark/README.md).

## Allow one 2-CPU pod per small node

The scorer and every producer shard request 2 CPU. Engine workers also request
2 CPU in the shipped cloud specs (`tm_cpu: 2` / `executor_cores: 2`). The example
`m6i.xlarge` nodes have 4 vCPU, but kubelet reservations and DaemonSets leave less
than 4 CPU available. Each such node therefore fits only one 2-CPU pod. Hour-long
runs need room for the scorer, five producer shards and the engine fleet at once.

Unschedulable pods remain Pending without application logs. A scorer in this
state can exhaust `FIRST_POLL_WAIT_S` (300 s) without publishing a measurement.
Check pod events for `FailedScheduling ... Insufficient cpu`; the drivers print
these alongside logs. `launch.sh` checks available capacity before applying pods
and warns if they will not fit. It permits the launch because an autoscaler may
add nodes. Without autoscaling, size the cluster first; see
[cluster sizing](../deploy/aws/README.md#sizing-the-cluster).

## Keep snapshots until geometry has been measured

The [engine contract](adding-an-engine.md) prohibits snapshot expiry during a
run. Keep expiry disabled through `finish.sh` as well: it walks manifests from
the final metadata document after teardown. Run it before scheduled maintenance
can expire snapshots, and before `purge.sh` deletes the measured data.

## Give every run a fresh identity

A run id combines its spec name and a UTC timestamp. The topic, table and run
directory derive from it, keeping runs independent and leftover resources
traceable. Tables remain until explicitly purged.

Never reuse a topic or table across runs. Committed consumer offsets, checkpoint
state and existing rows can contaminate the next measurement.

## Clean up after failed launches

`stage.sh` creates the topic, artifact prefix and, unless the engine owns DDL,
the table before launch. A failed launch preserves the fleet so pod events remain
available for diagnosis. Clean up explicitly:

- `teardown.sh <run_id>` deletes engine, producer and scorer resources and drops
  the topic.
- `purge.sh <run_id> --artifacts` drops the table, removes its files and deletes
  the run's artifact prefix.

## Set both AWS region variables

The Java SDK reads `AWS_REGION`; botocore uses `AWS_DEFAULT_REGION`. Pods receive
both so the MSK token signer and table FileIO use the intended region. Missing
region configuration can surface as S3 signing or redirect failures.

## Lowercase run ids for Kubernetes objects

Run ids contain uppercase `T` and `Z` in their UTC timestamps. Kubernetes names
must be lowercase, so rendered object names use a lowercased run id. The original
id remains in the topic, table, run directory and Spark application name.
Lowercase it when addressing Kubernetes objects manually.

## Use environment references for cluster credentials

Rendered catalog and Kafka properties are stored in ConfigMaps and uploaded to
the runs prefix. Cluster sites reject literal values for credential-named
properties. Use `${env:NAME}` and the Secret described in
[`run-spec.md`](run-spec.md#secrets).

Local sites allow literals for the stack's public default credentials. Copying
those settings into a cluster site triggers validation. Published results redact
credential-named catalog properties and omit `site.kafka.security` entirely.
