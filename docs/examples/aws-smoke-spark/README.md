# Recorded AWS Spark smoke

This run used the setup, image-push, stage, launch, gate, teardown and finish
scripts on 2026-09-09. The gate ran once per minute.

- EKS with three amd64 `m6i.xlarge` nodes.
- Kubeflow spark-operator 2.5.2 with its webhook enabled.
- Amazon MSK: three `kafka.m5.2xlarge` brokers, Kafka 3.9.x and IAM authentication.
- Glue Iceberg REST catalog and EKS Pod Identity.
- Images tagged `e508270`; spec `runs/aws-smoke-spark.yaml`.

The run predates the scorer and producer's 2-CPU requests. The current smoke
requires four nodes; see [cluster sizing](../../../deploy/aws/README.md#sizing-the-cluster).

## Run and verdict

`runs/aws-smoke-spark.yaml`: 2 executors x 2 cores, 4 GiB each, 2 GiB driver,
a 10 s trigger interval, hash distribution mode, 4 Kafka partitions, one
producer shard. Verdict `run_valid: true`, drained, 5,840,896 rows exact (0
loss, 0 duplicates, 300 scored batches), post-warmup freshness p50 9.682 s /
p95 22.943 s / max 36.01 s against a 60 s p95 bound and a 120 s max bound,
~96.5% absorbed at offer end, 8.418 s to drain, backlog max 272,384 rows, 32
snapshots.
These are the `freshness.window` quantiles used for the verdict. Including
warmup, `freshness.full` reports p50 9.695 s and p95 19.943 s.

## File geometry

`finish.sh` measured `geometry.json` from the final table metadata:
1,984 data files, 719,577,266 bytes (~686 MiB), p50 file
size 176,877.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold.

## Result artifacts

`run.json` is the original `collect` record (`schema_version: 2`). Object-store
roots, the image registry and the Glue warehouse account id are replaced with
placeholders. It predates the `run.compression` field.

`scripts/gate.sh` reported `PASS` on every tick, lag never exceeding 36 s
against the 120 s max bound.
